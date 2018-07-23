package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafov/m3u8"
	"github.com/pkg/errors"

	"github.com/puellanivis/breton/lib/files"
	"github.com/puellanivis/breton/lib/files/httpfiles"
	_ "github.com/puellanivis/breton/lib/files/plugins"
	"github.com/puellanivis/breton/lib/files/socketfiles"
	"github.com/puellanivis/breton/lib/glog"
	flag "github.com/puellanivis/breton/lib/gnuflag"
	"github.com/puellanivis/breton/lib/io/bufpipe"
	"github.com/puellanivis/breton/lib/metrics"
	_ "github.com/puellanivis/breton/lib/metrics/http"
	"github.com/puellanivis/breton/lib/mpeg/framer"
	"github.com/puellanivis/breton/lib/mpeg/ts"
	"github.com/puellanivis/breton/lib/mpeg/ts/dvb"
	"github.com/puellanivis/breton/lib/mpeg/ts/psi"
	"github.com/puellanivis/breton/lib/util"
)

// Flags contains all of the flags defined for the application.
var Flags struct {
	Output    string `flag:",short=o"            desc:"Specifies which file to write the output to"`
	UserAgent string `flag:",default=hlscat/1.0" desc:"Which User-Agent string to use"`
	Quiet     bool   `flag:",short=q"            desc:"If set, supresses output from subprocesses."`

	// --packet-size defaults to 1316, which is 1500 - (1500 mod 188)
	// Where 1500 is the typical ethernet MTU, and 188 is the mpegts packet size.
	PacketSize int `flag:",default=1316"         desc:"If outputing to udp, default to using this packet size."`

	Timeout time.Duration `flag:",default=5s"    desc:"The timeout between rapid copy errors."`

	Metrics        bool   `desc:"If set, publish metrics to the given metrics-port or metrics-addr."`
	MetricsPort    int    `desc:"Which port to publish metrics with. (default auto-assign)"`
	MetricsAddress string `desc:"Which local address to listen on; overrides metrics-port flag."`
}

func init() {
	flag.Struct("", &Flags)
}

var (
	bwLifetime = metrics.Gauge("bandwidth_lifetime_bps", "bandwidth of the copy to output process (bits/second)")
	bwRunning  = metrics.Gauge("bandwidth_running_bps", "bandwidth of the copy to output process (bits/second)")
)

var (
	stderr = os.Stderr
	mux    *ts.Mux
)

type discontinuityMarker interface {
	Discontinuity()
}

func openOutput(ctx context.Context, filename string) (io.WriteCloser, func(), error) {
	discontinuity := func() {}

	if !strings.HasPrefix(filename, "mpegts:") {
		f, err := files.Create(ctx, filename)
		if err != nil {
			return nil, nil, err
		}

		glog.Infof("output: %s", f.Name())
		return f, discontinuity, nil
	}

	filename = strings.TrimPrefix(filename, "mpegts:")

	uri, err := url.Parse(filename)
	if err != nil {
		return nil, nil, err
	}

	var opts []files.Option

	if uri.Scheme == "udp" {
		// Default packet size: what the flag --packet-size is.
		pktSize := Flags.PacketSize

		q := uri.Query()
		if urlPktSize := q.Get(socketfiles.FieldPacketSize); urlPktSize != "" {
			// If the output URL has a pkt_size value, override the default.
			sz, err := strconv.ParseInt(urlPktSize, 0, strconv.IntSize)
			if err != nil {
				return nil, nil, errors.Errorf("bad %s value: %s: %+v", socketfiles.FieldPacketSize, urlPktSize, err)
			}

			pktSize = int(sz)
		}

		// Our packet size needs to be an integer multiple of the mpegts packet size.
		pktSize -= (pktSize % ts.PacketSize)

		// Our packet size needs to be at least the mpegts packet size.
		if pktSize <= 0 {
			pktSize = ts.PacketSize
		}

		q.Set(socketfiles.FieldPacketSize, fmt.Sprint(pktSize))

		uri.RawQuery = q.Encode()
		filename = uri.String()

		opts = append(opts, socketfiles.WithIgnoreErrors(true))
	}

	f, err := files.Create(ctx, filename, opts...)
	if err != nil {
		return nil, nil, err
	}
	glog.Infof("output: %s", f.Name())

	mux = ts.NewMux(f)

	var wg sync.WaitGroup

	wr, err := mux.Writer(ctx, 1, ts.ProgramTypeAudio)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if s, ok := wr.(discontinuityMarker); ok {
		discontinuity = s.Discontinuity
	}

	pipe := bufpipe.New(ctx)
	s := framer.NewScanner(pipe)

	wg.Add(1)
	go func() {
		defer func() {
			if err := wr.Close(); err != nil {
				glog.Errorf("mux.Writer.Close: %+v", err)
			}

			wg.Done()
		}()

		for s.Scan() {
			b := s.Bytes()

			n, err := wr.Write(b)
			if err != nil {
				glog.Errorf("mux.Writer.Write: %+v", err)
				return
			}

			if n < len(b) {
				glog.Errorf("mux.Writer.Write: %+v", io.ErrShortWrite)
			}
		}

		if err := s.Err(); err != nil {
			glog.Errorf("framer.Scanner: %s: %+v", filename, err)
		}
	}()

	out := newTriggerWriter(pipe)

	go func() {
		<-out.Trigger()
		for err := range mux.Serve(ctx) {
			glog.Fatalf("mux.Serve: %+v", err)
		}
	}()

	go func() {
		wg.Wait()
		for err := range mux.Close() {
			glog.Errorf("mux.Close: %+v", err)
		}
	}()

	return out, discontinuity, nil
}

// DVBService sets the dvb.ServiceDescriptor to be used by the muxer.
func DVBService(desc *dvb.ServiceDescriptor) {
	if mux != nil {
		service := &dvb.Service{
			ID: 0x0001,
		}
		service.Descriptors = append(service.Descriptors, desc)

		sdt := &dvb.ServiceDescriptorTable{
			Syntax: &psi.SectionSyntax{
				TableIDExtension: 1,
				Current:          true,
			},
			OriginalNetworkID: 0xFF01,
			Services:          []*dvb.Service{service},
		}
		mux.SetDVBSDT(sdt)

		switch {
		case glog.V(5) == true:
			glog.Infof("dvb.sdt: %v", sdt)

		case glog.V(2) == true:
			glog.Infof("DVB Service Description: %v", desc)
		}
	}
}

// HLSReader returns an io.Reader from the given filename using the m3u8 library that reads an HLS stream.
func HLSReader(ctx context.Context, filename string, discontinuity func()) (io.Reader, error) {
	f, err := files.Open(ctx, filename)
	if err != nil {
		return nil, err
	}
	if f.Name() != filename {
		filename = f.Name()
		glog.V(2).Infof("catting %s", filename)
	}

	if false {
		name := filename

		desc := &dvb.ServiceDescriptor{
			Type:     dvb.ServiceTypeTV,
			Provider: "hlscat",
			Name:     name,
		}

		DVBService(desc)
	}

	ctx, err = files.WithRoot(ctx, filename)
	if err != nil {
		return nil, err
	}

	p := m3u8.NewMasterPlaylist()
	err = p.DecodeFrom(f, false)
	if err != nil {
		return nil, err
	}

	var variant *m3u8.Variant
	for _, v := range p.Variants {
		variant = v
		break
	}

	if glog.V(5) {
		glog.Infof("%#v", variant)
	}

	pipe := bufpipe.New(ctx)

	go func() {
		defer pipe.Close()

		var last string

		for {
			b, err := files.Read(ctx, variant.URI)
			if err != nil {
				glog.Errorf("%+v", err)
				return
			}

			pl, err := m3u8.NewMediaPlaylist(42, 42)
			if err != nil {
				glog.Errorf("%+v", err)
				return
			}

			// TODO: this API is dumb, passing a bytes.Buffer by value, rather than by reference…
			// bytes.Buffer contains a [64]byte pre-allocated buffer…
			// So, one is throwing 64-bytes of temporary “to avoid small allocation” storage around on the stack!
			if err := pl.Decode(*bytes.NewBuffer(b), false); err != nil {
				glog.Errorf("%+v", err)
				return
			}

			if glog.V(1) {
				glog.Infof("playlist sequence: %v (%d)", pl.SeqNo, pl.Count())
			}

			segs := pl.Segments[:pl.Count()]

			start := 0
			for i, seg := range segs {
				if last == seg.URI {
					start = i + 1
					break
				}
			}

			var d float64 // Cummulative duration from the target segments.
			for _, seg := range segs[start:] {
				// Accumulate the duration of each segment we will be adding.
				d += seg.Duration
			}
			d /= 2 // cut in half for a bit of buffer.

			// Minimum wait duration should be the TargetDuration.
			if d < pl.TargetDuration {
				d = pl.TargetDuration
			}

			// Now we setup our wait to be half of that total time.
			// This should hopefully give us a buffer.
			wait := time.After(time.Duration(d * float64(time.Second)))

			for _, seg := range segs[start:] {
				last = seg.URI

				if glog.V(1) {
					glog.Infof("copying to buffer: %s", seg.URI)
				}

				if seg.Discontinuity {
					discontinuity()
				}

				b, err := files.Read(ctx, seg.URI)
				if err != nil {
					glog.Errorf("files.Read: %s: %+v", seg.URI, err)
					return
				}

				if _, err := pipe.Write(b); err != nil {
					glog.Errorf("pipe.Write: %+v", err)
					return
				}
			}

			if pl.Closed {
				break
			}

			select {
			case <-wait:
			case <-ctx.Done():
				return
			}
		}
	}()

	return pipe, nil
}

func main() {
	ctx, finish := util.Init("hlscat", 0, 1)
	defer finish()

	ctx = httpfiles.WithUserAgent(ctx, Flags.UserAgent)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		util.Exit(1)
	}

	if Flags.Quiet {
		stderr = nil
	}

	if glog.V(2) {
		if err := flag.Set("stderrthreshold", "INFO"); err != nil {
			glog.Error(err)
		}
	}

	if Flags.MetricsPort != 0 || Flags.MetricsAddress != "" {
		Flags.Metrics = true
	}

	if Flags.Metrics {
		go func() {
			addr := Flags.MetricsAddress
			if addr == "" {
				addr = fmt.Sprintf(":%d", Flags.MetricsPort)
			}

			l, err := net.Listen("tcp", addr)
			if err != nil {
				glog.Fatal("net.Listen: ", err)
			}

			msg := fmt.Sprintf("metrics available at: http://%s/metrics", l.Addr())
			util.Statusln(msg)
			glog.Info(msg)

			http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				http.Redirect(w, req, "/metrics", http.StatusMovedPermanently)
			})

			srv := &http.Server{}

			go func() {
				<-ctx.Done()
				srv.Shutdown(util.Context())
				l.Close()
			}()

			if err := srv.Serve(l); err != nil {
				if err != http.ErrServerClosed {
					glog.Fatal("http.Serve: ", err)
				}
			}
		}()
	}

	out, discontinuity, err := openOutput(ctx, Flags.Output)
	if err != nil {
		glog.Fatal(err)
	}
	defer func() {
		if err := out.Close(); err != nil {
			glog.Error(err)
		}
	}()

	arg, args := args[0], args[1:]

	var opts []files.CopyOption

	if Flags.Metrics {
		opts = append(opts,
			files.WithMetricsScale(8), // bits instead of bytes
			files.WithBandwidthMetrics(bwLifetime),
			files.WithIntervalBandwidthMetrics(bwRunning, 10, 1*time.Second),
		)
	}

	in, err := HLSReader(ctx, arg, discontinuity)
	if err != nil {
		glog.Fatalf("HLSReader: %+v", err)
	}

	for {
		select {
		case <-ctx.Done():
			glog.Error(ctx.Err())
			return
		default:
		}

		start := time.Now()
		wait := time.After(Flags.Timeout)

		n, err := files.Copy(ctx, out, in, opts...)

		if err != nil && err != io.EOF {
			glog.Error(err)

			if n > 0 {
				glog.Errorf("%d bytes copied in %v", n, time.Since(start))
			}

		} else if glog.V(2) {
			glog.Infof("%d bytes copied in %v", n, time.Since(start))
		}

		if err == io.EOF {
			break
		}

		// minimum Flags.Timeout wait.
		select {
		case <-wait:
		case <-ctx.Done():
			glog.Error(ctx.Err())
			return
		}
	}
}
