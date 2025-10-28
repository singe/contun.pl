package pool

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrShowUsage indicates the caller requested help explicitly.
	ErrShowUsage = errors.New("show usage")

	usageText = `Usage: poolgo [options]

Required:
  -j, --hub-host <host>      Hub listener hostname or IP address (default 127.0.0.1).
  -p, --hub-port <port>      Hub listener port accepting pool workers.
  -m, --mode <mode>          Operation mode: direct or socks (default direct).

Direct mode:
  -t, --target-host <host>   Target hostname or IP the bastion can reach.
  -T, --target-port <port>   Target port to proxy traffic to.

Optional:
  -w, --workers <n>          Number of concurrent worker goroutines to keep alive (default 4).
  -r, --retry-delay <sec>    Seconds to wait before re-dialling the hub after a failure (default 1).
  -h, --help                 Show this help message and exit.

poolgo maintains a pool of outbound connections from the bastion to the hub.
In direct mode each worker declares a fixed target and repeatedly proxies
streams to that host:port. In socks mode, workers accept per-connection
destinations supplied by the hub.`
)

// Usage returns the command line help text.
func Usage() string {
	return usageText
}

// Mode identifies the pool operating mode.
type Mode string

const (
	ModeDirect Mode = "direct"
	ModeSocks  Mode = "socks"
)

// Options captures parsed CLI configuration.
type Options struct {
	HubHost    string
	HubPort    int
	Mode       Mode
	TargetHost string
	TargetPort int
	Workers    int
	RetryDelay time.Duration

	DirectDestination *Destination
}

// Destination represents a fixed direct-mode target.
type Destination struct {
	AddrType AddrType
	Host     string
	Port     int
}

// AddrType indicates the textual form of a destination.
type AddrType string

const (
	AddrIPv4   AddrType = "ipv4"
	AddrIPv6   AddrType = "ipv6"
	AddrDomain AddrType = "domain"
)

// ParseArgs parses CLI arguments into Options.
func ParseArgs(args []string) (*Options, error) {
	fs := flag.NewFlagSet("poolgo", flag.ContinueOnError)
	fs.SetOutput(flagDiscard{})

	var (
		hubHost       = fs.String("hub-host", "127.0.0.1", "")
		hubHostAlt    = fs.String("j", "", "")
		hubPort       = fs.Int("hub-port", 0, "")
		hubPortAlt    = fs.Int("p", 0, "")
		mode          = fs.String("mode", "direct", "")
		modeAlt       = fs.String("m", "", "")
		targetHost    = fs.String("target-host", "", "")
		targetHostAlt = fs.String("t", "", "")
		targetPort    = fs.Int("target-port", 0, "")
		targetPortAlt = fs.Int("T", 0, "")
		workers       = fs.Int("workers", 4, "")
		workersAlt    = fs.Int("w", 0, "")
		retryDelay    = fs.Float64("retry-delay", 1.0, "")
		retryDelayAlt = fs.Float64("r", 0.0, "")
		helpFlag      = fs.Bool("help", false, "")
		helpFlagAlt   = fs.Bool("h", false, "")
	)

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, ErrShowUsage
		}
		return nil, err
	}
	if *helpFlag || *helpFlagAlt {
		return nil, ErrShowUsage
	}

	hubHostVal := normalizeString(*hubHostAlt, *hubHost)
	hubPortVal := normalizeInt(*hubPortAlt, *hubPort)
	modeVal := Mode(strings.ToLower(normalizeString(*modeAlt, *mode)))
	workersVal := normalizeInt(*workersAlt, *workers)
	targetHostVal := normalizeString(*targetHostAlt, *targetHost)
	targetPortVal := normalizeInt(*targetPortAlt, *targetPort)

	opts := &Options{
		HubHost: hubHostVal,
		HubPort: hubPortVal,
		Mode:    modeVal,
		Workers: workersVal,
	}

	retrySeconds := normalizeFloat(*retryDelayAlt, *retryDelay)
	if retrySeconds <= 0 {
		retrySeconds = 1.0
	}
	opts.RetryDelay = time.Duration(float64(time.Second) * retrySeconds)

	switch opts.Mode {
	case ModeDirect:
		opts.TargetHost = targetHostVal
		opts.TargetPort = targetPortVal
	case ModeSocks:
		if targetHostVal != "" || targetPortVal != 0 {
			return nil, fmt.Errorf("--target-host/--target-port are not used in socks mode")
		}
	default:
		return nil, fmt.Errorf("--mode must be direct or socks")
	}

	if opts.HubPort <= 0 || opts.HubPort > 65535 {
		return nil, fmt.Errorf("missing or invalid --hub-port")
	}
	if opts.Workers <= 0 {
		return nil, fmt.Errorf("--workers must be positive")
	}

	if opts.Mode == ModeDirect {
		if opts.TargetHost == "" {
			return nil, fmt.Errorf("--target-host is required in direct mode")
		}
		if opts.TargetPort <= 0 || opts.TargetPort > 65535 {
			return nil, fmt.Errorf("--target-port must be between 1 and 65535")
		}
		addrType := classifyAddr(opts.TargetHost)
		opts.DirectDestination = &Destination{
			AddrType: addrType,
			Host:     opts.TargetHost,
			Port:     opts.TargetPort,
		}
	}

	return opts, nil
}

func normalizeString(override, base string) string {
	if override != "" {
		return override
	}
	return base
}

func normalizeInt(override, base int) int {
	if override != 0 {
		return override
	}
	return base
}

func normalizeFloat(override, base float64) float64 {
	if override != 0 {
		return override
	}
	return base
}

func classifyAddr(host string) AddrType {
	if ip := net.ParseIP(host); ip != nil {
		if ip.To4() != nil {
			return AddrIPv4
		}
		return AddrIPv6
	}
	return AddrDomain
}

// flagDiscard is a writer that ignores output to keep flag package quiet.
type flagDiscard struct{}

func (flagDiscard) Write(p []byte) (int, error) { return len(p), nil }

// FormatDestination renders the control plane DEST fragment.
func FormatDestination(dest *Destination) string {
	if dest == nil {
		return ""
	}
	return fmt.Sprintf("DEST %s %s %d", dest.AddrType, dest.Host, dest.Port)
}

// ParseRequest converts a hub REQUEST line into a Request struct.
func ParseRequest(line string) (*Request, error) {
	fields := strings.Fields(line)
	if len(fields) != 5 {
		return nil, fmt.Errorf("unexpected request line: %q", line)
	}
	if fields[0] != "REQUEST" || fields[1] != "CONNECT" {
		return nil, fmt.Errorf("unexpected request command: %q", line)
	}
	addrType := AddrType(strings.ToLower(fields[2]))
	switch addrType {
	case AddrIPv4, AddrIPv6, AddrDomain:
	default:
		return nil, fmt.Errorf("unknown address type %q", fields[2])
	}
	port, err := strconv.Atoi(fields[4])
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port in request: %q", fields[4])
	}
	return &Request{
		AddrType: addrType,
		Address:  fields[3],
		Port:     port,
	}, nil
}

// Request describes a hub connection request.
type Request struct {
	AddrType AddrType
	Address  string
	Port     int
}
