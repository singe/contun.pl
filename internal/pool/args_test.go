package pool

import (
	"testing"
	"time"
)

func TestParseArgsDirect(t *testing.T) {
	opts, err := ParseArgs([]string{
		"--hub-host", "example.com",
		"--hub-port", "5555",
		"--mode", "direct",
		"--target-host", "10.0.0.5",
		"--target-port", "22",
		"--workers", "2",
		"--retry-delay", "2.5",
	})
	if err != nil {
		t.Fatalf("ParseArgs returned error: %v", err)
	}
	if opts.HubHost != "example.com" {
		t.Fatalf("unexpected hub host %q", opts.HubHost)
	}
	if opts.HubPort != 5555 {
		t.Fatalf("unexpected hub port %d", opts.HubPort)
	}
	if opts.Mode != ModeDirect {
		t.Fatalf("unexpected mode %q", opts.Mode)
	}
	if opts.DirectDestination == nil {
		t.Fatal("direct destination nil")
	}
	if opts.DirectDestination.AddrType != AddrIPv4 {
		t.Fatalf("addr type %q", opts.DirectDestination.AddrType)
	}
	if opts.RetryDelay != 2500*time.Millisecond {
		t.Fatalf("retry delay %v", opts.RetryDelay)
	}
}

func TestParseArgsSocks(t *testing.T) {
	opts, err := ParseArgs([]string{
		"--hub-host", "hub.example",
		"--hub-port", "7777",
		"--mode", "socks",
		"--workers", "3",
	})
	if err != nil {
		t.Fatalf("ParseArgs returned error: %v", err)
	}
	if opts.Mode != ModeSocks {
		t.Fatalf("expected socks mode, got %q", opts.Mode)
	}
	if opts.Workers != 3 {
		t.Fatalf("unexpected workers %d", opts.Workers)
	}
	if opts.TargetHost != "" || opts.TargetPort != 0 {
		t.Fatalf("target should be empty in socks mode")
	}
}

func TestParseArgsSocksTargetRejected(t *testing.T) {
	if _, err := ParseArgs([]string{
		"--hub-port", "5555",
		"--mode", "socks",
		"--target-host", "example.com",
	}); err == nil {
		t.Fatalf("expected error when target host provided in socks mode")
	}
}

func TestParseRequest(t *testing.T) {
	req, err := ParseRequest("REQUEST CONNECT ipv4 203.0.113.9 443")
	if err != nil {
		t.Fatalf("ParseRequest error: %v", err)
	}
	if req.AddrType != AddrIPv4 || req.Address != "203.0.113.9" || req.Port != 443 {
		t.Fatalf("unexpected request %+v", req)
	}
	if _, err = ParseRequest("REQUEST CONNECT domain example.com 80"); err != nil {
		t.Fatalf("unexpected error parsing domain: %v", err)
	}
	if _, err = ParseRequest("REQUEST CONNECT badtype example 80"); err == nil {
		t.Fatalf("expected error for bad type")
	}
	if _, err = ParseRequest("REQUEST CONNECT ipv4 host notaport"); err == nil {
		t.Fatalf("expected error for bad port")
	}
}
