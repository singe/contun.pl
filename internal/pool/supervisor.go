package pool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// Supervisor manages pool workers.
type Supervisor struct {
	opts    Options
	logger  *log.Logger
	dialer  net.Dialer
	retries time.Duration
}

// NewSupervisor constructs a Supervisor for the provided options.
func NewSupervisor(opts Options) *Supervisor {
	return &Supervisor{
		opts:    opts,
		logger:  log.Default(),
		dialer:  net.Dialer{Timeout: 5 * time.Second},
		retries: opts.RetryDelay,
	}
}

// Run launches workers and blocks until context cancellation.
func (s *Supervisor) Run(ctx context.Context) error {
	s.logger.Printf("Starting pool with %d worker(s) in %s mode targeting hub %s:%d",
		s.opts.Workers, s.opts.Mode, s.opts.HubHost, s.opts.HubPort)
	if s.opts.Mode == ModeDirect && s.opts.DirectDestination != nil {
		s.logger.Printf("Direct mode destination %s:%d",
			s.opts.DirectDestination.Host, s.opts.DirectDestination.Port)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < s.opts.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.runWorker(ctx, id)
		}(i + 1)
	}

	wg.Wait()
	return ctx.Err()
}

func (s *Supervisor) runWorker(ctx context.Context, id int) {
	logger := log.New(log.Writer(), fmt.Sprintf("[pool worker %d] ", id), log.Flags())

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := s.dialHub(ctx)
		if err != nil {
			logger.Printf("failed to connect to hub: %v", err)
			if !sleepWithContext(ctx, s.retries) {
				return
			}
			continue
		}

		logger.Printf("connected to hub")
		sessionCtx, cancel := context.WithCancel(ctx)
		err = s.handleHubSession(sessionCtx, conn, logger)
		cancel()
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			logger.Printf("session error: %v", err)
		} else {
			logger.Printf("session ended")
		}
		_ = conn.Close()

		if !sleepWithContext(ctx, s.retries) {
			return
		}
	}
}

func (s *Supervisor) dialHub(ctx context.Context) (net.Conn, error) {
	address := net.JoinHostPort(s.opts.HubHost, fmt.Sprint(s.opts.HubPort))
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.dialer.DialContext(dialCtx, "tcp", address)
}

func (s *Supervisor) handleHubSession(ctx context.Context, hub net.Conn, logger *log.Logger) error {
	abort := make(chan struct{})
	defer close(abort)
	go func() {
		select {
		case <-ctx.Done():
			_ = hub.Close()
		case <-abort:
		}
	}()

	reader := bufio.NewReader(hub)
	writer := bufio.NewWriter(hub)

	if err := s.performHandshake(writer, reader); err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}

	for ctx.Err() == nil {
		line, err := readLine(reader)
		if err != nil {
			return err
		}
		if line == "" {
			continue
		}
		req, err := ParseRequest(line)
		if err != nil {
			logger.Printf("invalid request %q: %v", line, err)
			continue
		}
		if err := validateRequestAddress(req); err != nil {
			logger.Printf("invalid destination %q: %v", line, err)
			if err := sendReply(writer, 1, AddrIPv4, "0.0.0.0", 0); err != nil {
				return err
			}
			continue
		}

		if s.opts.Mode == ModeDirect && s.opts.DirectDestination != nil {
			dest := s.opts.DirectDestination
			if req.Address != dest.Host || req.Port != dest.Port || req.AddrType != dest.AddrType {
				logger.Printf("rejecting mismatched request %s:%d", req.Address, req.Port)
				if err := sendReply(writer, 1, AddrIPv4, "0.0.0.0", 0); err != nil {
					return err
				}
				continue
			}
		}

		targetConn, err := s.dialTarget(ctx, req)
		if err != nil {
			status := mapErrorToStatus(err)
			logger.Printf("failed to reach %s:%d: %v", req.Address, req.Port, err)
			if sendErr := sendReply(writer, status, AddrIPv4, "0.0.0.0", 0); sendErr != nil {
				return sendErr
			}
			continue
		}
		logger.Printf("bridging %s:%d", req.Address, req.Port)
		if err := sendReply(writer, 0, AddrIPv4, "0.0.0.0", 0); err != nil {
			_ = targetConn.Close()
			return err
		}
		if reader.Buffered() > 0 {
			_ = targetConn.Close()
			return fmt.Errorf("unexpected buffered data before streaming")
		}
		if err := writer.Flush(); err != nil {
			_ = targetConn.Close()
			return err
		}

		if err := s.bridge(ctx, hub, targetConn); err != nil && !errors.Is(err, context.Canceled) {
			logger.Printf("bridge ended: %v", err)
		}
		_ = targetConn.Close()
		reader.Reset(hub)
		writer.Reset(hub)
	}
	return ctx.Err()
}

func (s *Supervisor) performHandshake(writer *bufio.Writer, reader *bufio.Reader) error {
	var b strings.Builder
	b.WriteString("HELLO 1 ")
	b.WriteString(string(s.opts.Mode))
	if s.opts.Mode == ModeDirect && s.opts.DirectDestination != nil {
		b.WriteByte(' ')
		b.WriteString(FormatDestination(s.opts.DirectDestination))
	}
	b.WriteByte('\n')
	if _, err := writer.WriteString(b.String()); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	resp, err := readLine(reader)
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("hub rejected handshake: %s", resp)
	}
	return nil
}

func sendReply(writer *bufio.Writer, status int, addrType AddrType, addr string, port int) error {
	if _, err := writer.WriteString(fmt.Sprintf("REPLY %d %s %s %d\n", status, addrType, addr, port)); err != nil {
		return err
	}
	return writer.Flush()
}

func (s *Supervisor) dialTarget(ctx context.Context, req *Request) (net.Conn, error) {
	address := net.JoinHostPort(req.Address, fmt.Sprint(req.Port))
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.dialer.DialContext(dialCtx, "tcp", address)
}

func (s *Supervisor) bridge(ctx context.Context, hub net.Conn, target net.Conn) error {
	// Ensure cancellation tears down both sockets.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = hub.Close()
			_ = target.Close()
		case <-done:
		}
	}()

	errCh := make(chan error, 2)
	copyStream := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		_, err := io.CopyBuffer(dst, src, buf)
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		} else {
			_ = dst.Close()
		}
		errCh <- err
	}

	go copyStream(target, hub)
	go copyStream(hub, target)

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	close(done)
	return firstErr
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func mapErrorToStatus(err error) int {
	if err == nil {
		return 0
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "refused"):
		return 5
	case strings.Contains(msg, "network is unreachable"):
		return 3
	case strings.Contains(msg, "host is unreachable"):
		return 4
	case strings.Contains(msg, "no route"):
		return 4
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "timed out"):
		return 4
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "lookup"):
		return 4
	default:
		return 1
	}
}

func validateRequestAddress(req *Request) error {
	switch req.AddrType {
	case AddrIPv4:
		ip := net.ParseIP(req.Address)
		if ip == nil || ip.To4() == nil {
			return fmt.Errorf("invalid ipv4 address")
		}
	case AddrIPv6:
		ip := net.ParseIP(req.Address)
		if ip == nil || ip.To16() == nil || ip.To4() != nil {
			return fmt.Errorf("invalid ipv6 address")
		}
	case AddrDomain:
		if req.Address == "" {
			return fmt.Errorf("domain required")
		}
		if len(req.Address) > 255 {
			return fmt.Errorf("domain too long")
		}
	default:
		return fmt.Errorf("unsupported address type %q", req.AddrType)
	}
	return nil
}
