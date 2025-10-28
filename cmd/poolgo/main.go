package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"contun/internal/pool"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[pool] ")

	opts, err := pool.ParseArgs(os.Args[1:])
	if errors.Is(err, pool.ErrShowUsage) {
		fmt.Fprintln(os.Stderr, pool.Usage())
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		fmt.Fprintln(os.Stderr, pool.Usage())
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	supervisor := pool.NewSupervisor(*opts)
	if err := supervisor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("fatal: %v", err)
	}
}
