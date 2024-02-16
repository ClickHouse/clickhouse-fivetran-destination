package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pb "fivetran.com/fivetran_sdk/proto"
	"google.golang.org/grpc"
)

const Version = "1.0.0"

var port = flag.Uint("port", 50052,
	"Listen port")
var isDevelopment = flag.Bool("dev", false,
	"Whether the server is running in development mode, mainly for pretty logging")
var logLevel = flag.String("log-level", "notice",
	"Log level: notice, info, warning, severe")

var writeBatchSize = flag.Uint("write-batch-size", 100_000,
	"Batch size for all write operations")
var selectBatchSize = flag.Uint("select-batch-size", 1000,
	"Batch size for SELECT operations")
var maxParallelSelects = flag.Uint("max-parallel-selects", 10,
	"Max number of parallel SELECT queries")

var maxIdleConnections = flag.Uint("max-idle-connections", 5,
	"Max number of idle connections for ClickHouse client")
var maxOpenConnections = flag.Uint("max-open-connections", 10,
	"Max number of open connections for ClickHouse client (recommended: max-idle-connections + 5)")

var maxRetries = flag.Uint("max-retries", 10,
	"Max number of retries for ClickHouse client in case of network errors")
var initialRetryDelayMilliseconds = flag.Uint("initial-retry-delay-ms", 100,
	"Initial delay in milliseconds for backoff retries in case of network errors")
var maxRetryDelayMilliseconds = flag.Uint("max-retry-delay-ms", 10_000,
	"Max delay in milliseconds for backoff retries in case of network errors")

var mainExitChan = make(chan os.Signal, 1)

func main() {
	flag.Parse()
	err := InitLogger(*isDevelopment)
	if err != nil {
		LogError(fmt.Errorf("failed to initialize logger: %w", err))
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		LogError(fmt.Errorf("failed to listen: %w", err))
		os.Exit(1)
	}
	s := grpc.NewServer()
	pb.RegisterDestinationServer(s, &server{})

	errChan := make(chan error)
	signal.Notify(mainExitChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		var sb strings.Builder
		flag.VisitAll(func(f *flag.Flag) {
			sb.WriteString(fmt.Sprintf("%s: %s, ", f.Name, f.Value))
		})
		flagsValues := sb.String()
		LogInfo(fmt.Sprintf("Server is ready. Flags: %s", flagsValues[:len(flagsValues)-2]))
		err := s.Serve(listener)
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case <-mainExitChan:
		LogInfo("Shutting down the server...")
		s.GracefulStop()
	case err = <-errChan:
		LogError(fmt.Errorf("failed to serve: %w", err))
		os.Exit(1)
	}
}
