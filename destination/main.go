package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"google.golang.org/grpc"
)

const Version = "0.0.1"

var port = flag.Uint("port", 50052, "Listen port")
var isDevelopment = flag.Bool("dev", false, "Whether the server is running in development mode, mainly for pretty logging")
var logLevel = flag.String("log-level", "notice", "Log level: notice, info, warning, severe")
var logMemUsage = flag.Bool("log-mem-usage", false, "Whether to periodically log memory usage")

var replaceBatchSize = flag.Uint("replace-batch-size", 1_000_000, "Batch size for WriteBatch/Replace operations")
var updateBatchSize = flag.Uint("update-batch-size", 5000, "Batch size for WriteBatch/Update operations")
var deleteBatchSize = flag.Uint("delete-batch-size", 5000, "Batch size for WriteBatch/Delete operations")

var maxParallelUpdates = flag.Uint("max-parallel-updates", 10, "Max number of parallel batches to insert for WriteBatch/Update or WriteBatch/Delete operations")
var maxIdleConnections = flag.Uint("max-idle-connections", 10, "Max number of idle connections for ClickHouse client")
var maxOpenConnections = flag.Uint("max-open-connections", 10, "Max number of open connections for ClickHouse client (recommended: max-idle-connections + 5)")

var maxRetries = flag.Uint("max-retries", 10, "Max number of retries for ClickHouse client in case of network errors")
var initialRetryDelayMilliseconds = flag.Uint("initial-retry-delay-ms", 100, "Initial delay in milliseconds for backoff retries in case of network errors")
var maxRetryDelayMilliseconds = flag.Uint("max-retry-delay-ms", 10_000, "Max delay in milliseconds for backoff retries in case of network errors")

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
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGTERM, syscall.SIGINT)

	if *logMemUsage {
		go func() {
			mb := uint64(1024 * 1024)
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				LogNotice(fmt.Sprintf("Alloc = %v MB; HeapAlloc = %v MB; TotalAlloc = %v MB; NumGC = %v",
					m.Alloc/mb, m.HeapAlloc/mb, m.TotalAlloc/mb, m.NumGC))
				time.Sleep(5 * time.Second)
			}
		}()
	}

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
	case <-exitChan:
		LogInfo("Shutting down the server...")
		s.GracefulStop()
	case err = <-errChan:
		LogError(fmt.Errorf("failed to serve: %w", err))
		os.Exit(1)
	}
}
