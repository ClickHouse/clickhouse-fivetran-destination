package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	pb "fivetran.com/fivetran_sdk/proto"
	"google.golang.org/grpc"
)

var port = flag.Uint("port", 50052, "Listen port")
var isDevelopment = flag.Bool("dev", false, "Whether the server is running in development mode, mainly for pretty logging")

var replaceBatchSize = flag.Uint("replace-batch-size", 100_000, "Batch size for WriteBatch/Replace operations")
var updateBatchSize = flag.Uint("update-batch-size", 1000, "Batch size for WriteBatch/Update operations")
var deleteBatchSize = flag.Uint("delete-batch-size", 1000, "Batch size for WriteBatch/Delete operations")

var maxParallelUpdates = flag.Uint("max-parallel-updates", 5, "Max number of parallel batches to insert for WriteBatch/Update or WriteBatch/Delete operations")
var maxIdleConnections = flag.Uint("max-idle-connections", 5, "Max number of idle connections for ClickHouse client")
var maxOpenConnections = flag.Uint("max-open-connections", 10, "Max number of open connections for ClickHouse client (recommended: max-idle-connections + 5)")

var maxRetries = flag.Uint("max-retries", 30, "Max number of retries for ClickHouse client in case of network errors")
var retryDelayMilliseconds = flag.Uint("retry-delay-ms", 1000, "Delay in milliseconds for retries in case of network errors")

func main() {
	flag.Parse()
	InitLogger(*isDevelopment)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		LogError(fmt.Errorf("failed to listen: %w", err))
		os.Exit(1)
	}
	s := grpc.NewServer()
	pb.RegisterDestinationServer(s, &server{})
	LogInfo(fmt.Sprintf("Server listening at %v, dev mode: %t. "+
		"Client settings: max open connections: %d, max idle connections: %d. "+
		"Batch sizes: replace - %d, update - %d, delete - %d, max parallel updates - %d. "+
		"Retry settings: max retries - %d, delay - %d ms.",
		lis.Addr(), *isDevelopment, *maxOpenConnections, *maxIdleConnections,
		*replaceBatchSize, *updateBatchSize, *deleteBatchSize, *maxParallelUpdates,
		*maxRetries, *retryDelayMilliseconds))
	if err = s.Serve(lis); err != nil {
		LogError(fmt.Errorf("failed to serve: %w", err))
		os.Exit(1)
	}
}
