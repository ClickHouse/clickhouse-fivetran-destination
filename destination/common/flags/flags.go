package flags

import (
	"flag"
	"time"
)

var Port = flag.Uint("port", 50052,
	"Listen port")

var LogLevel = flag.String("log-level", "notice",
	"Log level: notice, info, warning, severe")
var LogPretty = flag.Bool("log-pretty", false,
	"Pretty logging instead of JSON")

var WriteBatchSize = flag.Uint("write-batch-size", 100_000,
	"Batch size for all write operations")
var SelectBatchSize = flag.Uint("select-batch-size", 5_000,
	"Batch size for SELECT operations")
var HardDeleteBatchSize = flag.Uint("hard-delete-batch-size", 5_000,
	"Batch size for hard delete operations")
var MaxParallelSelects = flag.Uint("max-parallel-selects", 10,
	"Max number of parallel SELECT queries")

var MaxIdleConnections = flag.Uint("max-idle-connections", 5,
	"Max number of idle connections for ClickHouse client")
var MaxOpenConnections = flag.Uint("max-open-connections", 10,
	"Max number of open connections for ClickHouse client (recommended: max-idle-connections + 5)")
var RequestTimeoutDuration = flag.Duration("request-timeout-duration", 300*time.Second,
	"Timeout for ClickHouse client requests")

var MaxRetries = flag.Uint("max-retries", 10,
	"Max number of retries for ClickHouse client in case of network errors")
var InitialRetryDelayMilliseconds = flag.Uint("initial-retry-delay-ms", 100,
	"Initial delay in milliseconds for backoff retries in case of network errors")
var MaxRetryDelayMilliseconds = flag.Uint("max-retry-delay-ms", 10_000,
	"Max delay in milliseconds for backoff retries in case of network errors")

var MaxInactiveReplicaCheckRetries = flag.Uint("max-inactive-replica-check-retries", 600,
	"Max number of retries when checking inactive replicas before failing")
var InactiveReplicaCheckInterval = flag.Duration("inactive-replica-check-interval", 1*time.Second,
	"Interval between inactive replicas check retries")

var MaxAsyncMutationsCheckRetries = flag.Uint("max-async-mutations-check-retries", 600,
	"Max number of retries when checking async mutations status before failing")
var AsyncMutationsCheckInterval = flag.Duration("async-mutations-check-interval", 1*time.Second,
	"Interval between async mutations status check retries")
