package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc"
)

const ConnectionTest = "connection"
const MutationTest = "mutation"

const DefaultWriteBatchSize = 500

var port = flag.Int("port", 50052, "The server port")
var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

type server struct {
	pb.UnimplementedDestinationServer
}

func (s *server) ConfigurationForm(_ context.Context, _ *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {
	return ConfigurationFormResponse, nil
}

func (s *server) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {
	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedTestResponse(in.Name, err), nil
	}
	defer conn.Close()

	logger.Printf("Running test: %v", in.Name)
	switch in.Name {
	case ConnectionTest:
		err := conn.ConnectionTest()
		if err != nil {
			return FailedTestResponse(in.Name, err), nil
		}
	case MutationTest:
		err := conn.MutationTest()
		if err != nil {
			return FailedTestResponse(in.Name, err), nil
		}
	default:
		return &pb.TestResponse{
			Response: &pb.TestResponse_Failure{
				Failure: fmt.Sprintf("Unexpected test name: %s", in.Name),
			},
		}, nil
	}

	return &pb.TestResponse{
		Response: &pb.TestResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) DescribeTable(ctx context.Context, in *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	defer conn.Close()

	tableDescription, err := conn.DescribeTable(in.SchemaName, in.TableName)
	if err != nil {
		chErr := &clickhouse.Exception{}
		// Code 60 => UNKNOWN_TABLE
		if errors.As(err, &chErr) && chErr.Code == 60 {
			return &pb.DescribeTableResponse{
				Response: &pb.DescribeTableResponse_NotFound{NotFound: true},
			}, nil
		}
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}

	columns, err := ToFivetranColumns(tableDescription)
	if err != nil {
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Table{Table: &pb.Table{
			Name:    in.TableName,
			Columns: columns,
		}},
	}, nil
}

func (s *server) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	cols, err := ToClickHouseColumns(in.Table)
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	err = conn.CreateTable(in.SchemaName, in.Table.Name, cols)
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) AlterTable(ctx context.Context, in *pb.AlterTableRequest) (*pb.AlterTableResponse, error) {
	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	currentTableDescription, err := conn.DescribeTable(in.SchemaName, in.Table.Name)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	logger.Printf("Current table description: %v", currentTableDescription)

	alterTableDescription, err := ToClickHouseColumns(in.Table)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	logger.Printf("Requested table description: %v", alterTableDescription)

	diff := GetAlterTableOps(currentTableDescription, alterTableDescription)
	err = conn.AlterTable(in.SchemaName, in.Table.Name, diff)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) Truncate(ctx context.Context, in *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}
	defer conn.Close()

	err = conn.TruncateTable(in.SchemaName, in.TableName)
	if err != nil {
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}

	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) WriteBatch(ctx context.Context, in *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	compression := pb.Compression_OFF
	encryption := pb.Encryption_NONE
	nullStr := ""
	unmodifiedStr := ""
	csvParams := in.GetCsv()
	if csvParams != nil {
		compression = csvParams.Compression
		encryption = csvParams.Encryption
		nullStr = csvParams.NullString
		unmodifiedStr = csvParams.UnmodifiedString
	} else {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("write batch request without CSV params")), nil
	}

	var pkCols []*PrimaryKeyColumn
	fivetranSyncedIdx := -1
	fivetranDeletedIdx := -1
	for i, col := range in.Table.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, &PrimaryKeyColumn{
				Name:  col.Name,
				Type:  col.Type,
				Index: i,
			})
		}
		if col.Name == FivetranSynced {
			fivetranSyncedIdx = i
		}
		if col.Name == FivetranDeleted {
			fivetranDeletedIdx = i
		}
	}
	if len(pkCols) == 0 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no primary keys found")), nil
	}
	if fivetranSyncedIdx == -1 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no %s column found", FivetranSynced)), nil
	}
	if fivetranDeletedIdx == -1 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no %s column found", FivetranDeleted)), nil
	}

	deleteFiles, failure := ReadAndDecryptWriteBatchFiles(in.SchemaName, in.Table.Name, in.DeleteFiles, in.Keys, compression, encryption)
	if failure != nil {
		return failure, nil
	}
	updateFiles, failure := ReadAndDecryptWriteBatchFiles(in.SchemaName, in.Table.Name, in.UpdateFiles, in.Keys, compression, encryption)
	if failure != nil {
		return failure, nil
	}
	replaceFiles, failure := ReadAndDecryptWriteBatchFiles(in.SchemaName, in.Table.Name, in.ReplaceFiles, in.Keys, compression, encryption)
	if failure != nil {
		return failure, nil
	}

	fmt.Printf("Delete files: %v\n", deleteFiles)
	fmt.Printf("Update files: %v\n", updateFiles)
	fmt.Printf("Replace files: %v\n", replaceFiles)

	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	columnTypes, err := conn.GetColumnTypes(in.SchemaName, in.Table.Name)
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	for _, csvData := range replaceFiles {
		err = conn.ReplaceBatch(in.SchemaName, in.Table, csvData, nullStr, DefaultWriteBatchSize)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
	}
	for _, csvData := range updateFiles {
		err = conn.UpdateBatch(in.SchemaName, in.Table, pkCols, columnTypes, csvData, nullStr, unmodifiedStr, DefaultWriteBatchSize)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
	}
	for _, csvData := range deleteFiles {
		err = conn.SoftDeleteBatch(in.SchemaName, in.Table, pkCols, columnTypes, csvData, DefaultWriteBatchSize, fivetranSyncedIdx, fivetranDeletedIdx)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
	}

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func ReadAndDecryptWriteBatchFiles(
	schemaName string,
	tableName string,
	fileNames []string,
	keys map[string][]byte,
	compression pb.Compression,
	encryption pb.Encryption,
) ([]CSV, *pb.WriteBatchResponse) {
	decryptedFiles := make([]CSV, len(fileNames))
	for i, fileName := range fileNames {
		result := ReadCSVFile(fileName, keys, compression, encryption)
		switch result.Type {
		case KeyNotFound:
			return nil, FailedWriteBatchResponse(tableName, schemaName, fmt.Errorf("key for file %s not found", fileName))
		case FileNotFound:
			return nil, FailedWriteBatchResponse(tableName, schemaName, fmt.Errorf("file %s not found", fileName))
		case FailedToDecompress:
			return nil, FailedWriteBatchResponse(tableName, schemaName, fmt.Errorf("failed to decompress file %s, cause: %s", fileName, *result.Error))
		case FailedToDecrypt:
			return nil, FailedWriteBatchResponse(tableName, schemaName, fmt.Errorf("failed to decrypt file %s, cause: %s", fileName, *result.Error))
		case Success:
			csvReader := csv.NewReader(bytes.NewReader(*result.Data))
			records, err := csvReader.ReadAll()
			if err != nil {
				return nil, FailedWriteBatchResponse(tableName, schemaName, fmt.Errorf("file %s is not a valid CSV, cause: %s", fileName, *result.Error))
			}
			if len(records) < 2 {
				return nil, FailedWriteBatchResponse(tableName, schemaName, fmt.Errorf("expected to have more than 1 line in file %s", fileName))
			}
			decryptedFiles[i] = records[1:] // skip the column names
		}
	}
	return decryptedFiles, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterDestinationServer(s, &server{})
	logger.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		logger.Fatalf("failed to serve: %v", err)
	}
}
