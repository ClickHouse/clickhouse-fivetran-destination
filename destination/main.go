package main

import (
	"context"
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

var port = flag.Int("port", 50052, "The server port")
var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

type server struct {
	pb.UnimplementedDestinationServer
}

func (s *server) ConfigurationForm(ctx context.Context, in *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {
	return &pb.ConfigurationFormResponse{
		SchemaSelectionSupported: true,
		TableSelectionSupported:  true,
		Fields: []*pb.FormField{
			{
				Name:     "hostname",
				Label:    "Hostname",
				Required: true,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     "port",
				Label:    "Port",
				Required: false,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     "database",
				Label:    "Database",
				Required: false,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     "username",
				Label:    "Username",
				Required: false,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_Password,
				},
			},
			{
				Name:     "password",
				Label:    "Password",
				Required: false,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_Password,
				},
			},
			{
				Name:     "ssl",
				Label:    "SSL",
				Required: false,
				Type: &pb.FormField_ToggleField{
					ToggleField: &pb.ToggleField{},
				},
			},
		},
		Tests: []*pb.ConfigurationTest{
			{
				Name:  ConnectionTest,
				Label: "Test connection and basic operations",
			},
			{
				Name:  MutationTest,
				Label: "Test mutation operations",
			},
		},
	}, nil
}

func (s *server) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {
	conn := GetClickHouseConnection(in.GetConfiguration())
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

	err := conn.Close()
	if err != nil {
		return nil, err
	}

	return &pb.TestResponse{
		Response: &pb.TestResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) DescribeTable(ctx context.Context, in *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	conn := GetClickHouseConnection(in.GetConfiguration())
	tableDescription, err := conn.DescribeTable(in.SchemaName, in.TableName)

	if err != nil {
		chErr := &clickhouse.Exception{}
		// Code 60 => UNKNOWN_TABLE
		if errors.As(err, &chErr) && chErr.Code == 60 {
			return &pb.DescribeTableResponse{
				Response: &pb.DescribeTableResponse_NotFound{NotFound: true},
			}, nil
		}
		return &pb.DescribeTableResponse{
			Response: &pb.DescribeTableResponse_Failure{
				Failure: fmt.Sprintf("Failed to describe `%s`.`%s`, cause: %s", in.SchemaName, in.TableName, err),
			},
		}, nil
	}

	err = conn.Close()
	if err != nil {
		return nil, err
	}

	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Table{Table: &pb.Table{
			Name:    in.TableName,
			Columns: ToFivetranColumns(tableDescription),
		}},
	}, nil
}

func (s *server) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	conn := GetClickHouseConnection(in.GetConfiguration())
	cols, err := ToClickHouseColumns(in.Table)
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	err = conn.CreateTable(in.SchemaName, in.Table.Name, cols, "Memory")
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	err = conn.Close()
	if err != nil {
		return nil, err
	}

	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) AlterTable(ctx context.Context, in *pb.AlterTableRequest) (*pb.AlterTableResponse, error) {
	conn := GetClickHouseConnection(in.GetConfiguration())
	currentTableDescription, err := conn.DescribeTable(in.SchemaName, in.Table.Name)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	logger.Printf("Current table description: %s", currentTableDescription)

	alterTableDescription, err := ToClickHouseColumns(in.Table)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	logger.Printf("Requested table description: %s", alterTableDescription)

	diff := GetAlterTableOps(currentTableDescription, alterTableDescription)
	err = conn.AlterTable(in.SchemaName, in.Table.Name, diff)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	err = conn.Close()
	if err != nil {
		return nil, err
	}

	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *server) Truncate(ctx context.Context, in *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	conn := GetClickHouseConnection(in.GetConfiguration())
	err := conn.TruncateTable(in.SchemaName, in.TableName)
	if err != nil {
		return &pb.TruncateResponse{
			Response: &pb.TruncateResponse_Failure{
				Failure: fmt.Sprintf("Failed to truncate table `%s`.`%s`, cause: %s", in.SchemaName, in.TableName, err),
			},
		}, nil
	}

	err = conn.Close()
	if err != nil {
		return nil, err
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
	csvParams := in.GetCsv()
	if csvParams != nil {
		compression = csvParams.Compression
		encryption = csvParams.Encryption
	}

	deleteFiles, failure := ReadAndDecryptWriteBatchFiles(in.DeleteFiles, in.Keys, compression, encryption)
	if failure != nil {
		return failure, nil
	}
	updateFiles, failure := ReadAndDecryptWriteBatchFiles(in.UpdateFiles, in.Keys, compression, encryption)
	if failure != nil {
		return failure, nil
	}
	replaceFiles, failure := ReadAndDecryptWriteBatchFiles(in.ReplaceFiles, in.Keys, compression, encryption)
	if failure != nil {
		return failure, nil
	}

	fmt.Printf("Delete files: %v\n", deleteFiles)
	fmt.Printf("Update files: %v\n", updateFiles)
	fmt.Printf("Replace files: %v\n", replaceFiles)

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func ReadAndDecryptWriteBatchFiles(
	files []string,
	keys map[string][]byte,
	compression pb.Compression,
	encryption pb.Encryption,
) ([]string, *pb.WriteBatchResponse) {
	decryptedFiles := make([]string, len(files))
	for i, file := range files {
		result := ReadAndDecryptCSVFile(file, keys, compression, encryption)
		switch result.Type {
		case KeyNotFound:
			return nil, FailedWriteBatchResponse(fmt.Sprintf("Key for file %s not found", file))
		case FileNotFound:
			return nil, FailedWriteBatchResponse(fmt.Sprintf("File %s not found", file))
		case FailedToDecompress:
			return nil, FailedWriteBatchResponse(fmt.Sprintf("Failed to decompress file %s, cause: %s", file, *result.Error))
		case FailedToDecrypt:
			return nil, FailedWriteBatchResponse(fmt.Sprintf("Failed to decrypt file %s, cause: %s", file, *result.Error))
		case Success:
			decryptedFiles[i] = string(*result.Data)
		}
	}
	return decryptedFiles, nil
}

func FailedWriteBatchResponse(reason string) *pb.WriteBatchResponse {
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Failure{
			Failure: fmt.Sprintf("Failed to write batch, cause: %s", reason),
		},
	}
}

func FailedTestResponse(name string, err error) *pb.TestResponse {
	return &pb.TestResponse{
		Response: &pb.TestResponse_Failure{
			Failure: fmt.Sprintf("Test %s failed, cause: %s", name, err),
		},
	}
}

func FailedCreateTableResponse(schemaName string, tableName string, err error) *pb.CreateTableResponse {
	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Failure{
			Failure: fmt.Sprintf("Failed to create table `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
}

func FailedAlterTableResponse(schemaName string, tableName string, err error) *pb.AlterTableResponse {
	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Failure{
			Failure: fmt.Sprintf("Failed to alter table `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
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
