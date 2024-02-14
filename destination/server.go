package main

import (
	"context"
	"fmt"

	pb "fivetran.com/fivetran_sdk/proto"
)

const ConnectionTest = "connection"
const MutationTest = "mutation"

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

	switch in.Name {
	case ConnectionTest:
		err = conn.ConnectionTest()
		if err != nil {
			return FailedTestResponse(in.Name, err), nil
		}
	case MutationTest:
		err = conn.MutationTest()
		if err != nil {
			return FailedTestResponse(in.Name, err), nil
		}
	default:
		return FailedTestResponse(in.Name, fmt.Errorf("unexpected test name: %s", in.Name)), nil
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
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	if tableDescription == nil || len(tableDescription.Columns) == 0 {
		return NotFoundDescribeTableResponse(), nil
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
	alterTableDescription, err := ToClickHouseColumns(in.Table)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

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
	var compression pb.Compression
	var encryption pb.Encryption
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
				Index: uint(i),
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
	if fivetranSyncedIdx < 0 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no %s column found", FivetranSynced)), nil
	}
	if fivetranDeletedIdx < 0 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no %s column found", FivetranDeleted)), nil
	}

	conn, err := GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	columnTypes, err := conn.GetColumnTypes(in.SchemaName, in.Table.Name)
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	for _, replaceFile := range in.ReplaceFiles {
		csvData, err := ReadCSVFile(replaceFile, in.Keys, compression, encryption)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
		err = conn.ReplaceBatch(in.SchemaName, in.Table, csvData, nullStr, *replaceBatchSize)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
	}
	for _, updateFile := range in.UpdateFiles {
		csvData, err := ReadCSVFile(updateFile, in.Keys, compression, encryption)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
		err = conn.UpdateBatch(in.SchemaName, in.Table, pkCols, columnTypes, csvData, nullStr, unmodifiedStr, *updateBatchSize, *maxParallelUpdates)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
	}
	for _, deleteFile := range in.DeleteFiles {
		csvData, err := ReadCSVFile(deleteFile, in.Keys, compression, encryption)
		if err != nil {
			return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
		}
		err = conn.SoftDeleteBatch(in.SchemaName, in.Table, pkCols, columnTypes, csvData, uint(fivetranSyncedIdx), uint(fivetranDeletedIdx), *deleteBatchSize, *maxParallelUpdates)
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
