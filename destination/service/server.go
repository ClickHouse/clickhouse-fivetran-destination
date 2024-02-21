package service

import (
	"context"
	"fmt"

	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db"
	pb "fivetran.com/fivetran_sdk/proto"
)

const ConnectionTest = "connection"

type Server struct {
	pb.UnimplementedDestinationServer
}

func (s *Server) ConfigurationForm(_ context.Context, _ *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {
	return GetConfigurationFormResponse(), nil
}

func (s *Server) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {
	conn, err := db.GetClickHouseConnection(in.GetConfiguration())
	if err != nil {
		return FailedTestResponse(in.Name, err), nil
	}
	defer conn.Close()

	switch in.Name {
	case ConnectionTest:
		err = conn.ConnectionTest(ctx)
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

func (s *Server) DescribeTable(ctx context.Context, in *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	conn, err := db.GetClickHouseConnection(in.GetConfiguration())
	if err != nil {
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	defer conn.Close()

	tableDescription, err := conn.DescribeTable(ctx, in.SchemaName, in.TableName)
	if err != nil {
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	if tableDescription == nil || len(tableDescription.Columns) == 0 {
		return NotFoundDescribeTableResponse(), nil
	}

	fivetranColumns, err := ToFivetran(tableDescription)
	if err != nil {
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Table{Table: &pb.Table{
			Name:    in.TableName,
			Columns: fivetranColumns,
		}},
	}, nil
}

func (s *Server) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	conn, err := db.GetClickHouseConnection(in.GetConfiguration())
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	cols, err := ToClickHouse(in.Table)
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	err = conn.CreateTable(ctx, in.SchemaName, in.Table.Name, cols)
	if err != nil {
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) AlterTable(ctx context.Context, in *pb.AlterTableRequest) (*pb.AlterTableResponse, error) {
	conn, err := db.GetClickHouseConnection(in.GetConfiguration())
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	currentTableDescription, err := conn.DescribeTable(ctx, in.SchemaName, in.Table.Name)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	alterTableDescription, err := ToClickHouse(in.Table)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	err = conn.AlterTable(ctx, in.SchemaName, in.Table.Name, currentTableDescription, alterTableDescription)
	if err != nil {
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) Truncate(ctx context.Context, in *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	conn, err := db.GetClickHouseConnection(in.GetConfiguration())
	if err != nil {
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}
	defer conn.Close()

	err = conn.TruncateTable(ctx, in.SchemaName, in.TableName)
	if err != nil {
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}

	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) WriteBatch(ctx context.Context, in *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
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

	var pkCols []*types.PrimaryKeyColumn
	fivetranSyncedIdx := -1
	fivetranDeletedIdx := -1
	for i, col := range in.Table.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, &types.PrimaryKeyColumn{
				Name:  col.Name,
				Type:  col.Type,
				Index: uint(i),
			})
		}
		if col.Name == constants.FivetranSynced {
			fivetranSyncedIdx = i
		}
		if col.Name == constants.FivetranDeleted {
			fivetranDeletedIdx = i
		}
	}
	if len(pkCols) == 0 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no primary keys found")), nil
	}
	if fivetranSyncedIdx < 0 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no %s column found", constants.FivetranSynced)), nil
	}
	if fivetranDeletedIdx < 0 {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("no %s column found", constants.FivetranDeleted)), nil
	}

	conn, err := db.GetClickHouseConnection(in.GetConfiguration())
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close()

	columnTypes, err := conn.GetColumnTypes(ctx, in.SchemaName, in.Table.Name)
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	// Benchmark overall WriteBatchRequest and Replace/Update/Delete operations
	err = benchmark.RunAndNotice(func() error {
		if len(in.ReplaceFiles) > 0 {
			err = benchmark.RunAndNotice(func() error {
				for _, replaceFile := range in.ReplaceFiles {
					csvData, err := ReadCSVFile(replaceFile, in.Keys, compression, encryption)
					if err != nil {
						return err
					}
					err = conn.ReplaceBatch(ctx, in.SchemaName, in.Table, csvData, nullStr, *flags.WriteBatchSize)
					if err != nil {
						return err
					}
				}
				return nil
			}, "WriteBatchRequest(Replace)")
			if err != nil {
				return err
			}
		}

		if len(in.UpdateFiles) > 0 {
			err = benchmark.RunAndNotice(func() error {
				for _, updateFile := range in.UpdateFiles {
					csvData, err := ReadCSVFile(updateFile, in.Keys, compression, encryption)
					if err != nil {
						return err
					}
					err = conn.UpdateBatch(ctx, in.SchemaName, in.Table, pkCols, columnTypes, csvData,
						nullStr, unmodifiedStr,
						*flags.WriteBatchSize, *flags.SelectBatchSize, *flags.MaxParallelSelects)
					if err != nil {
						return err
					}
				}
				return nil
			}, "WriteBatchRequest(Update)")
			if err != nil {
				return err
			}
		}

		if len(in.DeleteFiles) > 0 {
			err = benchmark.RunAndNotice(func() error {
				for _, deleteFile := range in.DeleteFiles {
					csvData, err := ReadCSVFile(deleteFile, in.Keys, compression, encryption)
					if err != nil {
						return err
					}
					err = conn.SoftDeleteBatch(ctx, in.SchemaName, in.Table, pkCols, columnTypes, csvData,
						uint(fivetranSyncedIdx), uint(fivetranDeletedIdx),
						*flags.WriteBatchSize, *flags.SelectBatchSize, *flags.MaxParallelSelects)
					if err != nil {
						return err
					}
				}
				return nil
			}, "WriteBatchRequest(Delete)")
			if err != nil {
				return err
			}
		}

		return nil
	}, "WriteBatchRequest(Total)")
	if err != nil {
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}
