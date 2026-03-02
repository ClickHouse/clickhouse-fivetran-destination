package service

import (
	"context"
	"fmt"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db"
	pb "fivetran.com/fivetran_sdk/proto"
)

const ConnectionTest = "connection"
const GrantsTest = "grants"

type Server struct {
	pb.UnimplementedDestinationConnectorServer
}

func (s *Server) ConfigurationForm(_ context.Context, _ *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {
	return GetConfigurationFormResponse(), nil
}

func (s *Server) Capabilities(_ context.Context, _ *pb.CapabilitiesRequest) (*pb.CapabilitiesResponse, error) {
	return &pb.CapabilitiesResponse{BatchFileFormat: pb.BatchFileFormat_CSV}, nil
}

func (s *Server) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {

	log.Info(fmt.Sprintf("[Test_%s] Starting test", in.Name))
	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[Test_%s] Failed to connect: %w", in.Name, err))
		return FailedTestResponse(in.Name, err), nil
	}
	defer conn.Close() //nolint:errcheck

	switch in.Name {
	case ConnectionTest:
		err = conn.ConnectionTest(ctx)
		if err != nil {
			log.Error(fmt.Errorf("[Test_%s] Connection test failed: %w", in.Name, err))
			return FailedTestResponse(in.Name, err), nil
		}
		log.Info("Connection test passed")
	case GrantsTest:
		err = conn.GrantsTest(ctx)
		if err != nil {
			log.Error(fmt.Errorf("[Test_%s] Grants test failed: %w", in.Name, err))
			return FailedTestResponse(in.Name, err), nil
		}
		log.Info("User grants test passed")
	default:
		err := fmt.Errorf("unexpected test name: %s", in.Name)
		log.Error(fmt.Errorf("[Test_%s] %w", in.Name, err))
		return FailedTestResponse(in.Name, err), nil
	}

	log.Info(fmt.Sprintf("[Test_%s] Test completed successfully", in.Name))
	return &pb.TestResponse{
		Response: &pb.TestResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) DescribeTable(ctx context.Context, in *pb.DescribeTableRequest) (*pb.DescribeTableResponse, error) {
	log.Info(fmt.Sprintf("[DescribeTable] Starting for %s.%s", in.SchemaName, in.TableName))

	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[DescribeTable] Failed to connect for %s.%s: %w", in.SchemaName, in.TableName, err))
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	defer conn.Close() //nolint:errcheck

	tableDescription, err := conn.DescribeTable(ctx, in.SchemaName, in.TableName)
	if err != nil {
		log.Error(fmt.Errorf("[DescribeTable] Failed to describe %s.%s: %w", in.SchemaName, in.TableName, err))
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}
	if tableDescription == nil || len(tableDescription.Columns) == 0 {
		log.Info(fmt.Sprintf("[DescribeTable] Table %s.%s not found", in.SchemaName, in.TableName))
		return NotFoundDescribeTableResponse(), nil
	}

	log.Info(fmt.Sprintf("[DescribeTable] Found %s.%s with %d columns", in.SchemaName, in.TableName, len(tableDescription.Columns)))

	fivetranColumns, err := ToFivetran(tableDescription)
	if err != nil {
		log.Error(fmt.Errorf("[DescribeTable] Failed to convert columns for %s.%s: %w", in.SchemaName, in.TableName, err))
		return FailedDescribeTableResponse(in.SchemaName, in.TableName, err), nil
	}

	log.Info(fmt.Sprintf("[DescribeTable] Completed successfully for %s.%s", in.SchemaName, in.TableName))
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Table{Table: &pb.Table{
			Name:    in.TableName,
			Columns: fivetranColumns,
		}},
	}, nil
}

func (s *Server) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	log.Info(fmt.Sprintf("[CreateTable] Starting for %s.%s with %d columns", in.SchemaName, in.Table.Name, len(in.Table.Columns)))

	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[CreateTable] Failed to connect for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close() //nolint:errcheck

	log.Info(fmt.Sprintf("[CreateTable] Converting columns for %s.%s", in.SchemaName, in.Table.Name))
	cols, err := ToClickHouse(in.Table)
	if err != nil {
		log.Error(fmt.Errorf("[CreateTable] Failed to convert columns for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Info(fmt.Sprintf("[CreateTable] Executing CREATE TABLE for %s.%s", in.SchemaName, in.Table.Name))
	err = conn.CreateTable(ctx, in.SchemaName, in.Table.Name, cols)
	if err != nil {
		log.Error(fmt.Errorf("[CreateTable] Failed to create table %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedCreateTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Info(fmt.Sprintf("[CreateTable] Completed successfully for %s.%s", in.SchemaName, in.Table.Name))
	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) AlterTable(ctx context.Context, in *pb.AlterTableRequest) (*pb.AlterTableResponse, error) {
	log.Info(fmt.Sprintf("[AlterTable] Starting for %s.%s with %d columns", in.SchemaName, in.Table.Name, len(in.Table.Columns)))

	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[AlterTable] Failed to connect for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close() //nolint:errcheck

	log.Info(fmt.Sprintf("[AlterTable] Describing current table %s.%s", in.SchemaName, in.Table.Name))
	currentTableDescription, err := conn.DescribeTable(ctx, in.SchemaName, in.Table.Name)
	if err != nil {
		log.Error(fmt.Errorf("[AlterTable] Failed to describe current table %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Info(fmt.Sprintf("[AlterTable] Converting columns for %s.%s", in.SchemaName, in.Table.Name))
	alterTableDescription, err := ToClickHouse(in.Table)
	if err != nil {
		log.Error(fmt.Errorf("[AlterTable] Failed to convert columns for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Info(fmt.Sprintf("[AlterTable] Executing ALTER TABLE for %s.%s", in.SchemaName, in.Table.Name))
	_, err = conn.AlterTable(ctx, in.SchemaName, in.Table.Name, currentTableDescription, alterTableDescription)
	if err != nil {
		log.Error(fmt.Errorf("[AlterTable] Failed to alter table %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedAlterTableResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Info(fmt.Sprintf("[AlterTable] Completed successfully for %s.%s", in.SchemaName, in.Table.Name))
	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) Truncate(ctx context.Context, in *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	var softDeleteColumn *string = nil
	if in.Soft != nil && in.Soft.DeletedColumn != "" {
		softDeleteColumn = &in.Soft.DeletedColumn
	}

	truncateBefore := time.Unix(in.UtcDeleteBefore.Seconds, int64(in.UtcDeleteBefore.Nanos)).UTC()
	deleteType := "hard"
	if softDeleteColumn != nil {
		deleteType = "soft"
	}

	log.Info(fmt.Sprintf("[Truncate] Starting %s delete for %s.%s, synced_column=%s, truncate_before=%s",
		deleteType, in.SchemaName, in.TableName, in.SyncedColumn, truncateBefore.Format(time.RFC3339)))

	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[Truncate] GetClickHouseConnection error for %s.%s: %w", in.SchemaName, in.TableName, err))
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}
	defer conn.Close() //nolint:errcheck

	log.Info(fmt.Sprintf("[Truncate] Checking if table %s.%s exists", in.SchemaName, in.TableName))
	// should not be failed if the table does not exist, as per SDK documentation
	tableDescription, err := conn.DescribeTable(ctx, in.SchemaName, in.TableName)
	if err != nil {
		log.Error(fmt.Errorf("[Truncate] DescribeTable error for %s.%s: %w", in.SchemaName, in.TableName, err))
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}
	if tableDescription == nil || len(tableDescription.Columns) == 0 {
		log.Info(fmt.Sprintf("[Truncate] Table %s.%s not found, skipping truncate", in.SchemaName, in.TableName))
		return SuccessfulTruncateTableResponse(), nil
	}

	log.Info(fmt.Sprintf("[Truncate] Executing %s truncate for %s.%s", deleteType, in.SchemaName, in.TableName))
	err = conn.TruncateTable(ctx, in.SchemaName, in.TableName, in.SyncedColumn, truncateBefore, softDeleteColumn)
	if err != nil {
		log.Error(fmt.Errorf("[Truncate] TruncateTable failed for %s.%s: %w", in.SchemaName, in.TableName, err))
		return FailedTruncateTableResponse(in.SchemaName, in.TableName, err), nil
	}

	log.Info(fmt.Sprintf("[Truncate] Completed successfully for %s.%s", in.SchemaName, in.TableName))
	return SuccessfulTruncateTableResponse(), nil
}

func (s *Server) WriteHistoryBatch(ctx context.Context, in *pb.WriteHistoryBatchRequest) (*pb.WriteBatchResponse, error) {
	log.Notice(fmt.Sprintf("[WriteHistoryBatch] Starting for %s.%s with earliest_start_files=%d, replace_files=%d, update_files=%d, delete_files=%d",
		in.SchemaName, in.Table.Name, len(in.EarliestStartFiles), len(in.ReplaceFiles), len(in.UpdateFiles), len(in.DeleteFiles)))

	var compression pb.Compression
	var encryption pb.Encryption
	nullStr := ""
	unmodifiedStr := ""
	csvParams := in.GetFileParams()
	if csvParams != nil {
		compression = csvParams.Compression
		encryption = csvParams.Encryption
		nullStr = csvParams.NullString
		unmodifiedStr = csvParams.UnmodifiedString
	} else {
		err := fmt.Errorf("cannot process a write history batch request without CSV params")
		log.Error(fmt.Errorf("[WriteHistoryBatch] %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteHistoryBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Notice(fmt.Sprintf("[WriteHistoryBatch] Compression=%s for %s.%s", compression.String(), in.SchemaName, in.Table.Name))
	metadata, err := GetFivetranTableMetadata(in.Table)
	if err != nil {
		log.Error(fmt.Errorf("[WriteHistoryBatch] GetFivetranTableMetadata error for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteHistoryBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("GetFivetranTableMetadata error: %w", err)), nil
	}

	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[WriteHistoryBatch] GetClickHouseConnection error for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteHistoryBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("GetClickHouseConnection error: %w", err)), nil
	}
	defer conn.Close() //nolint:errcheck

	log.Notice(fmt.Sprintf("[WriteHistoryBatch] Getting column types for %s.%s", in.SchemaName, in.Table.Name))
	columnTypes, err := conn.GetColumnTypes(ctx, in.SchemaName, in.Table.Name)
	if err != nil {
		log.Error(fmt.Errorf("[WriteHistoryBatch] GetColumnTypes error for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteHistoryBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("GetColumnTypes error: %w", err)), nil
	}
	driverColumns := types.MakeDriverColumns(columnTypes)

	// Benchmark overall WriteHistoryBatchRequest and, separately, EarliestStart/Replace/Update/Delete operations
	err = benchmark.RunAndNotice(func() error {
		err = s.processEarliestStartFilesForHistoryBatch(ctx, in, conn, compression, encryption, metadata, driverColumns)
		if err != nil {
			return err
		}

		err = s.processReplaceFilesForHistoryBatch(ctx, in, conn, compression, encryption, nullStr, metadata, driverColumns)
		if err != nil {
			return err
		}
		err = s.processUpdateFilesForHistoryBatch(ctx, in, conn, compression, encryption, nullStr, unmodifiedStr, metadata, driverColumns)
		if err != nil {
			return err
		}
		err = s.processDeleteFilesForHistoryBatch(ctx, in, conn, compression, encryption, metadata, driverColumns)
		if err != nil {
			return err
		}
		return nil
	}, writeHistoryBatchTotalOp)
	if err != nil {
		log.Error(fmt.Errorf("[WriteHistoryBatch] Operation error for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteHistoryBatchResponse(in.SchemaName, in.Table.Name, fmt.Errorf("operation error: %w", err)), nil
	}

	log.Notice(fmt.Sprintf("[WriteHistoryBatch] Completed successfully for %s.%s", in.SchemaName, in.Table.Name))
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) WriteBatch(ctx context.Context, in *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	log.Notice(fmt.Sprintf("[WriteBatch] Starting for %s.%s with replace_files=%d, update_files=%d, delete_files=%d",
		in.SchemaName, in.Table.Name, len(in.ReplaceFiles), len(in.UpdateFiles), len(in.DeleteFiles)))

	var compression pb.Compression
	var encryption pb.Encryption
	nullStr := ""
	unmodifiedStr := ""
	csvParams := in.GetFileParams()
	if csvParams != nil {
		compression = csvParams.Compression
		encryption = csvParams.Encryption
		nullStr = csvParams.NullString
		unmodifiedStr = csvParams.UnmodifiedString
	} else {
		err := fmt.Errorf("cannot process a write batch request without CSV params")
		log.Error(fmt.Errorf("[WriteBatch] %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Notice(fmt.Sprintf("[WriteBatch] Compression=%s for %s.%s", compression.String(), in.SchemaName, in.Table.Name))

	metadata, err := GetFivetranTableMetadata(in.Table)
	if err != nil {
		log.Error(fmt.Errorf("[WriteBatch] Failed to get table metadata for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	conn, err := db.GetClickHouseConnection(ctx, in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[WriteBatch] Failed to connect for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}
	defer conn.Close() //nolint:errcheck

	log.Notice(fmt.Sprintf("[WriteBatch] Getting column types for %s.%s", in.SchemaName, in.Table.Name))
	columnTypes, err := conn.GetColumnTypes(ctx, in.SchemaName, in.Table.Name)
	if err != nil {
		log.Error(fmt.Errorf("[WriteBatch] Failed to get column types for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}
	driverColumns := types.MakeDriverColumns(columnTypes)

	// Benchmark overall WriteBatchRequest and, separately, Replace/Update/Delete operations
	err = benchmark.RunAndNotice(func() error {
		err = s.processReplaceFiles(ctx, in, conn, compression, encryption, nullStr, metadata, driverColumns)
		if err != nil {
			return err
		}
		err = s.processUpdateFiles(ctx, in, conn, compression, encryption, nullStr, unmodifiedStr, metadata, driverColumns)
		if err != nil {
			return err
		}
		err = s.processDeleteFiles(ctx, in, conn, compression, encryption, metadata, driverColumns)
		if err != nil {
			return err
		}
		return nil
	}, writeBatchTotalOp)
	if err != nil {
		log.Error(fmt.Errorf("[WriteBatch] Failed for %s.%s: %w", in.SchemaName, in.Table.Name, err))
		return FailedWriteBatchResponse(in.SchemaName, in.Table.Name, err), nil
	}

	log.Notice(fmt.Sprintf("[WriteBatch] Completed successfully for %s.%s", in.SchemaName, in.Table.Name))
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Success{
			Success: true,
		},
	}, nil
}

func (s *Server) processReplaceFiles(
	ctx context.Context,
	in *pb.WriteBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	nullStr string,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.ReplaceFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d replace files for %s.%s", writeBatchReplaceOp, len(in.ReplaceFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, replaceFile := range in.ReplaceFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeBatchReplaceOp, fileIdx+1, len(in.ReplaceFiles), replaceFile))
				csvData, err := ReadCSVFile(replaceFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeBatchReplaceOp, replaceFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeBatchReplaceOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   replaceFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeBatchReplaceOp, replaceFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, true)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeBatchReplaceOp, replaceFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]
				log.Notice(fmt.Sprintf("[%s] Executing ReplaceBatch for %s.%s with %d rows", writeBatchReplaceOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.ReplaceBatch(ctx, in.SchemaName, in.Table, csvWithoutHeader, csvColumns, nullStr)
				if err != nil {
					log.Error(fmt.Errorf("[%s] ReplaceBatch failed for %s.%s: %w", writeBatchReplaceOp, in.SchemaName, in.Table.Name, err))
					return err
				}
			}
			return nil
		}, writeBatchReplaceOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all replace files for %s.%s", writeBatchReplaceOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

func (s *Server) processEarliestStartFilesForHistoryBatch(
	ctx context.Context,
	in *pb.WriteHistoryBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.EarliestStartFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d earliest start files for %s.%s", writeHistoryBatchEarliestStartOp, len(in.EarliestStartFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, earliestStartFile := range in.EarliestStartFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeHistoryBatchEarliestStartOp, fileIdx+1, len(in.EarliestStartFiles), earliestStartFile))
				csvData, err := ReadCSVFile(earliestStartFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeHistoryBatchEarliestStartOp, earliestStartFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeHistoryBatchEarliestStartOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   earliestStartFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeHistoryBatchEarliestStartOp, earliestStartFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, false)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeHistoryBatchEarliestStartOp, earliestStartFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]

				log.Notice(fmt.Sprintf("[%s] Executing HardDeleteForEarliestStartHistory for %s.%s with %d rows", writeHistoryBatchEarliestStartOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.HardDeleteForEarliestStartHistory(ctx, in.SchemaName, in.Table, csvWithoutHeader, csvColumns)
				if err != nil {
					log.Error(fmt.Errorf("[%s] HardDeleteForEarliestStartHistory failed for %s.%s: %w", writeHistoryBatchEarliestStartOp, in.SchemaName, in.Table.Name, err))
					return err
				}

				log.Notice(fmt.Sprintf("[%s] Executing UpdateForEarliestStartHistory for %s.%s with %d rows", writeHistoryBatchEarliestStartOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.UpdateForEarliestStartHistory(ctx, in.SchemaName, in.Table, csvWithoutHeader, csvColumns, constants.FivetranStart)
				if err != nil {
					log.Error(fmt.Errorf("[%s] UpdateForEarliestStartHistory failed for %s.%s: %w", writeHistoryBatchEarliestStartOp, in.SchemaName, in.Table.Name, err))
					return err
				}

			}
			return nil
		}, writeHistoryBatchEarliestStartOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all earliest start files for %s.%s", writeHistoryBatchEarliestStartOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

func (s *Server) processReplaceFilesForHistoryBatch(
	ctx context.Context,
	in *pb.WriteHistoryBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	nullStr string,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.ReplaceFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d replace files for %s.%s", writeHistoryBatchReplaceOp, len(in.ReplaceFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, replaceFile := range in.ReplaceFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeHistoryBatchReplaceOp, fileIdx+1, len(in.ReplaceFiles), replaceFile))
				csvData, err := ReadCSVFile(replaceFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeHistoryBatchReplaceOp, replaceFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeHistoryBatchReplaceOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   replaceFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeHistoryBatchReplaceOp, replaceFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, true)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeHistoryBatchReplaceOp, replaceFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]
				log.Notice(fmt.Sprintf("[%s] Executing ReplaceBatch for %s.%s with %d rows", writeHistoryBatchReplaceOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.ReplaceBatch(ctx, in.SchemaName, in.Table, csvWithoutHeader, csvColumns, nullStr)
				if err != nil {
					log.Error(fmt.Errorf("[%s] ReplaceBatch failed for %s.%s: %w", writeHistoryBatchReplaceOp, in.SchemaName, in.Table.Name, err))
					return err
				}
			}
			return nil
		}, writeHistoryBatchReplaceOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all replace files for %s.%s", writeHistoryBatchReplaceOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

func (s *Server) processUpdateFiles(
	ctx context.Context,
	in *pb.WriteBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	nullStr string,
	unmodifiedStr string,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.UpdateFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d update files for %s.%s", writeBatchUpdateOp, len(in.UpdateFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, updateFile := range in.UpdateFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeBatchUpdateOp, fileIdx+1, len(in.UpdateFiles), updateFile))

				csvData, err := ReadCSVFile(updateFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeBatchUpdateOp, updateFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeBatchUpdateOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   updateFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeBatchUpdateOp, updateFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, true)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeBatchUpdateOp, updateFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]
				log.Notice(fmt.Sprintf("[%s] Executing UpdateBatch for %s.%s with %d rows", writeBatchUpdateOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.UpdateBatch(ctx, in.SchemaName, in.Table, driverColumns, csvColumns, csvWithoutHeader, nullStr, unmodifiedStr, false)
				if err != nil {
					log.Error(fmt.Errorf("[%s] UpdateBatch failed for %s.%s: %w", writeBatchUpdateOp, in.SchemaName, in.Table.Name, err))
					return err
				}
			}
			return nil
		}, writeBatchUpdateOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all update files for %s.%s", writeBatchUpdateOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

func (s *Server) processUpdateFilesForHistoryBatch(
	ctx context.Context,
	in *pb.WriteHistoryBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	nullStr string,
	unmodifiedStr string,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.UpdateFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d update files for %s.%s", writeHistoryBatchUpdateOp, len(in.UpdateFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, updateFile := range in.UpdateFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeHistoryBatchUpdateOp, fileIdx+1, len(in.UpdateFiles), updateFile))
				csvData, err := ReadCSVFile(updateFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeHistoryBatchUpdateOp, updateFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeHistoryBatchUpdateOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   updateFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeHistoryBatchUpdateOp, updateFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, true)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeHistoryBatchUpdateOp, updateFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]
				log.Notice(fmt.Sprintf("[%s] Executing UpdateBatch for %s.%s with %d rows", writeHistoryBatchUpdateOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.UpdateBatch(ctx, in.SchemaName, in.Table, driverColumns, csvColumns, csvWithoutHeader, nullStr, unmodifiedStr, true)
				if err != nil {
					log.Error(fmt.Errorf("[%s] UpdateBatch failed for %s.%s: %w", writeHistoryBatchUpdateOp, in.SchemaName, in.Table.Name, err))
					return err
				}
			}
			return nil
		}, writeHistoryBatchUpdateOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all update files for %s.%s", writeHistoryBatchUpdateOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

func (s *Server) processDeleteFiles(
	ctx context.Context,
	in *pb.WriteBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.DeleteFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d delete files for %s.%s", writeBatchDeleteOp, len(in.DeleteFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, deleteFile := range in.DeleteFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeBatchDeleteOp, fileIdx+1, len(in.DeleteFiles), deleteFile))
				csvData, err := ReadCSVFile(deleteFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeBatchDeleteOp, deleteFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeBatchDeleteOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   deleteFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeBatchDeleteOp, deleteFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, true)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeBatchDeleteOp, deleteFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]
				log.Notice(fmt.Sprintf("[%s] Executing HardDelete for %s.%s with %d rows", writeBatchDeleteOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.HardDelete(ctx, in.SchemaName, in.Table, csvWithoutHeader, csvColumns)
				if err != nil {
					log.Error(fmt.Errorf("[%s] HardDelete failed for %s.%s: %w", writeBatchDeleteOp, in.SchemaName, in.Table.Name, err))
					return err
				}
			}
			return nil
		}, writeBatchDeleteOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all delete files for %s.%s", writeBatchDeleteOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

func (s *Server) processDeleteFilesForHistoryBatch(
	ctx context.Context,
	in *pb.WriteHistoryBatchRequest,
	conn *db.ClickHouseConnection,
	compression pb.Compression,
	encryption pb.Encryption,
	metadata *types.FivetranTableMetadata,
	driverColumns *types.DriverColumns,
) (err error) {
	if len(in.DeleteFiles) > 0 {
		log.Notice(fmt.Sprintf("[%s] Processing %d delete files for %s.%s", writeHistoryBatchDeleteOp, len(in.DeleteFiles), in.SchemaName, in.Table.Name))
		err = benchmark.RunAndNotice(func() error {
			for fileIdx, deleteFile := range in.DeleteFiles {
				log.Notice(fmt.Sprintf("[%s] Processing file %d/%d: %s", writeHistoryBatchDeleteOp, fileIdx+1, len(in.DeleteFiles), deleteFile))
				csvData, err := ReadCSVFile(deleteFile, in.Keys, compression, encryption)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to read CSV file %s: %w", writeHistoryBatchDeleteOp, deleteFile, err))
					return err
				}
				if len(csvData) < 2 {
					logEmptyCSV(&emptyCSVWarnParams{
						operation:  writeHistoryBatchDeleteOp,
						schemaName: in.SchemaName,
						tableName:  in.Table.Name,
						fileName:   deleteFile,
					})
					continue
				}
				log.Notice(fmt.Sprintf("[%s] File %s contains %d rows (excluding header)", writeHistoryBatchDeleteOp, deleteFile, len(csvData)-1))
				csvColumns, err := types.MakeCSVColumns(csvData[0], driverColumns, metadata.ColumnsMap, false)
				if err != nil {
					log.Error(fmt.Errorf("[%s] Failed to make CSV columns for file %s: %w", writeHistoryBatchDeleteOp, deleteFile, err))
					return err
				}
				csvWithoutHeader := csvData[1:]
				log.Notice(fmt.Sprintf("[%s] Executing UpdateForEarliestStartHistory for %s.%s with %d rows", writeHistoryBatchDeleteOp, in.SchemaName, in.Table.Name, len(csvWithoutHeader)))
				err = conn.UpdateForEarliestStartHistory(ctx, in.SchemaName, in.Table, csvWithoutHeader, csvColumns, constants.FivetranEnd)
				if err != nil {
					log.Error(fmt.Errorf("[%s] UpdateForEarliestStartHistory failed for %s.%s: %w", writeHistoryBatchDeleteOp, in.SchemaName, in.Table.Name, err))
					return err
				}
			}
			return nil
		}, writeHistoryBatchDeleteOp)
		if err != nil {
			return err
		}
		log.Notice(fmt.Sprintf("[%s] Completed processing all delete files for %s.%s", writeHistoryBatchDeleteOp, in.SchemaName, in.Table.Name))
	}
	return nil
}

type emptyCSVWarnParams struct {
	operation  string
	schemaName string
	tableName  string
	fileName   string
}

func logEmptyCSV(params *emptyCSVWarnParams) {
	log.Warn(fmt.Sprintf("[%s] %s.%s got a CSV file %s which contains the header only. Skipping",
		params.operation, params.schemaName, params.tableName, params.fileName))
}

type writeBatchOpType = string

const (
	writeHistoryBatchEarliestStartOp writeBatchOpType = "WriteHistoryBatchRequest(EarliestStart)"
	writeHistoryBatchUpdateOp        writeBatchOpType = "WriteHistoryBatchRequest(Update)"
	writeHistoryBatchReplaceOp       writeBatchOpType = "WriteHistoryBatchRequest(Replace)"
	writeHistoryBatchDeleteOp        writeBatchOpType = "WriteHistoryBatchRequest(Delete)"
	writeHistoryBatchTotalOp         writeBatchOpType = "WriteHistoryBatchRequest(Total)"
	writeBatchReplaceOp              writeBatchOpType = "WriteBatchRequest(Replace)"
	writeBatchUpdateOp               writeBatchOpType = "WriteBatchRequest(Update)"
	writeBatchDeleteOp               writeBatchOpType = "WriteBatchRequest(Delete)"
	writeBatchTotalOp                writeBatchOpType = "WriteBatchRequest(Total)"
)
