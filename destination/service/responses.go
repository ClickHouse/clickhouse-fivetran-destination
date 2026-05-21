package service

import (
	"context"
	"errors"
	"fmt"

	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/retry"
	"fivetran.com/fivetran_sdk/destination/db/config"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
)

var hostDescription = "ClickHouse Cloud service host without protocol or port. For example, my.service.clickhouse.cloud"
var portDescription = "ClickHouse Cloud service native protocol SSL/TLS port. Default is 9440"
var advancedConfigDescription = "Optional JSON configuration file for fine-tuning destination behavior. See the documentation for the file schema"

func GetConfigurationFormResponse() *pb.ConfigurationFormResponse {
	isHostRequired := true
	isPortRequired := false
	isUserNameRequired := true
	isPasswordRequired := true
	isNotRequired := false
	return &pb.ConfigurationFormResponse{
		SchemaSelectionSupported: true,
		TableSelectionSupported:  true,
		Fields: []*pb.FormField{
			{
				Name:        config.HostKey,
				Label:       "Host",
				Description: &hostDescription,
				Required:    &isHostRequired,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:        config.PortKey,
				Label:       "Port",
				Description: &portDescription,
				Required:    &isPortRequired,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     config.UsernameKey,
				Label:    "Username",
				Required: &isUserNameRequired,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     config.PasswordKey,
				Label:    "Password",
				Required: &isPasswordRequired,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_Password,
				},
			},
			{
				Name:        config.AdvancedConfigKey,
				Label:       "Advanced Configuration",
				Description: &advancedConfigDescription,
				Required:    &isNotRequired,
				Type: &pb.FormField_UploadField{
					UploadField: &pb.UploadField{
						AllowedFileType:  []string{".json"},
						MaxFileSizeBytes: 1_048_576, // 1 MiB
					},
				},
			},
		},
		Tests: []*pb.ConfigurationTest{
			{
				Name:  ConnectionTest,
				Label: "Connection test",
			},
			{
				Name:  GrantsTest,
				Label: "User grants test",
			},
		},
	}
}

func FailedWriteBatchResponse(schemaName string, tableName string, err error) *pb.WriteBatchResponse {
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Task{
			Task: toTask(addUserReadableHintsToError(fmt.Sprintf("Failed to write batch into `%s`.`%s`", schemaName, tableName), err)),
		},
	}
}

func FailedWriteHistoryBatchResponse(schemaName string, tableName string, err error) *pb.WriteBatchResponse {
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Task{
			Task: toTask(addUserReadableHintsToError(fmt.Sprintf("Failed to write history batch into `%s`.`%s`", schemaName, tableName), err)),
		},
	}
}

func FailedDescribeTableResponse(schemaName string, tableName string, err error) *pb.DescribeTableResponse {
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Task{
			Task: toTask(addUserReadableHintsToError(fmt.Sprintf("Failed to describe table `%s`.`%s`", schemaName, tableName), err)),
		},
	}
}

func NotFoundDescribeTableResponse() *pb.DescribeTableResponse {
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_NotFound{NotFound: true},
	}
}

func FailedTestResponse(name string, err error) *pb.TestResponse {
	return &pb.TestResponse{
		Response: &pb.TestResponse_Failure{
			Failure: addUserReadableHintsToError(fmt.Sprintf("Test %s failed", name), err),
		},
	}
}

func FailedCreateTableResponse(schemaName string, tableName string, err error) *pb.CreateTableResponse {
	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Task{
			Task: toTask(addUserReadableHintsToError(fmt.Sprintf("Failed to create table `%s`.`%s`", schemaName, tableName), err)),
		},
	}
}

func FailedAlterTableResponse(schemaName string, tableName string, err error) *pb.AlterTableResponse {
	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Task{
			Task: toTask(addUserReadableHintsToError(fmt.Sprintf("Failed to alter table `%s`.`%s`", schemaName, tableName), err)),
		},
	}
}

func SuccessfulTruncateTableResponse() *pb.TruncateResponse {
	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Success{
			Success: true,
		},
	}
}

func FailedTruncateTableResponse(schemaName string, tableName string, err error) *pb.TruncateResponse {
	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Task{
			Task: toTask(addUserReadableHintsToError(fmt.Sprintf("Failed to truncate table `%s`.`%s`", schemaName, tableName), err)),
		},
	}
}

func SuccessfulMigrateResponse() *pb.MigrateResponse {
	return &pb.MigrateResponse{
		Response: &pb.MigrateResponse_Success{
			Success: true,
		},
	}
}

func UnsupportedMigrateResponse() *pb.MigrateResponse {
	return &pb.MigrateResponse{
		Response: &pb.MigrateResponse_Unsupported{
			Unsupported: true,
		},
	}
}

func FailedMigrateResponse(schemaName string, tableName string, err error) *pb.MigrateResponse {
	logError("Migrate", err)
	return &pb.MigrateResponse{
		Response: &pb.MigrateResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to migrate `%s`.`%s`, cause: %s", schemaName, tableName, err)),
		},
	}
}

func logError(endpoint string, err error) {
	log.Error(fmt.Errorf("%s failed: %w", endpoint, err))
}

func toTask(taskMessage string) *pb.Task {
	return &pb.Task{
		Message: taskMessage,
	}
}

// ClickHouse server error codes used to translate driver errors into a
// friendly Task message via addUserReadableHintsToError.
// Reference: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
const (
	chCodeUnknownTable         = 60
	chCodeDatabaseDoesNotExist = 81
	chCodeTimeoutExceeded      = 159
	chCodeUnknownUser          = 192
	chCodeSocketTimeout        = 209
	chCodeNotEnoughPrivileges  = 497
	chCodeAuthenticationFailed = 516
)

// addUserReadableHintsToError produces the user-facing Task message that Fivetran will display:
//
//	"<operation>: <friendly message> Technical details: <err>"
func addUserReadableHintsToError(operation string, err error) string {
	friendly := "Unexpected error in the ClickHouse destination. Please contact Fivetran support and include the technical details below."

	var ex *clickhouse.Exception
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		friendly = "The ClickHouse operation took too long to complete. Retry the sync. If the problem persists, check the performance of the SQL executed. You may need to optimize batch sizes or scale up the ClickHouse service."
	case errors.Is(err, context.Canceled):
		friendly = "The operation was cancelled before ClickHouse could complete it. Retry the sync."
	case errors.As(err, &ex):
		switch ex.Code {
		case chCodeAuthenticationFailed, chCodeUnknownUser:
			friendly = "ClickHouse rejected the credentials. Verify the username and password configured for the destination."
		case chCodeNotEnoughPrivileges:
			friendly = "The ClickHouse user is missing required privileges. Re-run the grants test and apply the privileges listed in the documentation."
		case chCodeDatabaseDoesNotExist, chCodeUnknownTable:
			friendly = "The target database or table does not exist in ClickHouse. Verify the schema and table names; the destination will create them on the next sync if needed."
		case chCodeTimeoutExceeded, chCodeSocketTimeout:
			friendly = "The ClickHouse query took too long to complete. Retry the sync. If the problem persists, check the performance of the SQL executed. You may need to optimize batch sizes or scale up the ClickHouse service."
		}
	case retry.IsNetError(err):
		friendly = "Could not reach the ClickHouse service. Verify the ClickHouse Cloud service is running and reachable from Fivetran (host, port, IP allowlist)."
	}

	return fmt.Sprintf("%s: %s Technical details: %s", operation, friendly, err)
}
