package service

import (
	"fmt"

	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/db/config"
	pb "fivetran.com/fivetran_sdk/proto"
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
	logError("WriteBatch", err)
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to write batch into `%s`.`%s`, cause: %s", schemaName, tableName, err)),
		},
	}
}

func FailedWriteHistoryBatchResponse(schemaName string, tableName string, err error) *pb.WriteBatchResponse {
	logError("WriteHistoryBatch", err)
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to write history batch into `%s`.`%s`, cause: %s", schemaName, tableName, err)),
		},
	}
}

func FailedDescribeTableResponse(schemaName string, tableName string, err error) *pb.DescribeTableResponse {
	logError("DescribeTable", err)
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to describe table `%s`.`%s`, cause: %s", schemaName, tableName, err)),
		},
	}
}

func NotFoundDescribeTableResponse() *pb.DescribeTableResponse {
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_NotFound{NotFound: true},
	}
}

func FailedTestResponse(name string, err error) *pb.TestResponse {
	logError("Test", err)
	return &pb.TestResponse{
		Response: &pb.TestResponse_Failure{
			Failure: fmt.Sprintf("Test %s failed, cause: %s", name, err),
		},
	}
}

func FailedCreateTableResponse(schemaName string, tableName string, err error) *pb.CreateTableResponse {
	logError("CreateTable", err)
	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to create table `%s`.`%s`, cause: %s", schemaName, tableName, err)),
		},
	}
}

func FailedAlterTableResponse(schemaName string, tableName string, err error) *pb.AlterTableResponse {
	logError("AlterTable", err)
	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to alter table `%s`.`%s`, cause: %s", schemaName, tableName, err)),
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
	logError("TruncateTable", err)
	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Task{
			Task: toTask(fmt.Sprintf("Failed to truncate table `%s`.`%s`, cause: %s", schemaName, tableName, err)),
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
