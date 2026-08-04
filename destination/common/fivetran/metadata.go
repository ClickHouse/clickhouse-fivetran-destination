package fivetran

import (
	"os"
	"strings"
)

// Environment variables with runtime metadata provided by the Fivetran platform, see:
// https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide/development-guide.md#environment-variables
const (
	AccountNameEnvVar = "FIVETRAN_ACCOUNT_NAME"
	GroupNameEnvVar   = "FIVETRAN_GROUP_NAME"
)

// Metadata contains the Fivetran runtime metadata attributes.
// Fields are empty when the corresponding environment variables are not set,
// e.g. when running locally or with the SDK tester.
type Metadata struct {
	AccountName string
	GroupName   string
}

// GetMetadata reads the Fivetran runtime metadata from the environment.
func GetMetadata() Metadata {
	return Metadata{
		AccountName: sanitize(os.Getenv(AccountNameEnvVar)),
		GroupName:   sanitize(os.Getenv(GroupNameEnvVar)),
	}
}

// String renders the metadata as a comma-separated list of key-value pairs
// Returns an empty string when no metadata is set.
func (m Metadata) String() string {
	var parts []string
	if m.AccountName != "" {
		parts = append(parts, "fivetran_account_name: "+m.AccountName)
	}
	if m.GroupName != "" {
		parts = append(parts, "fivetran_group_name: "+m.GroupName)
	}
	return strings.Join(parts, ", ")
}

// sanitize strips line breaks so the value cannot terminate a single-line SQL comment.
func sanitize(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}
