package fivetran

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMetadata(t *testing.T) {
	tests := []struct {
		name        string
		accountName string
		groupName   string
		expected    Metadata
	}{
		{
			name:     "no env vars set",
			expected: Metadata{},
		},
		{
			name:        "both env vars set",
			accountName: "acme",
			groupName:   "my_destination",
			expected:    Metadata{AccountName: "acme", GroupName: "my_destination"},
		},
		{
			name:        "values are trimmed and line breaks are stripped",
			accountName: "  acme\ncorp\r\n",
			groupName:   "\tmy_destination ",
			expected:    Metadata{AccountName: "acme corp", GroupName: "my_destination"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(AccountNameEnvVar, tt.accountName)
			t.Setenv(GroupNameEnvVar, tt.groupName)
			assert.Equal(t, tt.expected, GetMetadata())
		})
	}
}

func TestMetadataString(t *testing.T) {
	tests := []struct {
		name     string
		metadata Metadata
		expected string
	}{
		{
			name:     "no metadata",
			metadata: Metadata{},
			expected: "",
		},
		{
			name:     "account name only",
			metadata: Metadata{AccountName: "acme"},
			expected: "fivetran_account_name: acme",
		},
		{
			name:     "group name only",
			metadata: Metadata{GroupName: "my_destination"},
			expected: "fivetran_group_name: my_destination",
		},
		{
			name:     "both fields",
			metadata: Metadata{AccountName: "acme", GroupName: "my_destination"},
			expected: "fivetran_account_name: acme, fivetran_group_name: my_destination",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.metadata.String())
		})
	}
}
