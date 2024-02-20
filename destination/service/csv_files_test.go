package service

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"os"
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

var key, _ = base64.StdEncoding.DecodeString("VyEZCkPngvf4mtRHemjGkC6tmd/22j0R9z+DQv2he/Q=")

func TestReadCSVFile(t *testing.T) {
	expectedCSV := readExpectedCSV(t, "../../tests/resources/campaign.csv")
	fileName := "../../tests/resources/campaign.csv.zst.aes"
	records, err := ReadCSVFile(fileName, map[string][]byte{fileName: key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.NoError(t, err)
	assert.Equal(t, expectedCSV, records)

	_, err = ReadCSVFile("nonexistent.csv", map[string][]byte{"nonexistent.csv": key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.ErrorContains(t, err, "file nonexistent.csv does not exist")

	_, err = ReadCSVFile("nonexistent.csv", map[string][]byte{"not-found": key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.ErrorContains(t, err, "key for file nonexistent.csv not found")

	_, err = ReadCSVFile(fileName, map[string][]byte{fileName: []byte("wrong-key")}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.ErrorContains(t, err, "failed to decrypt")

	// Wrong compression type
	_, err = ReadCSVFile(fileName, map[string][]byte{fileName: key}, pb.Compression_GZIP, pb.Encryption_AES)
	assert.ErrorContains(t, err, "gzip: invalid header")

	// Wrong encryption type -> can't decompress
	_, err = ReadCSVFile(fileName, map[string][]byte{fileName: key}, pb.Compression_ZSTD, pb.Encryption_NONE)
	assert.ErrorContains(t, err, "magic number mismatch")

	// File is not a CSV
	invalidCSVFileName := "../../tests/resources/invalid.csv"
	_, err = ReadCSVFile(invalidCSVFileName, map[string][]byte{invalidCSVFileName: key}, pb.Compression_OFF, pb.Encryption_NONE)
	assert.ErrorContains(t, err, "parse error")

	// CSV has column names only
	shortCSVFileName := "../../tests/resources/short.csv"
	_, err = ReadCSVFile(shortCSVFileName, map[string][]byte{shortCSVFileName: key}, pb.Compression_OFF, pb.Encryption_NONE)
	assert.ErrorContains(t, err, "expected to have more than 1 line in file")
}

func readExpectedCSV(t *testing.T, path string) [][]string {
	expectedBytes, err := os.ReadFile(path)
	assert.NoError(t, err)
	expectedCSV, err := csv.NewReader(bytes.NewBuffer(expectedBytes)).ReadAll()
	assert.NoError(t, err)
	return expectedCSV[1:] // skip the column names
}
