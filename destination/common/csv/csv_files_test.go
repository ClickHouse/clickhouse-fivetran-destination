package csv

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

func TestCSVFileReaderStreaming(t *testing.T) {
	expectedCSV := readExpectedCSV(t, "../../../tests/resources/campaign.csv")
	fileName := "../../../tests/resources/campaign.csv.zst.aes"

	reader, err := NewCSVFileReader(fileName, map[string][]byte{fileName: key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, expectedCSV[0], reader.Header())

	// Read all data rows in one batch
	batch, err := reader.ReadBatch(100)
	assert.NoError(t, err)
	assert.Equal(t, expectedCSV[1:], batch)

	// Next read should return nil (EOF)
	batch, err = reader.ReadBatch(100)
	assert.NoError(t, err)
	assert.Nil(t, batch)
}

func TestCSVFileReaderBatchedReading(t *testing.T) {
	expectedCSV := readExpectedCSV(t, "../../../tests/resources/campaign.csv")
	fileName := "../../../tests/resources/campaign.csv.zst.aes"

	reader, err := NewCSVFileReader(fileName, map[string][]byte{fileName: key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, expectedCSV[0], reader.Header())

	// Read one row at a time (batch size = 1)
	batch1, err := reader.ReadBatch(1)
	assert.NoError(t, err)
	assert.Len(t, batch1, 1)
	assert.Equal(t, expectedCSV[1], batch1[0])

	batch2, err := reader.ReadBatch(1)
	assert.NoError(t, err)
	assert.Len(t, batch2, 1)
	assert.Equal(t, expectedCSV[2], batch2[0])

	// No more rows
	batch3, err := reader.ReadBatch(1)
	assert.NoError(t, err)
	assert.Nil(t, batch3)
}

func TestCSVFileReaderHeaderOnly(t *testing.T) {
	fileName := "../../../tests/resources/short.csv"

	reader, err := NewCSVFileReader(fileName, map[string][]byte{fileName: key}, pb.Compression_OFF, pb.Encryption_NONE)
	assert.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, []string{"foo", "bar"}, reader.Header())

	// No data rows -- header only
	batch, err := reader.ReadBatch(100)
	assert.NoError(t, err)
	assert.Nil(t, batch)
}

func TestCSVFileReaderErrors(t *testing.T) {
	fileName := "../../../tests/resources/campaign.csv.zst.aes"

	// File not found
	_, err := NewCSVFileReader("nonexistent.csv", map[string][]byte{"nonexistent.csv": key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.ErrorContains(t, err, "failed to open file nonexistent.csv")

	// Key was not provided
	_, err = NewCSVFileReader("nonexistent.csv", map[string][]byte{"not-found": key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.ErrorContains(t, err, "key for file nonexistent.csv not found")

	// Wrong key
	_, err = NewCSVFileReader(fileName, map[string][]byte{fileName: []byte("wrong-key")}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.ErrorContains(t, err, "failed to decrypt")

	// Wrong compression type
	_, err = NewCSVFileReader(fileName, map[string][]byte{fileName: key}, pb.Compression_GZIP, pb.Encryption_AES)
	assert.ErrorContains(t, err, "gzip: invalid header")

	// Wrong encryption type -> can't decompress
	_, err = NewCSVFileReader(fileName, map[string][]byte{fileName: key}, pb.Compression_ZSTD, pb.Encryption_NONE)
	assert.ErrorContains(t, err, "magic number mismatch")

	// File is not a valid CSV (parse error surfaces during ReadBatch, not during construction)
	invalidCSVFileName := "../../../tests/resources/invalid.csv"
	invalidReader, err := NewCSVFileReader(invalidCSVFileName, map[string][]byte{invalidCSVFileName: key}, pb.Compression_OFF, pb.Encryption_NONE)
	assert.NoError(t, err)
	_, err = invalidReader.ReadBatch(100)
	assert.ErrorContains(t, err, "parse error")
	invalidReader.Close()

	// CSV is empty
	emptyCSVName := "../../../tests/resources/empty.csv"
	_, err = NewCSVFileReader(emptyCSVName, map[string][]byte{emptyCSVName: key}, pb.Compression_OFF, pb.Encryption_NONE)
	assert.ErrorContains(t, err, "received an empty CSV file")

	// Batch size is 0
	validReader, err := NewCSVFileReader(fileName, map[string][]byte{fileName: key}, pb.Compression_ZSTD, pb.Encryption_AES)
	assert.NoError(t, err)
	_, err = validReader.ReadBatch(0)
	assert.ErrorContains(t, err, "batchSize must be greater than 0")
	validReader.Close()
}

func readExpectedCSV(t *testing.T, path string) [][]string {
	expectedBytes, err := os.ReadFile(path)
	assert.NoError(t, err)
	expectedCSV, err := csv.NewReader(bytes.NewBuffer(expectedBytes)).ReadAll()
	assert.NoError(t, err)
	return expectedCSV
}
