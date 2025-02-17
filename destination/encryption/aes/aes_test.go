package aes

import (
	"encoding/base64"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var key, _ = base64.StdEncoding.DecodeString("VyEZCkPngvf4mtRHemjGkC6tmd/22j0R9z+DQv2he/Q=")

var fileName = "../../../tests/resources/campaign.csv.zst.aes"
var expectedFileName = "../../../tests/resources/campaign.csv.zst"

func TestAES256ReaderReadAll(t *testing.T) {
	file, err := os.Open(fileName)
	assert.NoError(t, err)
	defer file.Close()
	expected, err := os.ReadFile(expectedFileName)
	assert.NoError(t, err)

	reader, err := NewReader(file, key)
	assert.NoError(t, err)
	assert.IsType(t, &Decoder{}, reader)

	result, err := io.ReadAll(reader)
	assert.NoError(t, err)

	assert.Equal(t, len(expected), len(result))
	assert.Equal(t, expected, result)

	assert.False(t, reader.isClosed)
	reader.Close()
	assert.True(t, reader.isClosed)

	n, err := reader.Read(make([]byte, 1))
	assert.Equal(t, 0, n)
	assert.Equal(t, err, io.ErrClosedPipe)
}

func TestAES256ReaderVariousBufferSizes(t *testing.T) {
	expected, err := os.ReadFile(expectedFileName)
	assert.NoError(t, err)

	bufferSizes := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 50, 100, 1000}
	for _, bufferSize := range bufferSizes {
		file, err := os.Open(fileName)
		assert.NoError(t, err)
		reader, err := NewReader(file, key)
		assert.NoError(t, err)
		result := readWithBuffer(t, reader, make([]byte, bufferSize))
		assert.Equal(t, len(expected), len(result), "Buffer size: %d", bufferSize)
		assert.Equal(t, expected, result, "Buffer size: %d", bufferSize)
		reader.Close()
		file.Close()
	}
}

func TestAES256ReaderInvalidOpenFile(t *testing.T) {
	_, err := NewReader(nil, key)
	assert.ErrorContains(t, err, "file cannot be nil")

	file, err := os.Open(fileName)
	assert.NoError(t, err)
	file.Close()

	_, err = NewReader(file, key)
	assert.Error(t, err)

	file2, err := os.Open(fileName)
	assert.NoError(t, err)
	defer file2.Close()
	_, err = io.ReadAll(file2)
	assert.NoError(t, err)
	_, err = NewReader(file2, key)
	assert.Equal(t, err, io.EOF)
}

func readWithBuffer(t *testing.T, reader io.Reader, buf []byte) (res []byte) {
	for {
		n, err := reader.Read(buf)
		res = append(res, buf[:n]...)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		if err != nil {
			break
		}
	}
	return res
}
