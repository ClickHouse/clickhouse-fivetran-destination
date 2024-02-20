package service

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"fivetran.com/fivetran_sdk/destination/encryption/aes"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

func ReadCSVFile(
	fileName string,
	keys map[string][]byte,
	compression pb.Compression,
	encryption pb.Encryption,
) ([][]string, error) {
	key, ok := keys[fileName]
	if !ok {
		return nil, fmt.Errorf("key for file %s not found", fileName)
	}
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("file %s does not exist", fileName)
	}
	defer file.Close()

	var decryptedReader io.Reader
	if encryption == pb.Encryption_AES {
		aesReader, err := aes.NewReader(file, key)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt file %s, cause: %w", fileName, err)
		}
		defer aesReader.Close()
		decryptedReader = aesReader
	} else {
		decryptedReader = file
	}

	var decompressedReader io.Reader
	switch compression {
	case pb.Compression_ZSTD:
		zstdReader, err := zstd.NewReader(decryptedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress file %s with ZSTD, cause: %w", fileName, err)
		}
		defer zstdReader.Close()
		decompressedReader = zstdReader
	case pb.Compression_GZIP:
		gzipReader, err := gzip.NewReader(decryptedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress file %s with GZIP, cause: %w", fileName, err)
		}
		defer gzipReader.Close()
		decompressedReader = gzipReader
	case pb.Compression_OFF:
		decompressedReader = decryptedReader
	}

	csvReader := csv.NewReader(decompressedReader)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to load CSV file %s with encryption %s and compression %s, cause: %w",
			fileName, encryption.String(), compression.String(), err)
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("expected to have more than 1 line in file %s", fileName)
	}

	return records[1:], nil // skip the column names
}
