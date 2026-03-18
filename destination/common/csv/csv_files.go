package csv

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

// CSVFileReader provides streaming access to a CSV file,
// handling decryption and decompression transparently.
// The header is read eagerly on construction; subsequent data rows
// are read in batches via ReadBatch to limit memory usage.
type CSVFileReader struct {
	fileName  string
	csvReader *csv.Reader
	header    []string
	done      bool
	// Keep track of the open files to close them in the Close method
	file        *os.File
	aesDecoder  *aes.Decoder
	zstdDecoder *zstd.Decoder
	gzipReader  *gzip.Reader
}

func NewCSVFileReader(
	fileName string,
	keys map[string][]byte,
	compression pb.Compression,
	encryption pb.Encryption,
) (*CSVFileReader, error) {
	key, ok := keys[fileName]
	if !ok {
		return nil, fmt.Errorf("key for file %s not found", fileName)
	}
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}

	r := &CSVFileReader{
		fileName: fileName,
		file:     file,
	}

	var decryptedReader io.Reader
	if encryption == pb.Encryption_AES {
		aesReader, err := aes.NewReader(file, key)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("failed to decrypt file %s, cause: %w", fileName, err)
		}
		r.aesDecoder = aesReader
		decryptedReader = aesReader
	} else {
		decryptedReader = file
	}

	var decompressedReader io.Reader
	switch compression {
	case pb.Compression_ZSTD:
		zstdReader, err := zstd.NewReader(decryptedReader)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("failed to decompress file %s with ZSTD, cause: %w", fileName, err)
		}
		r.zstdDecoder = zstdReader
		decompressedReader = zstdReader
	case pb.Compression_GZIP:
		gzipReader, err := gzip.NewReader(decryptedReader)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("failed to decompress file %s with GZIP, cause: %w", fileName, err)
		}
		r.gzipReader = gzipReader
		decompressedReader = gzipReader
	case pb.Compression_OFF:
		decompressedReader = decryptedReader
	}

	r.csvReader = csv.NewReader(decompressedReader)

	header, err := r.csvReader.Read()
	if err == io.EOF {
		r.Close()
		return nil, fmt.Errorf("received an empty CSV file %s without a header", fileName)
	}
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("failed to read CSV header from file %s: %w", fileName, err)
	}
	r.header = header

	return r, nil
}

func (r *CSVFileReader) Header() []string {
	return r.header
}

// ReadBatch reads up to batchSize data rows from the CSV.
// Returns (nil, nil) when there are no more rows to read.
func (r *CSVFileReader) ReadBatch(batchSize uint) ([][]string, error) {
	if r.done {
		return nil, nil
	}
	batch := make([][]string, 0, batchSize)
	for range batchSize {
		record, err := r.csvReader.Read()
		if err == io.EOF {
			r.done = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row from file %s: %w", r.fileName, err)
		}
		batch = append(batch, record)
	}
	if len(batch) == 0 {
		return nil, nil
	}
	return batch, nil
}

func (r *CSVFileReader) Close() {
	if r.gzipReader != nil {
		r.gzipReader.Close() //nolint:errcheck
	}
	if r.zstdDecoder != nil {
		r.zstdDecoder.Close()
	}
	if r.aesDecoder != nil {
		r.aesDecoder.Close()
	}
	if r.file != nil {
		r.file.Close() //nolint:errcheck
	}
}
