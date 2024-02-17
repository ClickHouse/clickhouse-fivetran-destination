package service

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/csv"
	"fmt"
	"io"
	"os"

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
	file, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("file %s does not exist", fileName)
	}
	decrypted, err := Decrypt(key, file, encryption)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt file %s, cause: %w", fileName, err)
	}
	decompressed, err := Decompress(decrypted, compression)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress file %s, cause: %w", fileName, err)
	}
	csvReader := csv.NewReader(bytes.NewReader(decompressed))
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("file %s is not a valid CSV, cause: %w", fileName, err)
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("expected to have more than 1 line in file %s", fileName)
	}
	return records[1:], nil // skip the column names
}

func Decrypt(key []byte, data []byte, encryption pb.Encryption) ([]byte, error) {
	switch encryption {
	case pb.Encryption_AES:
		return DecryptAES256(key, data)
	default:
		return data, nil
	}
}

func DecryptAES256(key []byte, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	if len(data) < block.BlockSize() {
		return nil, err
	}
	iv, data := data[:block.BlockSize()], data[block.BlockSize():]
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(data, data)
	return PKCS5Padding(data), nil
}

func PKCS5Padding(data []byte) []byte {
	padding := data[len(data)-1]
	return data[:len(data)-int(padding)]
}

func Decompress(data []byte, compression pb.Compression) ([]byte, error) {
	var (
		res []byte
		err error
	)
	switch compression {
	case pb.Compression_ZSTD:
		res, err = DecompressZSTD(data)
	case pb.Compression_GZIP:
		res, err = DecompressGZIP(data)
	case pb.Compression_OFF:
		res, err = data, nil
	}
	return res, err
}

var zstdReader, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

func DecompressZSTD(data []byte) ([]byte, error) {
	return zstdReader.DecodeAll(data, nil)
}

func DecompressGZIP(data []byte) ([]byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()
	res, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, err
	}
	return res, nil
}
