package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"io"
	"os"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

type ReadCSVFileResultType int

const (
	Success ReadCSVFileResultType = iota
	KeyNotFound
	FileNotFound
	FailedToDecompress
	FailedToDecrypt
)

type ReadCSVFileResult struct {
	Type  ReadCSVFileResultType
	Data  *[]byte // only set if Type is Success
	Error *error  // only set if Type is FailedToDecrypt or FailedToDecompress
}

func ReadCSVFile(
	fileName string,
	keys map[string][]byte,
	compression pb.Compression,
	encryption pb.Encryption,
) *ReadCSVFileResult {
	key, ok := keys[fileName]
	if !ok {
		return &ReadCSVFileResult{Type: KeyNotFound}
	}
	fileContent, err := os.ReadFile(fileName)
	if err != nil {
		return &ReadCSVFileResult{Type: FileNotFound}
	}
	decrypted, err := Decrypt(key, fileContent, encryption)
	if err != nil {
		return &ReadCSVFileResult{Type: FailedToDecrypt, Error: &err}
	}
	decompressed, err := Decompress(decrypted, compression)
	if err != nil {
		return &ReadCSVFileResult{Type: FailedToDecompress, Error: &err}
	}
	return &ReadCSVFileResult{Type: Success, Data: &decompressed}
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
	decrypted := make([]byte, len(data))
	mode.CryptBlocks(decrypted, data)
	return PKCS5Padding(decrypted), nil
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
	res, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, err
	}
	err = gzipReader.Close()
	if err != nil {
		return nil, err
	}
	return res, nil
}
