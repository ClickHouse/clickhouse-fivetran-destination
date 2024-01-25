package main

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const Content = "foobar\nqazqux\n123!@\n\n"

func TestDecompressZSTD(t *testing.T) {
	file, err := os.ReadFile("../tests/resources/encoded.txt.zst")
	assert.NoError(t, err)
	res, err := DecompressZSTD(file)
	assert.NoError(t, err)
	assert.Equal(t, Content, string(res))
}

func TestDecompressGZIP(t *testing.T) {
	file, err := os.ReadFile("../tests/resources/encoded.txt.gz")
	assert.NoError(t, err)
	res, err := DecompressGZIP(file)
	assert.NoError(t, err)
	assert.Equal(t, Content, string(res))
}

func TestDecryptAESWithZSTD(t *testing.T) {
	key, _ := base64.StdEncoding.DecodeString("VyEZCkPngvf4mtRHemjGkC6tmd/22j0R9z+DQv2he/Q=")
	file, err := os.ReadFile("../tests/resources/campaign.csv.zst.aes")
	assert.NoError(t, err)
	decrypted, err := DecryptAES256(key, file)
	assert.NoError(t, err)
	decompressed, err := DecompressZSTD(decrypted)
	assert.NoError(t, err)
	expected, err := os.ReadFile("../tests/resources/campaign.csv")
	assert.NoError(t, err)
	assert.Equal(t, string(expected), string(decompressed))
}
