package aes

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"os"
)

const FileReadBufferSize = aes.BlockSize * 1024 * 1024

// Decoder for AES-256 taking PKCS #5 Padding into consideration.
type Decoder struct {
	mode        cipher.BlockMode
	file        *os.File
	fileLen     int64  // remaining file length to be read
	buf         []byte // operational buffer; variable size during the reads
	fileReadBuf []byte // file read buffer; size/capacity should NOT be modified
	isClosed    bool
}

func (r *Decoder) Read(dest []byte) (readBytes int, err error) {
	if len(dest) == 0 {
		return 0, fmt.Errorf("destination buffer cannot be empty")
	}
	if r.isClosed {
		return 0, io.ErrClosedPipe
	}
	if r.fileLen == 0 && len(r.buf) == 0 {
		return 0, io.EOF
	}
	offset := 0
	if len(r.buf) > 0 {
		if len(dest) < len(r.buf) {
			copy(dest, r.buf[:len(dest)])
			r.buf = r.buf[len(dest):]
			return len(dest), nil
		} else if len(dest) == len(r.buf) {
			copy(dest, r.buf)
			r.buf = nil
			if r.fileLen == 0 {
				return len(dest), io.EOF
			} else {
				return len(dest), nil
			}
		} else {
			offset = copy(dest, r.buf)
			if r.fileLen == 0 {
				return offset, io.EOF
			}
		}
		r.buf = nil
	}
	err = r.decryptNextFileBlocksToBuffer()
	if err != nil {
		return 0, err
	}
	if len(dest)-offset < len(r.buf) {
		copy(dest[offset:], r.buf[:len(dest)-offset])
		r.buf = r.buf[len(dest)-offset:]
		return len(dest), nil
	} else {
		copied := copy(dest[offset:], r.buf)
		r.buf = nil
		if r.fileLen == 0 {
			return copied, io.EOF
		}
		return copied, nil
	}
}

// Close does not close the underlying file.
func (r *Decoder) Close() {
	r.isClosed = true
}

func (r *Decoder) decryptNextFileBlocksToBuffer() error {
	if len(r.buf) != 0 {
		return fmt.Errorf("operational buffer is not empty")
	}
	n, err := r.file.Read(r.fileReadBuf)
	if err != nil {
		return fmt.Errorf("failed to read the next AES blocks, cause: %w", err)
	}
	if n%aes.BlockSize != 0 {
		return fmt.Errorf("expected to read a multiple of %d bytes, but read %d", aes.BlockSize, n)
	}
	r.buf = r.fileReadBuf[:n]
	r.mode.CryptBlocks(r.buf, r.buf)
	r.fileLen -= int64(n)
	if r.fileLen == 0 {
		padding := int(r.fileReadBuf[n-1])
		r.buf = r.buf[:n-padding]
	}
	return nil
}

func NewReader(file *os.File, key []byte) (*Decoder, error) {
	if file == nil {
		return nil, errors.New("file cannot be nil")
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < aes.BlockSize*2 { // at least IV and padding blocks
		return nil, fmt.Errorf("file is expected to be at least %d bytes", aes.BlockSize*2)
	}
	iv := make([]byte, aes.BlockSize)
	_, err = file.Read(iv)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	mode := cipher.NewCBCDecrypter(block, iv)

	readBuffer := make([]byte, FileReadBufferSize)
	fileLen := stat.Size() - aes.BlockSize // minus previously read IV block
	return &Decoder{
		mode:        mode,
		file:        file,
		fileReadBuf: readBuffer,
		buf:         nil,
		fileLen:     fileLen,
		isClosed:    false,
	}, nil
}
