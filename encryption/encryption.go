package encryption

import (
	"io"

	"github.com/gargakshit/zfsbackrest/config"
)

type Encryption interface {
	EncryptedWriter(dst io.Writer) (io.WriteCloser, error)
	DecryptedReader(src io.ReadCloser) (io.ReadCloser, error)
}

func NewEncryption(encryptionConfig *config.Encryption) (Encryption, error) {
	return NewAge(&encryptionConfig.Age)
}
