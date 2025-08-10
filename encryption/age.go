package encryption

import (
	"io"
	"log/slog"

	"filippo.io/age"
	"github.com/gargakshit/zfsbackrest/config"
)

type Age struct {
	RecipientPublicKey *age.X25519Recipient
}

func NewAge(ageConfig *config.Age) (*Age, error) {
	recipient, err := age.ParseX25519Recipient(ageConfig.RecipientPublicKey)
	if err != nil {
		slog.Error("Failed to parse age recipient public key", "error", err)
		return nil, err
	}

	return &Age{
		RecipientPublicKey: recipient,
	}, nil
}

func (a *Age) EncryptedWriter(dst io.Writer) (io.WriteCloser, error) {
	return age.Encrypt(dst, a.RecipientPublicKey)
}

func ValidateRecipientPublicKey(recipientPublicKey string) error {
	_, err := age.ParseX25519Recipient(recipientPublicKey)
	if err != nil {
		slog.Error("Failed to parse age recipient public key", "error", err)
		return err
	}

	return nil
}
