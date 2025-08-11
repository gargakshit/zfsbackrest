package encryption

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"filippo.io/age"
	"github.com/gargakshit/zfsbackrest/config"
)

type Age struct {
	RecipientPublicKey *age.X25519Recipient
	Identity           *age.X25519Identity
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

func NewAgeFromIdentity(identityContent string, ageConfig *config.Age) (*Age, error) {
	identity, err := age.ParseX25519Identity(strings.TrimSpace(identityContent))
	if err != nil {
		slog.Error("Failed to parse age identity", "error", err)
		return nil, err
	}

	recipient, err := age.ParseX25519Recipient(ageConfig.RecipientPublicKey)
	if err != nil {
		slog.Error("Failed to parse age recipient public key", "error", err)
		return nil, err
	}

	if recipient.String() != identity.Recipient().String() {
		slog.Error("Recipient public key does not match identity", "recipient", recipient.String(), "identity", identity.Recipient().String())
		return nil, fmt.Errorf("recipient public key does not match identity")
	}

	return &Age{
		RecipientPublicKey: recipient,
		Identity:           identity,
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

func (a *Age) DecryptedReader(src io.Reader) (io.Reader, error) {
	if a.Identity == nil {
		slog.Error("Identity is not set. Please use NewAgeFromIdentity to create an Age instance with an identity.")
		return nil, fmt.Errorf("identity is not set. Please use NewAgeFromIdentity to create an Age instance with an identity.")
	}

	return age.Decrypt(src, a.Identity)
}
