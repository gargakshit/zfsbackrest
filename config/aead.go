package config

type Encryption struct {
	Age Age `json:"age"`
}

type Age struct {
	RecipientPublicKey string `json:"recipient_public_key"`
}
