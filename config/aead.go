package config

type Encryption struct {
	Age Age
}

type Age struct {
	RecipientPublicKey string
}
