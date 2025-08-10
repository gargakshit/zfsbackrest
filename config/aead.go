package config

type Encryption struct {
	Age Age `mapstructure:"age"`
}

type Age struct {
	RecipientPublicKey string `mapstructure:"recipient_public_key"`
}
