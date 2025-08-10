package config

type Encryption struct {
	Age Age `mapstructure:"age" json:"age"`
}

type Age struct {
	RecipientPublicKey string `mapstructure:"recipient_public_key" json:"recipient_public_key"`
}
