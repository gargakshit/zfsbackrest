package config

type S3Store struct {
	Endpoint string `mapstructure:"endpoint"`
	Bucket   string `mapstructure:"bucket"`
	Key      string `mapstructure:"key"`
	Secret   string `mapstructure:"secret"`
	Region   string `mapstructure:"region"`
}
