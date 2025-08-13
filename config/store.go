package config

type S3Store struct {
	Endpoint string `mapstructure:"endpoint"`
	Bucket   string `mapstructure:"bucket"`
	Key      string `mapstructure:"key"`
	Secret   string `mapstructure:"secret"`
	Region   string `mapstructure:"region"`

	PartSize      uint64 `mapstructure:"part_size"`
	UploadThreads uint   `mapstructure:"upload_threads"`
}
