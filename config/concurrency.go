package config

type UploadConcurrency struct {
	Full int `mapstructure:"full"`
	Diff int `mapstructure:"diff"`
	Incr int `mapstructure:"incr"`
}
