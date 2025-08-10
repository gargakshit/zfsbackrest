package config

import "time"

type Repository struct {
	Expiry           Expiry           `mapstructure:"expiry"`
	S3               S3Store          `mapstructure:"s3"`
	IncludedDatasets IncludedDatasets `mapstructure:"included_datasets"`
}

type Expiry struct {
	Full time.Duration `mapstructure:"full"`
	Diff time.Duration `mapstructure:"diff"`
	Incr time.Duration `mapstructure:"incr"`
}

type IncludedDatasets []string
