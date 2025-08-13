package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Repository        Repository        `mapstructure:"repository"`
	Debug             bool              `mapstructure:"debug"`
	UploadConcurrency UploadConcurrency `mapstructure:"upload_concurrency"`
	ZFS               ZFS               `mapstructure:"zfs"`
}

func LoadConfig(v *viper.Viper, path string) (*Config, error) {
	v.SetConfigFile(path)
	v.SetConfigType("toml")
	v.SetEnvPrefix("ZFSBACKREST")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults.
	v.SetDefault("repository.s3.part_size", 128*1024*1024)
	v.SetDefault("repository.s3.upload_threads", 1)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
