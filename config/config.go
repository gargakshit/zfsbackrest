package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	S3         S3Store
	Repository Repository
	Debug      bool
}

func LoadConfig(v *viper.Viper, path string) (*Config, error) {
	v.SetConfigFile(path)
	v.SetConfigType("toml")
	v.SetEnvPrefix("ZFSBACKREST")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
