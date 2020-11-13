package adapter

import (
	"github.com/rs/zerolog/log"
	"time"

	"github.com/caarlos0/env"
)

type fbConfig struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
	LogFmt   string `env:"LOG_FMT" envDefault:"console"`

	DbAddr    string        `env:"DB_ADDR" envDefault:"http://mlsearch:7700"`
	DbTimeout time.Duration `env:"DB_TIMEOUT" envDefault:"100ms"`
	ApiKey    string        `env:"API_KEY" envDefault:""`
}

func load() (*fbConfig, error) {
	log.Debug().Msg("loading configuration")
	cfg := &fbConfig{}

	if err := env.Parse(cfg); err != nil {
		return cfg, err
	}
	log.Debug().Interface("config", cfg).Msg("initialize configuration")
	return cfg, nil
}
