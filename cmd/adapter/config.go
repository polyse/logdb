package main

import (
	"github.com/rs/zerolog/log"
	"time"

	"github.com/caarlos0/env"
)

// Config is main application configuration structure.
type config struct {
	Listen  string `env:"LISTEN" envDefault:"localhost:9000"`
	Network string `env:"NETWORK" envDefault:"tcp"`

	LogLevel string `env:"LOG_LEVEL" envDefault:"debug"`
	LogFmt   string `env:"LOG_FMT" envDefault:"console"`

	DbAddr     string        `env:"DB_ADDR" envDefault:"http://localhost:7700"`
	MaxDbCount int           `env:"DB_CONN_COUNT" envDefault:"100"`
	DbTimeout  time.Duration `env:"DB_TIMEOUT" envDefault:"100ms"`
	ApiKey     string        `env:"API_KEY" envDefault:""`

	Timeout time.Duration `env:"TIMEOUT" envDefault:"100ms"`
	Name    string        `env:"HTTP_CLIENT_NAME" envDefault:"log-db-adapter"`

	MaxErrorCount int           `env:"MAX_ERR_COUNT" envDefault:"100"`
	ResetTime     time.Duration `env:"RESET_TIME" envDefault:"10m"`
}

func load() (*config, error) {
	log.Debug().Msg("loading configuration")
	cfg := &config{}

	if err := env.Parse(cfg); err != nil {
		return cfg, err
	}
	log.Debug().Interface("config", cfg).Msg("initialize configuration")
	return cfg, nil
}
