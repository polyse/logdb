package main

import (
	"context"
	"fmt"
	ml "github.com/meilisearch/meilisearch-go"
	"github.com/polyse/logdb/internal/adapter"
	"github.com/polyse/logdb/internal/api"
	"github.com/polyse/logdb/internal/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xlab/closer"

	"os"
	"strings"
)

func main() {
	defer closer.Close()

	ctx, cancelCtx := context.WithCancel(context.Background())

	closer.Bind(cancelCtx)

	cfg, err := load()

	if err != nil {
		log.Error().Err(err).Msg("error while loading config")
		return
	}
	if err = initLogger(cfg); err != nil {
		log.Error().Err(err).Msg("error while configure logger")
		return
	}
	log.Debug().Msg("logger initialized")

	log.Debug().Msg("initialize error handler")
	errConf := createErrorHandlerConf(cfg)
	errCtx, errorChan := errors.NewHandler(ctx, errConf)

	log.Debug().Msg("initialize adapter api")
	adapterApi, closeApi, err := initLogAdapterApi(ctx, cfg, errorChan)
	closer.Bind(closeApi)
	if err != nil {
		log.Error().Err(err).Msg("error while init adapter api")
		return
	}

	go func() {
		<-errCtx.Done()
		log.Info().Msg("Stopping app")
		closer.Close()

	}()

	if adapterApi.Run() != nil {
		log.Error().Err(err).Msg("error while starting server")
		return
	}
}

func createErrorHandlerConf(c *config) *errors.Config {
	return &errors.Config{
		MaxErrorCount: uint32(c.MaxErrorCount),
		ResetTime:     c.ResetTime,
	}
}

func createLogAdapterConfig(c *config) *adapter.Config {
	return &adapter.Config{Config: ml.Config{
		Host:   c.DbAddr,
		APIKey: c.ApiKey,
	}, Timeout: c.DbTimeout}
}

func createApiConfig(c *config) *api.Config {
	return &api.Config{
		Addr:      c.Listen,
		Network:   c.Network,
		MaxDbConn: uint16(c.MaxDbCount),
		Timeout:   c.Timeout,
	}
}

func initLogger(c *config) error {
	log.Debug().Msg("initialize logger")
	logLvl, err := zerolog.ParseLevel(strings.ToLower(c.LogLevel))
	if err != nil {
		return err
	}
	zerolog.SetGlobalLevel(logLvl)
	switch c.LogFmt {
	case "console":
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	case "json":
	default:
		return fmt.Errorf("unknown output format %s", c.LogFmt)
	}
	return nil
}
