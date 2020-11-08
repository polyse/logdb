//+build wireinject

package main

import (
	"context"
	"github.com/google/wire"
	"github.com/polyse/logdb/internal/adapter"
	"github.com/polyse/logdb/internal/api"
)

func initLogAdapterApi(ctx context.Context, c *config, ch chan<- error) (*api.API, func(), error) {
	wire.Build(createLogAdapterConfig, createApiConfig,
		adapter.NewAdapter, wire.Bind(new(adapter.Adapter), new(*adapter.SimpleAdapter)), api.NewAdapterApi)
	return nil, nil, nil
}
