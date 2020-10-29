package api

import (
	"context"
	"fmt"
	"github.com/polyse/logDb/internal/adapter"
	"github.com/rs/zerolog/log"
	atr "github.com/savsgio/atreugo/v11"
	"net"
	"net/http"
	"os"
	"time"
)

type API struct {
	a             *adapter.Adapter
	server        *atr.Atreugo
	listener      net.Listener
	concurrencyCh chan<- struct{}
	timeout       time.Duration
	errChan       chan<- error
}

type Config struct {
	Addr      string
	Network   string
	MaxDbConn uint16
	Timeout   time.Duration
}

type Context struct {
	*atr.RequestCtx
	Ctx context.Context
}

func NewAdapterApi(ctx context.Context, conf *Config, adapter *adapter.Adapter, errCh chan<- error) (*API, func(), error) {
	file, err := os.Open(os.DevNull)
	if err != nil {
		return nil, nil, err
	}
	atrCfg := atr.Config{
		Addr:             conf.Addr,
		Network:          conf.Network,
		GracefulShutdown: true,
		ReadTimeout:      conf.Timeout,
		WriteTimeout:     conf.Timeout,
		LogOutput:        file,
	}

	listener, err := net.Listen(conf.Network, conf.Addr)
	if err != nil {
		return nil, nil, err
	}
	api := &API{
		a:             adapter,
		server:        nil,
		listener:      listener,
		concurrencyCh: make(chan<- struct{}, conf.MaxDbConn),
		timeout:       conf.Timeout,
		errChan:       errCh,
	}
	server := atr.New(atrCfg)

	apiRouter := server.NewGroupPath("/api")
	apiRouter.UseBefore(func(rctx *atr.RequestCtx) error {
		cc := &Context{
			RequestCtx: rctx,
			Ctx:        ctx,
		}
		return cc.Next()
	})
	apiRouter.UseAfter(logRequest())
	apiRouter.PUT("/logs/{tag:*}", api.HandleNewLog)
	apiRouter.GET("/health", func(ctx *atr.RequestCtx) error {
		log.Debug().Msg("health check")
		ctx.Response.SetStatusCode(http.StatusOK)
		ctx.Response.AppendBodyString("OK")
		return nil
	})
	log.Debug().
		Interface("paths", server.ListPaths()).
		Str("network", listener.Addr().Network()).
		Str("listening", listener.Addr().String()).
		Msg("server initialized")

	api.server = server

	return api, func() {
		if err := listener.Close(); err != nil {
			log.Err(err).Msg("can not stop listener")
		}
	}, err
}

func (a *API) HandleNewLog(ctx *atr.RequestCtx) error {
	select {
	case a.concurrencyCh <- struct{}{}:
	default:
		var err error
		if err = a.a.DatabaseHealthCheck(); err != nil {
			ctx.Response.SetStatusCode(http.StatusGatewayTimeout)
			log.Warn().Msg("database is unavailable")
		} else {
			ctx.Response.SetStatusCode(http.StatusServiceUnavailable)
			err = fmt.Errorf("too many goroutines")
		}
		return err
	}
	data := ctx.Request.Body()
	tag := ctx.UserValue("tag").(string)
	go func(data []byte, tag string) {
		if err := a.a.SaveData(data, tag); err != nil {
			a.errChan <- err
		}
	}(data, tag)
	ctx.Response.SetStatusCode(http.StatusAccepted)
	return nil
}

func logRequest() atr.Middleware {
	return func(ctx *atr.RequestCtx) error {
		res := &ctx.Response
		start := ctx.Time()
		stop := time.Now()

		log.Debug().
			Str("remote", ctx.RemoteAddr().String()).
			Bytes("user_agent", ctx.UserAgent()).
			Bytes("method", ctx.Method()).
			Bytes("path", ctx.Path()).
			Int("status", res.StatusCode()).
			Dur("duration", stop.Sub(start)).
			Str("duration_human", stop.Sub(start).String()).
			Msgf("called url %s", ctx.URI())

		return ctx.Next()
	}
}

func (a *API) Run() error {
	return a.server.ServeGracefully(a.listener)
}
