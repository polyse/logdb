package api

import (
	"context"
	"fmt"
	"github.com/polyse/logdb/internal/adapter"
	"github.com/rs/zerolog/log"
	atr "github.com/savsgio/atreugo/v11"
	"net"
	"net/http"
	"os"
	"time"
)

type API struct {
	ad    adapter.Adapter
	srv   *atr.Atreugo
	ln    net.Listener
	conCh chan struct{}
	errCh chan<- error
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

func NewAdapterApi(ctx context.Context, conf *Config, adapter adapter.Adapter, errCh chan<- error) (*API, func(), error) {
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

	l, err := net.Listen(conf.Network, conf.Addr)
	if err != nil {
		return nil, nil, err
	}
	api := &API{
		ad:    adapter,
		srv:   nil,
		ln:    l,
		conCh: make(chan struct{}, conf.MaxDbConn),
		errCh: errCh,
	}

	return api, api.initRouter(ctx, l, atrCfg), err

}

func (a *API) initRouter(ctx context.Context, l net.Listener, cfg atr.Config) func() {
	srv := atr.New(cfg)

	apiRouter := srv.NewGroupPath("/api")
	apiRouter.UseBefore(func(rctx *atr.RequestCtx) error {
		cc := &Context{
			RequestCtx: rctx,
			Ctx:        ctx,
		}
		return cc.Next()
	})
	apiRouter.UseAfter(logRequest())
	apiRouter.POST("/logs/{tag:*}", a.HandleNewLog)
	apiRouter.GET("/health", func(ctx *atr.RequestCtx) error {
		log.Debug().Msg("health check")
		ctx.Response.SetStatusCode(http.StatusOK)
		ctx.Response.AppendBodyString("OK")
		return nil
	})

	log.Debug().
		Interface("paths", srv.ListPaths()).
		Str("network", l.Addr().Network()).
		Str("listening", l.Addr().String()).
		Msg("server initialized")

	a.srv = srv

	return func() {
		if err := l.Close(); err != nil {
			log.Err(err).Msg("can not stop listener")
		}
	}
}

func (a *API) HandleNewLog(ctx *atr.RequestCtx) error {
	select {
	case a.conCh <- struct{}{}:
		log.Debug().Msg("try to write")
	default:
		var err error
		if err = a.ad.DatabaseHealthCheck(); err != nil {
			ctx.Response.SetStatusCode(http.StatusGatewayTimeout)
			log.Warn().Msg("database is unavailable")
		} else {
			ctx.Response.SetStatusCode(http.StatusServiceUnavailable)
			err = fmt.Errorf("too many goroutines")
		}
		a.errCh <- err
		return err
	}
	data := ctx.Request.Body()
	tag := ctx.UserValue("tag").(string)
	go func(data []byte, tag string) {
		if err := a.ad.SaveData(data, tag); err != nil {
			a.errCh <- err
		}
	}(data, tag)
	ctx.Response.SetStatusCode(http.StatusAccepted)
	<-a.conCh
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
	return a.srv.ServeGracefully(a.ln)
}
