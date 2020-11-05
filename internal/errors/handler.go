package errors

import (
	"context"
	"github.com/rs/zerolog/log"
	"sync/atomic"
	"time"
)

type Handler struct {
	errChan  chan error
	errCount uint32
	conf     *Config
	ticker   *time.Ticker
}

type Config struct {
	MaxErrorCount uint32
	ResetTime     time.Duration
}

func NewHandler(pCtx context.Context, conf *Config) (context.Context, chan<- error) {
	handler := &Handler{
		errChan:  make(chan error, conf.MaxErrorCount),
		errCount: 0,
		conf:     conf,
		ticker:   time.NewTicker(conf.ResetTime),
	}
	ctx := asyncHandleError(pCtx, handler)
	return ctx, handler.errChan
}

func asyncHandleError(pCtx context.Context, handler *Handler) context.Context {
	ctx, cancelFunc := context.WithCancel(pCtx)
	go func() {
		for {
			select {
			case err := <-handler.errChan:
				log.Warn().Err(err).Msg("handle error")
				if atomic.AddUint32(&handler.errCount, 1) > handler.conf.MaxErrorCount {
					cancelFunc()
				}
			case <-pCtx.Done():
				return
			case <-handler.ticker.C:
				atomic.StoreUint32(&handler.errCount, 0)
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}

	}()
	return ctx
}
