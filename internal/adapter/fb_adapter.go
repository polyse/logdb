package adapter

import "C"
import (
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/google/uuid"
	ml "github.com/meilisearch/meilisearch-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/ugorji/go/codec"
	"github.com/valyala/fasthttp"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

var h = new(codec.MsgpackHandle)

type FBAdapter struct {
	c     ml.ClientInterface
	ind   map[string]*ml.Index
	lock  sync.Mutex
	keys  map[string]struct{}
	kLock sync.Mutex
}

type RawData map[string]interface{}

func NewFBAdapter() (*FBAdapter, error) {
	var err error
	regex, err = regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		return nil, err
	}
	cfg, err := load()
	if err != nil {
		return nil, err
	}
	if err = initLogger(cfg); err != nil {
		return nil, err
	}

	client := &fasthttp.Client{
		WriteTimeout: cfg.DbTimeout,
		ReadTimeout:  cfg.DbTimeout,
	}
	c := ml.NewFastHTTPCustomClient(createMLConf(cfg), client)

	indexes, err := c.Indexes().List()
	if err != nil {
		return nil, err
	}
	indexMap := map[string]*ml.Index{}
	for i := range indexes {
		ind := indexes[i]
		indexMap[ind.UID] = &ind
	}
	keys := make(map[string]struct{})
	adapter := &FBAdapter{c: c, ind: indexMap, keys: keys}
	return adapter, nil
}

func initLogger(c *fbConfig) error {
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

func (a *FBAdapter) SaveData(data []byte, tag string) error {
	ind, err := getOrCreateIndex(a.ind, &a.lock, a.c, tag)
	if err != nil {
		return err
	}

	log.Debug().Bytes("recieved", data).Msg("new data")

	mpdec := codec.NewDecoderBytes(data, h)
	var sdata []RawData
	var nKeys bool
	for {
		ts, rec, err := decode(mpdec)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		prData := make(RawData)

		rec[IdF] = uuid.New().String()
		rec[TimestampF] = getTs(ts)

		k, err := checkKeysContains(rec, &prData, a)
		if k {
			nKeys = k
		}
		if err != nil {
			return err
		}
		sdata = append(sdata, prData)
	}

	if nKeys {
		err := sendNewKeysToDb(a.keys, a.c, ind)
		if err != nil {
			a.keys = make(map[string]struct{})
			return err
		}
	}

	_, err = a.c.Documents(ind.UID).AddOrReplace(sdata)
	if err != nil {
		return err
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return nil
}

func checkKeysContains(rec map[interface{}]interface{}, prData *RawData, a *FBAdapter) (nKeys bool, err error) {
	if *prData == nil {
		*prData = make(RawData)
	}
	for k, v := range rec {
		sk, ok := k.(string)
		if !ok || k == nil {
			return false, fmt.Errorf("unsupported key %+v", k)
		}
		if nv, ok := v.([]byte); ok {
			(*prData)[sk] = string(nv)
		} else {
			(*prData)[sk] = v
		}
		if _, ok := a.keys[sk]; !ok {
			func() {
				a.kLock.Lock()
				defer a.kLock.Unlock()
				if _, ok := a.keys[sk]; !ok {
					nKeys = true
					a.keys[sk] = struct{}{}
				}
			}()
		}
	}
	return nKeys, nil
}

func getTs(ts interface{}) (timestamp time.Time) {
	switch t := ts.(type) {
	case output.FLBTime:
		timestamp = ts.(output.FLBTime).Time
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		log.Info().Msg("time provided invalid, defaulting to now.")
		timestamp = time.Now()
	}
	return
}

func decode(mpdec *codec.Decoder) (t interface{}, rec map[interface{}]interface{}, err error) {
	var m interface{}

	if err = mpdec.Decode(&m); err != nil {
		return 0, nil, err
	}

	slice := reflect.ValueOf(m)
	if slice.Kind() != reflect.Slice || slice.Len() != 2 {
		return 0, nil, fmt.Errorf("unknown type")
	}

	t = slice.Index(0).Interface()
	data := slice.Index(1)

	rec = data.Interface().(map[interface{}]interface{})

	return
}

func (a *FBAdapter) DatabaseHealthCheck() error {
	return a.c.Health().Get()
}

type Err struct {
	error
	code int
}

func (e *Err) GetCode() int {
	return e.code
}

func createMLConf(cfg *fbConfig) ml.Config {
	return ml.Config{
		Host:   cfg.DbAddr,
		APIKey: cfg.ApiKey,
	}
}
