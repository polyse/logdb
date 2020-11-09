package adapter

import "C"
import (
	"encoding/json"
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	ml "github.com/senyast4745/meilisearch-go"
	"github.com/ugorji/go/codec"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"io"
	"reflect"
	"regexp"
	"sync"
	"time"
)

var (
	arPool fastjson.ArenaPool
)

type FBAdapter struct {
	c     ml.ClientInterface
	ind   map[string]*ml.Index
	lock  sync.Mutex
	keys  map[string]struct{}
	kLock sync.Mutex
}

type RawData map[string]interface{}

func (r RawData) MarshalJSON() ([]byte, error) {
	log.Debug().Msg("start parsed")
	ar := arPool.Get()
	defer arPool.Put(ar)
	defer ar.Reset()
	val := ar.NewObject()
	for k, v := range r {
		if v, ok := v.(json.Marshaler); ok {
			b, err := v.MarshalJSON()
			if err != nil {
				return nil, err
			}
			val.Set(k, ar.NewStringBytes(b))
		}
	}
	var data []byte
	data = val.MarshalTo(data)
	log.Debug().Bytes("data", data).Msg("parsed")
	return data, nil
}

func NewFBAdapter(cfg *Config) (*FBAdapter, error) {
	var err error
	regex, err = regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		return nil, err
	}
	client := &fasthttp.Client{
		WriteTimeout: cfg.Timeout,
		ReadTimeout:  cfg.Timeout,
	}
	c := ml.NewFastHTTPCustomClient(cfg.Config, client)

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

func (a *FBAdapter) SaveData(data []byte, tag string) error {
	tag = regex.ReplaceAllString(tag, "")
	ind, err := getOrCreateIndex(a.ind, &a.lock, a.c, tag)
	if err != nil {
		return err
	}
	h := new(codec.MsgpackHandle)
	mpdec := codec.NewDecoderBytes(data, h)
	var sdata []map[string]interface{}
	nKeys := false
	for {
		err, ts, rec := decode(mpdec)

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		prData := make(RawData)

		nKeys = checkKeysContains(rec, prData, a, nKeys)

		tsp := getTs(ts)
		prData[IdF] = uuid.New().String()
		prData[TimestampF] = tsp
		sdata = append(sdata, prData)
	}

	if nKeys {
		err := sendNewKeysToDb(a.keys, a.c, ind)
		if err != nil {
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

func checkKeysContains(rec map[interface{}]interface{}, prData RawData, a *FBAdapter, nKeys bool) bool {
	for k, v := range rec {
		sk := k.(string)
		prData[sk] = ml.RawType(v.([]byte))
		log.Debug().Bytes("test", v.([]byte)).Msg("test me")
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
	log.Debug().Interface("pr pr", prData).Msg("data data data")
	return nKeys
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

func decode(mpdec *codec.Decoder) (err error, t interface{}, rec map[interface{}]interface{}) {
	var m interface{}

	if err := mpdec.Decode(&m); err != nil {
		return err, 0, nil
	}

	slice := reflect.ValueOf(m)
	if slice.Kind() != reflect.Slice || slice.Len() != 2 {
		return fmt.Errorf("unknown type"), 0, nil
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
