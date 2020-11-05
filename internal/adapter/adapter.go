package adapter

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	ml "github.com/senyast4745/meilisearch-go"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"net/http"
	"sync"
	"time"
)

type Adapter interface {
	SaveData(data []byte, indexUid string) error
	DatabaseHealthCheck() error
}

type SimpleAdapter struct {
	c      ml.ClientInterface
	ind    map[string]*ml.Index
	lock   sync.Mutex
	pPool  fastjson.ParserPool
	arPool fastjson.ArenaPool
	keys   map[string]struct{}
	kLock  sync.Mutex
}

type Config struct {
	ml.Config
	Timeout time.Duration
}

type KeyData struct {
	Id   string `json:"@id"`
	Keys []string
}

const KeyId = "key"

func NewAdapter(conf *Config) (*SimpleAdapter, error) {
	client := &fasthttp.Client{
		WriteTimeout: conf.Timeout,
		ReadTimeout:  conf.Timeout,
	}
	c := ml.NewFastHTTPCustomClient(conf.Config, client)

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
	adapter := &SimpleAdapter{c: c, ind: indexMap, keys: keys}
	return adapter, nil
}

func (a *SimpleAdapter) SaveData(data []byte, indexUid string) error {
	index, err := getOrCreateIndex(a, indexUid)
	if err != nil {
		return err
	}
	p := a.pPool.Get()
	defer a.pPool.Put(p)
	log.Debug().Bytes("data", data).Msg("request data")

	val, err := p.ParseBytes(data)
	if err != nil {
		return err
	}

	ar := a.arPool.Get()
	defer a.arPool.Put(ar)
	defer ar.Reset()

	timeF := time.Now().Unix()

	val.Set("@id", ar.NewString(uuid.New().String()))
	val.Set("@timestamp", ar.NewNumberInt(int(timeF)))

	arr := ar.NewArray()
	arr.SetArrayItem(0, val)
	data = arr.MarshalTo(data[:0])
	raw := data

	_, err = a.c.Documents(index.UID).AddOrReplace(raw)
	if err != nil {
		return err
	}
	if o, err := val.Object(); err != nil {
		return err
	} else {
		var strData string
		nKeys := false
		o.Visit(func(key []byte, v *fastjson.Value) {
			strData = string(key)
			if _, ok := a.keys[strData]; !ok {
				a.kLock.Lock()
				defer a.kLock.Unlock()
				if _, ok := a.keys[strData]; !ok {
					nKeys = true
					a.keys[strData] = struct{}{}
				}
			}
		})
		if nKeys {
			kData := &KeyData{
				Id:   KeyId,
				Keys: make([]string, 0, len(a.keys)),
			}
			for k := range a.keys {
				kData.Keys = append(kData.Keys, k)
			}
			_, err = a.c.Documents(index.UID).AddOrReplace(kData)
			return err
		}
	}

	return nil
}

func getOrCreateIndex(a *SimpleAdapter, indexUid string) (index *ml.Index, err error) {
	var ok bool
	log.Debug().Str("index uid", indexUid).Msg("start finding index by uid")
	if index, ok = a.ind[indexUid]; !ok {

		a.lock.Lock()
		defer a.lock.Unlock()
		if index, ok = a.ind[indexUid]; !ok {
			apiInd := a.c.Indexes()
			if index, err = apiInd.Get(indexUid); index == nil {
				if cliErr, ok := err.(*ml.Error); ok {
					if cliErr.StatusCode != http.StatusNotFound {
						return nil, err
					}
				} else if err != nil {
					return nil, err
				}
				crInd := ml.CreateIndexRequest{
					UID: indexUid,
				}
				if indResp, err := apiInd.Create(crInd); err == nil {
					log.Debug().Interface("created", indResp).Msg("index created")
					index = &ml.Index{
						Name:       indResp.Name,
						UID:        indResp.UID,
						CreatedAt:  indResp.CreatedAt,
						UpdatedAt:  indResp.UpdatedAt,
						PrimaryKey: indResp.PrimaryKey,
					}
				} else {
					return nil, err
				}
			} else if err != nil {
				return nil, err
			}
		}
		a.ind[index.UID] = index
	}
	log.Debug().Str("index uid", index.UID).Msg("using index")
	return index, nil
}

func (a *SimpleAdapter) DatabaseHealthCheck() error {
	return a.c.Health().Get()
}
