package adapter

import (
	"github.com/rs/zerolog/log"
	ml "github.com/senyast4745/meilisearch-go"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"net/http"
	"sync"
	"time"
)

type Adapter struct {
	c    ml.ClientInterface
	ind  map[string]*ml.Index
	lock sync.Mutex
	pp   fastjson.ParserPool
	arp  fastjson.ArenaPool
}

type Config struct {
	ml.Config
	Timeout time.Duration
}

func NewAdapter(conf *Config) (*Adapter, error) {
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
	adapter := &Adapter{c: c, ind: indexMap}
	return adapter, nil
}

func (a *Adapter) SaveData(data []byte, indexUid string) error {
	index, err := getOrCreateIndex(a, indexUid)
	if err != nil {
		return err
	}
	p := a.pp.Get()
	defer a.pp.Put(p)
	log.Debug().Bytes("data", data).Msg("request data")
	val, err := p.ParseBytes(data)
	if err != nil {
		return err
	}
	if o, err := val.Object(); err != nil {
		return err
	} else {
		o.Visit(func(key []byte, v *fastjson.Value) {
			log.Debug().Bytes("key", key).Msg("key iter")
		})
	}
	ar := a.arp.Get()
	defer a.arp.Put(ar)
	defer ar.Reset()

	val.Set("@timestamp", ar.NewString(time.Now().String()))
	arr := ar.NewArray()
	arr.SetArrayItem(0, val)
	data = arr.MarshalTo(data[:0])
	var raw ml.RawType
	raw = data

	_, err = a.c.Documents(index.UID).AddOrReplace(raw)
	if err != nil {
		return err
	}
	return nil
}

func getOrCreateIndex(a *Adapter, indexUid string) (index *ml.Index, err error) {
	var ok bool
	log.Debug().Str("index uid", indexUid).Msg("start finding index by uid")
	if index, ok = a.ind[indexUid]; !ok {

		a.lock.Lock()
		defer a.lock.Unlock()
		if index, ok = a.ind[indexUid]; !ok {
			apiInd := a.c.Indexes()
			if index, err = apiInd.Get(indexUid); index == nil {
				if cError, ok := err.(ml.Error); ok {
					if cError.StatusCode != http.StatusNotFound {
						return nil, err
					}
				}
				createIndexRequest := ml.CreateIndexRequest{
					UID: indexUid,
				}
				if indResp, err := apiInd.Create(createIndexRequest); err == nil {
					log.Debug().Interface("created", indResp).Msg("created index")
					a.ind[indResp.UID] = &ml.Index{
						Name:       indResp.Name,
						UID:        indResp.UID,
						CreatedAt:  indResp.CreatedAt,
						UpdatedAt:  indResp.UpdatedAt,
						PrimaryKey: indResp.PrimaryKey,
					}
					index = a.ind[indResp.UID]
				} else {
					return nil, err
				}
			} else if err != nil {
				return nil, err
			}

		}
	}
	log.Debug().Str("index uid", index.UID).Msg("using index")
	return index, nil
}

func (a *Adapter) DatabaseHealthCheck() error {
	return a.c.Health().Get()
}
