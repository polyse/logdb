package adapter

import (
	"github.com/google/uuid"
	ml "github.com/meilisearch/meilisearch-go"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"net/http"
	"regexp"
	"sync"
	"time"
)

var regex *regexp.Regexp

type Adapter interface {
	SaveData([]byte, string) error
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

func NewAdapter(conf *Config) (*SimpleAdapter, error) {
	var err error
	regex, err = regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		return nil, err
	}

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

func (a *SimpleAdapter) SaveData(data []byte, tag string) error {
	index, err := getOrCreateIndex(a.ind, &a.lock, a.c, tag)
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

	log.Debug().Str("data", val.String()).Msg("before adding fields")
	if val, err = a.addUtilFields(val, ar); err != nil {
		return err
	}
	data = val.MarshalTo(data[:0])
	raw := ml.RawType(data)

	log.Debug().Bytes("data", raw).Msg("raw request body")

	_, err = a.c.Documents(index.UID).AddOrReplace(raw)
	if err != nil {
		return err
	}

	if va, err := val.Array(); err != nil {
		return a.getAllKeys(val, index)
	} else {
		for _, o := range va {
			if err = a.getAllKeys(o, index); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *SimpleAdapter) addUtilFields(val *fastjson.Value, ar *fastjson.Arena) (*fastjson.Value, error) {
	var (
		vals []*fastjson.Value
		err  error
	)
	tsu := time.Now().Unix()
	if vals, err = val.Array(); err != nil {
		if v, err := val.Object(); err != nil {
			return nil, err
		} else {
			v.Set(IdF, ar.NewString(uuid.New().String()))
			v.Set(TimestampF, ar.NewNumberInt(int(tsu)))
			arr := ar.NewArray()
			arr.SetArrayItem(0, val)
			return arr, nil
		}
	} else {
		for _, v := range vals {
			v.Set(IdF, ar.NewString(uuid.New().String()))
			v.Set(TimestampF, ar.NewNumberInt(int(tsu)))
		}
	}
	return val, nil
}

func (a *SimpleAdapter) getAllKeys(val *fastjson.Value, index *ml.Index) (err error) {
	var obj *fastjson.Object
	if obj, err = val.Object(); err != nil {
		return err
	}
	var strData string
	nKeys := false
	obj.Visit(func(key []byte, v *fastjson.Value) {
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

		if err = sendNewKeysToDb(a.keys, a.c, index); err != nil {
			a.keys = make(map[string]struct{})
		}
	}
	return nil
}

type KeyData struct {
	Id        string `json:"@id"`
	Keys      []string
	Timestamp time.Time `json:"@timestamp"`
}

func sendNewKeysToDb(keys map[string]struct{}, c ml.ClientInterface, index *ml.Index) error {
	kData := &KeyData{
		Id:        KeyId,
		Keys:      make([]string, 0, len(keys)),
		Timestamp: time.Now(),
	}
	for k := range keys {
		kData.Keys = append(kData.Keys, k)
	}
	log.Debug().Interface("new keys", kData).Msg("new logs founded")
	_, err := c.Documents(index.UID).AddOrReplace([]*KeyData{kData})
	if err != nil {
		return err
	}
	return nil
}

func getOrCreateIndex(ind map[string]*ml.Index, lock *sync.Mutex, c ml.ClientInterface, tag string) (index *ml.Index, err error) {
	tag = regex.ReplaceAllString(tag, "")
	var ok bool
	log.Debug().Str("index uid", tag).Msg("start finding index by uid")
	if index, ok = ind[tag]; !ok {

		lock.Lock()
		defer lock.Unlock()
		if index, ok = ind[tag]; !ok {
			apiInd := c.Indexes()
			if index, err = apiInd.Get(tag); index == nil {
				if cliErr, ok := err.(*ml.Error); ok {
					if cliErr.StatusCode != http.StatusNotFound {
						return nil, err
					}
				} else if err != nil {
					return nil, err
				}
				crInd := ml.CreateIndexRequest{
					UID: tag,
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
		ind[index.UID] = index
	}
	log.Debug().Str("index uid", index.UID).Msg("using index")
	return index, nil
}

func (a *SimpleAdapter) DatabaseHealthCheck() error {
	return a.c.Health().Get()
}
