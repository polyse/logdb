package main

import (
	"C"
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/polyse/logdb/internal/adapter"
	"github.com/rs/zerolog/log"
	"github.com/senyast4745/meilisearch-go"
	"time"
	"unsafe"
)

var adp adapter.Adapter

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "gstdout", "Stdout GO!")
}

//export FLBPluginInit
// (fluentbit will call this)
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	param := output.FLBPluginConfigKey(plugin, "param")
	fmt.Printf("[flb-go] plugin parameter = '%s'\n", param)

	cfg := &adapter.Config{
		Config: meilisearch.Config{
			Host:   "http://mlsearch:7700",
			APIKey: "",
		},
		Timeout: 1 * time.Second,
	}
	var err error
	adp, err = adapter.NewFBAdapter(cfg)
	if err != nil {
		log.Debug().Err(err).Msg("error while init adapter")
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	t := C.GoString(tag)

	if t == "" {
		t = adapter.TagD
	}

	b := C.GoBytes(data, C.int(length))

	err := adp.SaveData(b, t)

	if err != nil {
		log.Debug().Err(err).Msg("error while saving data")
		if err, ok := err.(adapter.Err); !ok {
			return output.FLB_ERROR
		} else {
			return err.GetCode()
		}
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
