package integration

import (
	"github.com/rs/zerolog/log"
	"testing"
)

func Test_Hello_World(t *testing.T) {
	log.Debug().Msg("hello integration")
}
