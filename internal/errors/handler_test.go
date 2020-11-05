package errors

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

type HandlerTestSuite struct {
	suite.Suite
	handler *Handler
	logF    *os.File
	ctx     context.Context
}

var testErr = fmt.Errorf("test error")

func (s *HandlerTestSuite) SetupTest() {
	cfg := &Config{
		MaxErrorCount: 1,
		ResetTime:     50 * time.Millisecond,
	}
	handler := &Handler{
		errChan:  make(chan error, cfg.ResetTime),
		errCount: 0,
		conf:     cfg,
		ticker:   time.NewTicker(1 * cfg.ResetTime),
	}
	s.handler = handler

	file, err := ioutil.TempFile(s.T().TempDir(), "test*.log")
	s.NoError(err)
	multyW := zerolog.MultiLevelWriter(file, os.Stdout)
	log.Logger = zerolog.New(multyW).With().Logger()
	s.logF = file
	s.ctx = asyncHandleError(context.Background(), s.handler)

}

func (s *HandlerTestSuite) TearDownTest() {
	err := s.logF.Close()
	s.NoError(err)
}

func TestRunAdapterUnitTestSuite(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (s *HandlerTestSuite) Test_HandlerLog_Normal_With_Reset() {
	s.handler.errChan <- testErr
	time.Sleep(100 * time.Millisecond)
	s.handler.errChan <- testErr
	time.Sleep(50 * time.Millisecond)
	logs, _ := ioutil.ReadFile(s.logF.Name())
	s.True(strings.Contains(string(logs), "\"level\":\"warn\""))
	s.NoError(s.ctx.Err())
}

func (s *HandlerTestSuite) Test_HandlerLog_Many_Errors() {
	s.handler.errChan <- testErr
	time.Sleep(10 * time.Millisecond)
	s.handler.errChan <- testErr
	time.Sleep(10 * time.Millisecond)
	logs, _ := ioutil.ReadFile(s.logF.Name())
	s.True(strings.Contains(string(logs), "\"level\":\"warn\""))
	s.Error(s.ctx.Err())
}

func (s *HandlerTestSuite) Test_HandlerLog_Normal() {

	s.handler.errChan <- testErr
	time.Sleep(10 * time.Millisecond)
	logs, _ := ioutil.ReadFile(s.logF.Name())
	s.True(strings.Contains(string(logs), "\"level\":\"warn\""))
	s.NoError(s.ctx.Err())
}
