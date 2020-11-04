package api

import (
	"context"
	"fmt"
	mocks "github.com/polyse/logDb/test/mock"
	atr "github.com/savsgio/atreugo/v11"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/valyala/fasthttp"
	"net"
	"net/http"
	"testing"
	"time"
)

type APIUnitTestSuite struct {
	suite.Suite
	adapter *mocks.Adapter
	ln      net.Listener
	api     *API
	errs    chan error
	host    string
	httpCli *fasthttp.Client
}

func (a *APIUnitTestSuite) SetupTest() {
	addr, err := getFreeLocalAddr()
	a.NoError(err)
	ln, err := net.Listen(addr.Network(), addr.String())
	a.NoError(err)
	a.ln = ln
	a.adapter = &mocks.Adapter{}
	a.errs = make(chan error, 1)
	a.api = &API{
		ad:    a.adapter,
		srv:   nil,
		ln:    ln,
		conCh: make(chan struct{}, 1),
		errCh: a.errs,
	}
	cfg := atr.Config{
		GracefulShutdown: true,
		ReadTimeout:      1 * time.Second,
		WriteTimeout:     1 * time.Second,
	}
	a.httpCli = &fasthttp.Client{}
	a.api.initRouter(context.Background(), ln, cfg)
	a.host = fmt.Sprintf("http://%v/api", addr.String())
	go func() {
		err := a.api.Run()
		a.NoError(err)
	}()

}

func (a *APIUnitTestSuite) TearDownTest() {
	err := a.ln.Close()
	if err != nil {
		a.Failf("can not shutdown listener", "failed while shutting down listener with error %+v", err)
	}
}

func TestRunApiUnitTestSuite(t *testing.T) {
	suite.Run(t, new(APIUnitTestSuite))
}

func (a *APIUnitTestSuite) Test_HealthCheck() {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(a.host + "/health")
	req.Header.SetMethod(http.MethodGet)
	req.Header.Set("Content-Type", "application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	err := a.httpCli.Do(req, resp)
	a.NoError(err)
	a.Equal(http.StatusOK, resp.StatusCode())
	a.Equal("OK", string(resp.Body()))
}

func (a *APIUnitTestSuite) Test_SaveData_Normal() {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(a.host + "/logs/test")
	req.Header.SetMethod(http.MethodPut)
	req.Header.Set("Content-Type", "application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	a.adapter.On("SaveData", mock.AnythingOfType("[]uint8"), "test").Times(1).Return(nil)
	err := a.httpCli.Do(req, resp)

	time.Sleep(10 * time.Millisecond)

	a.NoError(err)
	a.Equal(http.StatusAccepted, resp.StatusCode())
	a.adapter.AssertExpectations(a.T())

}

func (a *APIUnitTestSuite) Test_SaveData_Server_Err() {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(a.host + "/logs/test")
	req.Header.SetMethod(http.MethodPut)
	req.Header.Set("Content-Type", "application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	a.adapter.On("SaveData", mock.AnythingOfType("[]uint8"), "test").Times(1).Return(fmt.Errorf("test error"))

	err := a.httpCli.Do(req, resp)

	time.Sleep(10 * time.Millisecond)

	a.NoError(err)
	a.Equal(http.StatusAccepted, resp.StatusCode())

	err = <-a.errs
	a.Error(err)
	a.adapter.AssertExpectations(a.T())
}

func (a *APIUnitTestSuite) Test_SaveData_Server_Busy() {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	a.api.conCh <- struct{}{}

	req.SetRequestURI(a.host + "/logs/test")
	req.Header.SetMethod(http.MethodPut)
	req.Header.Set("Content-Type", "application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	a.adapter.On("DatabaseHealthCheck").Times(1).Return(nil)

	err := a.httpCli.Do(req, resp)
	a.NoError(err)
	a.Equal(http.StatusServiceUnavailable, resp.StatusCode())

	err = <-a.errs
	a.Error(err)

	a.adapter.AssertExpectations(a.T())
	a.adapter.AssertNotCalled(a.T(), "SaveData", mock.AnythingOfType("[]uint8"), "test")
}

func (a *APIUnitTestSuite) Test_SaveData_Server_Dead() {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	a.api.conCh <- struct{}{}

	req.SetRequestURI(a.host + "/logs/test")
	req.Header.SetMethod(http.MethodPut)
	req.Header.Set("Content-Type", "application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	a.adapter.On("DatabaseHealthCheck").Times(1).Return(fmt.Errorf("test error"))
	err := a.httpCli.Do(req, resp)
	a.NoError(err)
	a.Equal(http.StatusGatewayTimeout, resp.StatusCode())

	err = <-a.errs
	a.Error(err)

	a.adapter.AssertExpectations(a.T())
	a.adapter.AssertNotCalled(a.T(), "SaveData", mock.AnythingOfType("[]uint8"), "test")

}

func getFreeLocalAddr() (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr), nil
}
