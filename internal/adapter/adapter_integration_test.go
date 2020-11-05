// +build integration

package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	ml "github.com/senyast4745/meilisearch-go"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tsc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"net/http"
	"testing"
	"time"
)

type AdapterIntegrationTestSuite struct {
	suite.Suite
	adapter  *SimpleAdapter
	mlC      tsc.Container
	ctx      context.Context
	dbClient ml.ClientInterface
}

func (s *AdapterIntegrationTestSuite) SetupTest() {
	ctx := context.Background()

	wfS := &wait.HTTPStrategy{
		Port: "7700/tcp",
		Path: "/health",
		StatusCodeMatcher: func(status int) bool {
			return status == http.StatusOK
		},
		ResponseMatcher: func(body io.Reader) bool {
			return true
		},
		UseTLS:       false,
		TLSConfig:    nil,
		Method:       http.MethodGet,
		Body:         nil,
		PollInterval: 50 * time.Millisecond,
	}

	wfS.WithStartupTimeout(30 * time.Second)

	req := tsc.ContainerRequest{
		Image:        "getmeili/meilisearch",
		ExposedPorts: []string{"7700/tcp"},

		WaitingFor: wfS,
	}
	mlC, err := tsc.GenericContainer(ctx, tsc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.NoError(err)

	mappedPort, err := mlC.MappedPort(ctx, "7700")
	s.NoError(err)
	ip, err := mlC.Host(ctx)
	s.NoError(err)

	address := fmt.Sprintf("http://%s:%s", ip, mappedPort.Port())
	cfg := &Config{
		Config:  ml.Config{Host: address},
		Timeout: 1 * time.Second,
	}
	adapter, err := NewAdapter(cfg)
	s.NoError(err)

	s.adapter = adapter
	s.mlC = mlC
	s.ctx = ctx
	s.dbClient = adapter.c
}

func (s *AdapterIntegrationTestSuite) TearDownTest() {
	_ = s.mlC.Terminate(s.ctx)
}

func TestRunAdapterIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(AdapterIntegrationTestSuite))
}

func (s *AdapterIntegrationTestSuite) Test_GetOrCreateIndex_Normal_Mode() {
	testIndexes := []string{
		"test1", "test2",
	}

	for _, ti := range testIndexes {
		_, err := getOrCreateIndex(s.adapter, ti)
		s.NoError(err)
	}

	for _, ti := range testIndexes {
		i, err := s.dbClient.Indexes().Get(ti)
		s.NoError(err)
		s.NotNil(i)
		s.Equal(ti, i.UID)
	}
	var indNames []string
	for k := range s.adapter.ind {
		indNames = append(indNames, k)
	}
	s.ElementsMatch(testIndexes, indNames)
}

func (s *AdapterIntegrationTestSuite) Test_GetOrCreateIndex_Error() {
	testIndexes := []string{
		"test1", "test2",
	}

	_ = s.mlC.Terminate(s.ctx)

	for _, ti := range testIndexes {
		_, err := getOrCreateIndex(s.adapter, ti)
		s.Error(err)
	}

	s.Empty(s.adapter.ind)
}

func (s *AdapterIntegrationTestSuite) Test_SaveData_Normal_Mode() {
	testIndexUid := "test"
	testData := struct {
		Test string `json:"test"`
	}{
		Test: "test message",
	}

	expKeys := map[string]struct{}{
		"@id":        {},
		"test":       {},
		"@timestamp": {},
	}

	bytesData, err := json.Marshal(testData)
	s.NoError(err, "no error while marshaling test data")

	startTime := time.Now()

	err = s.adapter.SaveData(bytesData, testIndexUid)

	s.NoError(err)

	type testActualData struct {
		TestId    string `json:"@id" mapstructure:"@id"`
		Test      string `json:"test" mapstructure:"test"`
		Timestamp int64  `json:"@timestamp" mapstructure:"@timestamp"`
	}

	var actual testActualData

	upds, err := s.dbClient.Updates(testIndexUid).List()

	for i := range upds {
		upd := upds[i]
		updStat, err := s.dbClient.DefaultWaitForPendingUpdate(testIndexUid, &ml.AsyncUpdateID{UpdateID: upd.UpdateID})
		s.NoError(err)
		s.Equal(ml.UpdateStatusProcessed, updStat)
	}

	resp, err := s.dbClient.Search(testIndexUid).Search(ml.SearchRequest{
		Query: "test message",
	})
	s.NoError(err)
	data := resp.Hits[0].(map[string]interface{})
	err = mapstructure.Decode(data, &actual)
	s.WithinDuration(startTime, time.Unix(actual.Timestamp, 0), 3*time.Second)
	s.Equal(testData.Test, actual.Test)
	s.Equal(expKeys, s.adapter.keys)
}

func (s *AdapterIntegrationTestSuite) Test_SaveData_Parse_Error() {
	testIndexUid := "test"

	incorrectData := []byte("{\"test:")

	err := s.adapter.SaveData(incorrectData, testIndexUid)
	s.Error(err)

}

func Test_List_Indexes(t *testing.T) {
	ctx := context.Background()

	wfS := &wait.HTTPStrategy{
		Port: "7700/tcp",
		Path: "/health",
		StatusCodeMatcher: func(status int) bool {
			return status == http.StatusOK
		},
		ResponseMatcher: func(body io.Reader) bool {
			return true
		},
		UseTLS:       false,
		TLSConfig:    nil,
		Method:       http.MethodGet,
		Body:         nil,
		PollInterval: 50 * time.Millisecond,
	}

	wfS.WithStartupTimeout(30 * time.Second)

	req := tsc.ContainerRequest{
		Image:        "getmeili/meilisearch",
		ExposedPorts: []string{"7700/tcp"},

		WaitingFor: wfS,
	}
	mlC, err := tsc.GenericContainer(ctx, tsc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer mlC.Terminate(ctx)

	mappedPort, err := mlC.MappedPort(ctx, "7700")
	require.NoError(t, err)
	ip, err := mlC.Host(ctx)
	require.NoError(t, err)

	address := fmt.Sprintf("http://%s:%s", ip, mappedPort.Port())

	dbCli := ml.NewClient(ml.Config{Host: address})

	crIndReq := []ml.CreateIndexRequest{
		{
			Name:       "test",
			UID:        "test",
			PrimaryKey: "testId",
		},
		{
			Name:       "test2",
			UID:        "test2",
			PrimaryKey: "test2Id",
		},
	}

	for _, cInd := range crIndReq {
		resp, err := dbCli.Indexes().Create(cInd)
		require.NoError(t, err)

		require.Equal(t, cInd.UID, resp.UID)
		require.Equal(t, cInd.Name, resp.Name)
		require.Equal(t, cInd.PrimaryKey, resp.PrimaryKey)

	}
}
