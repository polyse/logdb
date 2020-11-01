package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/polyse/logDb/test/mock"
	ml "github.com/senyast4745/meilisearch-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tsc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"net/http"
	"testing"
	"time"
)

type AdapterUnitTestSuite struct {
	suite.Suite
	adapter    *Adapter
	mockClient *mocks.ClientInterface
}

var testErr = fmt.Errorf("test error")

func (s *AdapterUnitTestSuite) SetupTest() {
	mockClient := new(mocks.ClientInterface)
	adapter := &Adapter{
		c: mockClient,
		ind: map[string]*ml.Index{
			"test": {},
		},
		keys: make(map[string]struct{}),
	}
	s.adapter = adapter
	s.mockClient = mockClient
}

func TestRunAdapterUnitTestSuite(t *testing.T) {
	suite.Run(t, new(AdapterUnitTestSuite))
}

func (s *AdapterUnitTestSuite) Test_DatabaseHealthCheck_If_Alive() {

	mockHealth := new(mocks.APIHealth)
	mockHealth.On("Get").Return(nil)

	s.mockClient.On("Health", mock.Anything).Return(mockHealth)

	s.NoError(s.adapter.DatabaseHealthCheck(), "simulate alive database")

	mockHealth.AssertExpectations(s.T())
}

func (s *AdapterUnitTestSuite) Test_DatabaseHealthCheck_If_Dead() {

	mockHealth := new(mocks.APIHealth)
	mockHealth.On("Get").Return(fmt.Errorf("test error"))

	s.mockClient.On("Health", mock.Anything).Return(mockHealth)

	s.Error(s.adapter.DatabaseHealthCheck(), "simulate dead database")

	mockHealth.AssertExpectations(s.T())
}

func (s *AdapterUnitTestSuite) Test_GetOrCreateIndex_If_Present_In_Local_Map() {

	mockIndex := new(mocks.APIIndexes)

	mockIndex.On("Get", mock.Anything).Times(0).Return(&ml.Index{}, nil)

	s.mockClient.On("Indexes", mock.Anything).Return(mockIndex)

	_, err := getOrCreateIndex(s.adapter, "test")
	s.NoError(err)
	mockIndex.AssertNotCalled(s.T(), "Get", mock.Anything)

}

func (s *AdapterUnitTestSuite) Test_GetOrCreateIndex_If_Not_Present_In_Local_Map_Present_In_Db() {

	mockIndex := new(mocks.APIIndexes)

	indexNotFound := "testDbFound"
	mockIndex.On("Get", mock.Anything).Times(1).Return(&ml.Index{}, nil)

	s.mockClient.On("Indexes", mock.Anything).Return(mockIndex)

	_, err := getOrCreateIndex(s.adapter, indexNotFound)
	s.NoError(err)
	mockIndex.AssertCalled(s.T(), "Get", indexNotFound)
	mockIndex.AssertNotCalled(s.T(), "Create", mock.Anything)

}

func (s *AdapterUnitTestSuite) Test_GetOrCreateIndex_If_Not_Present_In_Local_Map_Db_Err() {

	mockIndex := new(mocks.APIIndexes)

	indexNotFound := "testDbFound"
	mockIndex.On("Get", mock.Anything).Times(1).Return(nil, testErr)

	s.mockClient.On("Indexes", mock.Anything).Return(mockIndex)

	_, err := getOrCreateIndex(s.adapter, indexNotFound)
	s.Error(err)
	mockIndex.AssertCalled(s.T(), "Get", indexNotFound)
	mockIndex.AssertNotCalled(s.T(), "Create", mock.Anything)

}
func (s *AdapterUnitTestSuite) Test_GetOrCreateIndex_If_Not_Present_In_Local_Map_Db_Http_Err() {

	mockIndex := new(mocks.APIIndexes)

	indexNotFound := "testDbFound"
	mockIndex.On("Get", mock.Anything).Times(1).Return(nil, ml.Error{
		StatusCode: http.StatusInternalServerError,
	})

	s.mockClient.On("Indexes", mock.Anything).Return(mockIndex)

	_, err := getOrCreateIndex(s.adapter, indexNotFound)
	s.Error(err)
	mockIndex.AssertCalled(s.T(), "Get", indexNotFound)
	mockIndex.AssertNotCalled(s.T(), "Create", mock.Anything)

}

func (s *AdapterUnitTestSuite) Test_GetOrCreateIndex_If_Not_Present_In_Local_Map_Not_Present_In_Db() {

	mockIndex := new(mocks.APIIndexes)

	indexNotFound := "testNotDbFound"
	mockIndex.On("Get", mock.Anything).Times(1).Return(nil, &ml.Error{
		StatusCode: http.StatusNotFound,
	})

	mockIndex.On("Create", mock.Anything).Times(1).Return(&ml.CreateIndexResponse{
		Name:       indexNotFound,
		UID:        indexNotFound,
		UpdateID:   0,
		CreatedAt:  time.Time{},
		UpdatedAt:  time.Time{},
		PrimaryKey: "",
	}, nil)

	s.mockClient.On("Indexes", mock.Anything).Return(mockIndex)

	index, err := getOrCreateIndex(s.adapter, indexNotFound)
	s.NoError(err)
	expected := &ml.Index{
		Name:       indexNotFound,
		UID:        indexNotFound,
		CreatedAt:  time.Time{},
		UpdatedAt:  time.Time{},
		PrimaryKey: "",
	}
	s.Equal(expected, index)
	mockIndex.AssertCalled(s.T(), "Get", indexNotFound)
	mockIndex.AssertCalled(s.T(), "Create", ml.CreateIndexRequest{
		UID: indexNotFound,
	})
}

func (s *AdapterUnitTestSuite) Test_SaveData_Normal_Mode() {
	// given
	testIndexUid := "test"
	testData := struct {
		Test string `json:"test"`
	}{
		Test: "test message",
	}
	bytesData, err := json.Marshal(testData)
	s.NoError(err, "no error while marshaling test data")

	mockDocuments := new(mocks.APIDocuments)

	startTestTime := time.Now()

	type testActualData struct {
		Test      string `json:"test"`
		Timestamp int64  `json:"@timestamp"`
	}

	mockDocuments.On("AddOrReplace", mock.Anything).Run(func(args mock.Arguments) {
		actualRaw := args.Get(0).(ml.RawType)
		actual := make([]testActualData, 0, 0)
		err := json.Unmarshal(actualRaw, &actual)

		s.NoError(err)
		s.Equal(testData.Test, actual[0].Test)
		s.WithinDuration(startTestTime, time.Unix(actual[0].Timestamp, 0), 1*time.Second)
	}).Return(nil, nil)

	s.mockClient.On("Documents", mock.Anything).Return(mockDocuments)

	expKeys := map[string]struct{}{
		"test":       {},
		"@timestamp": {},
	}

	//when
	err = s.adapter.SaveData(bytesData, testIndexUid)

	//then
	s.NoError(err)
	s.Equal(expKeys, s.adapter.keys)
	mockDocuments.AssertExpectations(s.T())
}

func (s *AdapterUnitTestSuite) Test_SaveData_With_Error() {
	testIndexUid := "test"
	testData := struct {
		Test string
	}{
		Test: "test message",
	}
	bytesData, err := json.Marshal(testData)
	s.NoError(err, "no error while marshaling test data")

	mockDocuments := new(mocks.APIDocuments)

	mockDocuments.On("AddOrReplace", mock.Anything).Return(nil, testErr)

	s.mockClient.On("Documents", mock.Anything).Return(mockDocuments)

	err = s.adapter.SaveData(bytesData, testIndexUid)
	s.Error(err)
	s.Empty(s.adapter.keys)
	mockDocuments.AssertExpectations(s.T())
}

type AdapterIntegrationTestSuite struct {
	suite.Suite
	adapter  *Adapter
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
		TestId string `json:"testId"`
		Test   string `json:"test"`
	}{
		TestId: "test1",
		Test:   "test message",
	}

	expKeys := map[string]struct{}{
		"testId":     {},
		"test":       {},
		"@timestamp": {},
	}

	bytesData, err := json.Marshal(testData)
	s.NoError(err, "no error while marshaling test data")

	startTime := time.Now()

	err = s.adapter.SaveData(bytesData, testIndexUid)

	s.NoError(err)

	type testActualData struct {
		TestId    string `json:"testId"`
		Test      string `json:"test"`
		Timestamp int64  `json:"@timestamp"`
	}

	var actual testActualData

	upds, err := s.dbClient.Updates(testIndexUid).List()

	for i := range upds {
		upd := upds[i]
		updStat, err := s.dbClient.DefaultWaitForPendingUpdate(testIndexUid, &ml.AsyncUpdateID{UpdateID: upd.UpdateID})
		s.NoError(err)
		s.Equal(ml.UpdateStatusProcessed, updStat)
	}

	err = s.dbClient.Documents(testIndexUid).Get("test1", &actual)
	s.NoError(err)
	s.WithinDuration(startTime, time.Unix(actual.Timestamp, 0), 3*time.Second)
	s.Equal(testData.TestId, actual.TestId)
	s.Equal(testData.Test, actual.Test)
	s.Equal(expKeys, s.adapter.keys)
}

func (s *AdapterIntegrationTestSuite) Test_SaveData_Parse_Error() {
	testIndexUid := "test"

	incorrectData := []byte("{\"test:")

	err := s.adapter.SaveData(incorrectData, testIndexUid)
	s.Error(err)

}

func (s *AdapterIntegrationTestSuite) Test_SaveData_Db_Error() {
	testIndexUid := "test"

	incorrectData := []byte("{\"test\":1}")

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
