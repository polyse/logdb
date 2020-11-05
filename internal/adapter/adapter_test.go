package adapter

import (
	"encoding/json"
	"fmt"
	"github.com/polyse/logDb/test/mock"
	ml "github.com/senyast4745/meilisearch-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"net/http"
	"testing"
	"time"
)

type AdapterUnitTestSuite struct {
	suite.Suite
	adapter    *SimpleAdapter
	mockClient *mocks.ClientInterface
}

var testErr = fmt.Errorf("test error")

func (s *AdapterUnitTestSuite) SetupTest() {
	mockClient := new(mocks.ClientInterface)
	adapter := &SimpleAdapter{
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

func (s *AdapterUnitTestSuite) Test_SaveData_Keys_Presents_local() {
	// given

	s.adapter.keys = map[string]struct{}{
		"test":       {},
		"@timestamp": {},
		"@id":        {},
	}
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
		Id        string `json:"@id"`
		Test      string `json:"test"`
		Timestamp int64  `json:"@timestamp"`
	}

	mockDocuments.On("AddOrReplace", mock.Anything).Times(1).Run(func(args mock.Arguments) {
		actualRaw := args.Get(0).(ml.RawType)
		actual := make([]testActualData, 0, 0)
		err := json.Unmarshal(actualRaw, &actual)

		s.NoError(err)
		s.Equal(testData.Test, actual[0].Test)
		s.WithinDuration(startTestTime, time.Unix(actual[0].Timestamp, 0), 1*time.Second)
	}).Return(nil, nil)

	s.mockClient.On("Documents", mock.Anything).Return(mockDocuments)

	//when
	err = s.adapter.SaveData(bytesData, testIndexUid)

	//then
	s.NoError(err)
	mockDocuments.AssertExpectations(s.T())
}

func (s *AdapterUnitTestSuite) Test_SaveData_Keys_Not_Presents_local() {
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
		Id        string `json:"@id"`
	}

	mockDocuments.On("AddOrReplace", mock.Anything).Times(2).Run(func(args mock.Arguments) {
		if actualRaw, ok := args.Get(0).(ml.RawType); ok {
			actual := make([]testActualData, 0, 0)
			err := json.Unmarshal(actualRaw, &actual)

			s.NoError(err)
			s.Equal(testData.Test, actual[0].Test)
			s.WithinDuration(startTestTime, time.Unix(actual[0].Timestamp, 0), 1*time.Second)
		}
	}).Return(nil, nil)

	s.mockClient.On("Documents", mock.Anything).Return(mockDocuments)

	expKeys := map[string]struct{}{
		"test":       {},
		"@timestamp": {},
		"@id":        {},
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
