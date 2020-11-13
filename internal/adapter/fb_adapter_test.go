package adapter

import (
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	ml "github.com/meilisearch/meilisearch-go"
	"github.com/polyse/logdb/test/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"regexp"
	"testing"
	"time"
)

type FBAdapterUnitTestSuite struct {
	suite.Suite
	adapter    *FBAdapter
	mockClient *mocks.ClientInterface
}

func (s *FBAdapterUnitTestSuite) SetupTest() {
	mockClient := new(mocks.ClientInterface)
	adapter := &FBAdapter{
		c: mockClient,
		ind: map[string]*ml.Index{
			"test": {},
		},
		keys: make(map[string]struct{}),
	}
	s.adapter = adapter
	s.mockClient = mockClient

	var err error
	regex, err = regexp.Compile("[^a-zA-Z0-9]+")
	s.NoError(err)
}

func TestRunFBAdapterUnitTestSuite(t *testing.T) {
	suite.Run(t, new(FBAdapterUnitTestSuite))
}

func (s *FBAdapterUnitTestSuite) Test_DatabaseHealthCheck_If_Alive() {
	mockHealth := new(mocks.APIHealth)
	mockHealth.On("Get").Return(nil)

	s.mockClient.On("Health", mock.Anything).Return(mockHealth)

	s.NoError(s.adapter.DatabaseHealthCheck(), "simulate alive database")

	mockHealth.AssertExpectations(s.T())
}

func (s *FBAdapterUnitTestSuite) Test_DatabaseHealthCheck_If_Dead() {

	mockHealth := new(mocks.APIHealth)
	mockHealth.On("Get").Return(fmt.Errorf("test error"))

	s.mockClient.On("Health", mock.Anything).Return(mockHealth)

	s.Error(s.adapter.DatabaseHealthCheck(), "simulate dead database")

	mockHealth.AssertExpectations(s.T())
}
func (s *FBAdapterUnitTestSuite) Test_getTs() {

	testFBTime := time.Date(2020, time.January, 1, 1, 0, 0, 0, time.Local)
	testUxTime := time.Date(2020, time.January, 2, 1, 0, 0, 0, time.Local)
	for _, t := range []struct {
		testName string
		input    interface{}
		expect   time.Time
	}{
		{"FBTime input", output.FLBTime{Time: testFBTime}, testFBTime},
		{"unix time input", uint64(testUxTime.Unix()), testUxTime},
		{"undefined time input", struct{}{}, time.Now()},
	} {
		s.Run(t.testName, func() {
			actT := getTs(t.input)
			s.WithinDuration(t.expect, actT, 1*time.Second)
		})

	}
}

func (s *FBAdapterUnitTestSuite) Test_checkKeysContains_Presents_In_Cache() {
	rec := map[interface{}]interface{}{
		"test": "test",
	}
	s.adapter.keys = map[string]struct{}{
		"test": {},
	}
	expected := RawData{
		"test": "test",
	}
	act := make(RawData)

	nKeys, err := checkKeysContains(rec, &act, s.adapter)
	s.False(nKeys)
	s.NoError(err)
	s.Equal(expected, act)
}

func (s *FBAdapterUnitTestSuite) Test_checkKeysContains_Not_Presents_In_Cache() {
	rec := map[interface{}]interface{}{
		"test": "test",
	}
	expected := RawData{
		"test": "test",
	}
	var act RawData

	nKeys, err := checkKeysContains(rec, &act, s.adapter)
	s.True(nKeys)
	s.NoError(err)
	s.Equal(expected, act)
}

func (s *FBAdapterUnitTestSuite) Test_checkKeysContains_With_Error() {
	rec := map[interface{}]interface{}{
		struct{}{}: "test",
	}
	var act RawData
	_, err := checkKeysContains(rec, &act, s.adapter)
	s.Error(err)
}
