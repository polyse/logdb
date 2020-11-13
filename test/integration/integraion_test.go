// +build integration

package integration

import (
	"fmt"
	"github.com/google/uuid"
	ml "github.com/meilisearch/meilisearch-go"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

type FBAdapterIntegrationTestSuite struct {
	suite.Suite
	dbClient  ml.ClientInterface
	compose   tc.DockerCompose
	logClient net.Conn
}

var (
	dockerComposeFBFile = "./docker-compose-fb-test.yaml"
	dockerComposeFDFile = "./docker-compose-fd-test.yaml"
	envSetting          string
)

func (s *FBAdapterIntegrationTestSuite) SetupSuite() {
	envSetting = os.Getenv("TEST_MODE")
}

func (s *FBAdapterIntegrationTestSuite) SetupTest() {
	composeFilePaths := []string{dockerComposeFBFile}
	if envSetting == "FD" {
		composeFilePaths = []string{dockerComposeFDFile}
	}
	identifier := strings.ToLower(uuid.New().String())

	log.Debug().Strs("docker comp", composeFilePaths).Msg("print docker compose")

	compose := tc.NewLocalDockerCompose(composeFilePaths, identifier)
	execError := compose.
		WithCommand([]string{"up", "-d"}).
		Invoke()
	err := execError.Error

	s.NoError(err)
	s.compose = compose

	c, err := net.Dial("tcp", "127.0.0.1:5170")
	s.NoError(err)

	s.NoError(err)
	s.logClient = c
	s.NoError(err)

	cfg := ml.Config{
		Host:   "http://127.0.0.1:7700",
		APIKey: "",
	}

	s.dbClient = ml.NewClient(cfg)
}

func (s *FBAdapterIntegrationTestSuite) TearDownTest() {
	err := s.logClient.Close()
	s.NoError(err)
	execErr := s.compose.Down()
	s.NoError(execErr.Error)
}

func TestRunFBAdapterIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(FBAdapterIntegrationTestSuite))
}

func (s *FBAdapterIntegrationTestSuite) Test_E2E_Normal_data() {
	err := s.dbClient.Health().Get()
	s.NoError(err)
	text := "{\"message\":\"data\"}"
	err = s.logClient.SetDeadline(time.Now().Add(5 * time.Second))
	s.NoError(err)
	_, err = fmt.Fprintf(s.logClient, text+"\n")
	s.NoError(err)

	_, err = fmt.Fprintf(s.logClient, text+"\n")
	s.NoError(err)

	time.Sleep(1 * time.Second)

	li, err := s.dbClient.Indexes().List()
	s.NoError(err)
	indUID := li[0].UID

	upds, err := s.dbClient.Updates(indUID).List()
	s.NoError(err)

	var docs []map[string]interface{}
	err = s.dbClient.Documents(indUID).List(ml.ListDocumentsRequest{}, &docs)
	s.NoError(err)

	for _, u := range upds {
		_, err := s.dbClient.DefaultWaitForPendingUpdate(indUID, &ml.AsyncUpdateID{UpdateID: u.UpdateID})
		s.NoError(err)
	}

	var keyM map[string]interface{}
	var dataA []map[string]interface{}
	for _, d := range docs {
		if _, ok := d["Keys"]; ok {
			keyM = d
		} else {
			dataA = append(dataA, d)
		}

	}

	expKey := []interface{}{"@id", "@timestamp", "message"}
	s.ElementsMatch(expKey, keyM["Keys"].([]interface{}))
	actT, err := time.Parse(time.RFC3339Nano, keyM["@timestamp"].(string))
	s.NoError(err)
	s.WithinDuration(time.Now(), actT, 5*time.Second)

	s.Equal(2, len(dataA))
	for _, d := range dataA {
		s.Equal("data", d["message"])
		actT, err = time.Parse(time.RFC3339Nano, keyM["@timestamp"].(string))
		s.NoError(err)
		s.WithinDuration(time.Now(), actT, 5*time.Second)

	}

}

func (s *FBAdapterIntegrationTestSuite) Test_E2E_Incorrect_data() {
	err := s.dbClient.Health().Get()
	s.NoError(err)
	text := "{\"message\":\"dat"
	err = s.logClient.SetDeadline(time.Now().Add(1 * time.Second))
	s.NoError(err)
	_, err = fmt.Fprintf(s.logClient, text+"\n")
	s.NoError(err)

	_, err = fmt.Fprintf(s.logClient, text+"\n")
	s.NoError(err)

	time.Sleep(1 * time.Second)

	li, err := s.dbClient.Indexes().List()
	s.NoError(err)
	s.Equal(0, len(li))

}
