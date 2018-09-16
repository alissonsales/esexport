package cursor

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/alissonsales/esexport/client"
)

type MockElasticSearchClient struct {
	SearchArgsReceived struct {
		SearchBody map[string]interface{}
	}
	SearchReturn struct {
		Response *client.ESSearchResponse
		Err      error
	}
	ScrollArgsReceived struct {
		ScrollID string
	}
	ScrollReturn struct {
		Response *client.ESSearchResponse
		Err      error
	}
}

func (m *MockElasticSearchClient) Scroll(scrollID string) (*client.ESSearchResponse, error) {
	m.ScrollArgsReceived.ScrollID = scrollID
	return m.ScrollReturn.Response, m.ScrollReturn.Err
}

func (m *MockElasticSearchClient) Search(searchBody map[string]interface{}) (*client.ESSearchResponse, error) {
	m.SearchArgsReceived.SearchBody = searchBody
	return m.SearchReturn.Response, m.SearchReturn.Err
}

func TestNewSlicedScrollCursorWithInvalidArgs(t *testing.T) {
	mockClient := &MockElasticSearchClient{}
	scenarios := []struct {
		id  int
		max int
		err error
	}{
		{0, 0, nil},
		{0, 1, nil},
		{2, 2, errors.New("Max must be greater than id")},
		{3, 2, errors.New("Max must be greater than id")},
	}

	for _, scenario := range scenarios {
		_, err := NewSlicedScrollCursor(mockClient, scenario.id, scenario.max, "", map[string]interface{}{})

		if err != nil && err.Error() != scenario.err.Error() {
			t.Errorf("Expected error to be '%v', got '%v'", scenario.err, err)
		}
	}
}

func TestNextSliceBody(t *testing.T) {
	mockClient := &MockElasticSearchClient{}
	searchResponse := `
	{
		"_scroll_id": "scroll_id",
		"_shards": { "total": 2, "successful": 2, "failed": 0 },
		"hits": {
			"total": 1,
			"hits": [
			{ "_id": "id", "_source": { "field": "value" } }
			]
		}
	}
	`
	json.NewDecoder(strings.NewReader(searchResponse)).Decode(&mockClient.SearchReturn.Response)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	scenarios := []struct {
		id           int
		max          int
		field        string
		expectedBody string
	}{
		{0, 0, "", `{"query":{"match_all":{}}}`},
		{1, 0, "", `{"query":{"match_all":{}}}`},
		{0, 0, "my_field", `{"query":{"match_all":{}}}`},
		{0, 1, "", `{"query":{"match_all":{}}}`},
		{1, 2, "", `{"query":{"match_all":{}},"slice":{"id":1,"max":2}}`},
		{1, 2, "my_field", `{"query":{"match_all":{}},"slice":{"field":"my_field","id":1,"max":2}}`},
	}

	for _, scenario := range scenarios {
		cursor, err := NewSlicedScrollCursor(mockClient, scenario.id, scenario.max, scenario.field, query)

		if err != nil {
			fmt.Printf("id: %v, max: %v\n", scenario.id, scenario.max)
			t.Fatalf("Failed to create cursor: %v", err)
		}

		cursor.Next()

		receivedBody, err := json.Marshal(mockClient.SearchArgsReceived.SearchBody)

		if err != nil {
			t.Errorf("Failed to parse query")
		}

		if scenario.expectedBody != string(receivedBody) {
			t.Errorf("Expected cursor query to be '%v', got '%s'", scenario.expectedBody, receivedBody)
		}
	}
}

func TestNext(t *testing.T) {
	mockClient := &MockElasticSearchClient{}
	searchResponse := &client.ESSearchResponse{
		ScrollID: "aScrollId",
		Hits: client.Hits{
			Total: 1,
			Hits:  []client.Hit{client.Hit{ID: "docId", Source: map[string]interface{}{}}},
		},
	}
	mockClient.SearchReturn.Response = searchResponse

	ssc, err := NewSlicedScrollCursor(mockClient, 1, 2, "", map[string]interface{}{})

	if err != nil {
		t.Fatalf("Failed to create SlicedScrollCursor: %v", err)
	}

	exhaustCursor := func(cursor *SlicedScrollCursor) (numCalls, numHits int) {
		for {
			hits, err := ssc.Next()

			if err != nil {
				t.Errorf("Failed to retrieve next batch of hits: %v", err)
			}

			if len(hits) > 0 {
				numCalls++
				numHits += len(hits)
			} else {
				break
			}
		}
		return
	}

	numCalls, numHits := exhaustCursor(ssc)

	if numCalls != 1 {
		t.Errorf("Expected number of iteractions to be 1, performed %v", numCalls)
	}

	if numHits != 1 {
		t.Errorf("Expected number of total hits to be 1, got %v", numHits)
	}

	searchResponse.Hits.Total = 2
	mockClient.ScrollReturn.Response = searchResponse
	ssc, err = NewSlicedScrollCursor(mockClient, 1, 2, "", map[string]interface{}{})
	numCalls, numHits = exhaustCursor(ssc)

	if numCalls != 2 {
		t.Errorf("Expected number of iteractions to be 2, performed %v", numCalls)
	}

	if numHits != 2 {
		t.Errorf("Expected number of total hits to be 2, got %v", numHits)
	}

	searchResponse.Hits.Total = 3
	mockClient.ScrollReturn.Response = searchResponse
	ssc, err = NewSlicedScrollCursor(mockClient, 1, 2, "", map[string]interface{}{})
	numCalls, numHits = exhaustCursor(ssc)

	if numCalls != 3 {
		t.Errorf("Expected number of iteractions to be 3, performed %v", numCalls)
	}

	if numHits != 3 {
		t.Errorf("Expected number of total hits to be 3, got %v", numHits)
	}
}
