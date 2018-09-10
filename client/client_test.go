package client

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

type MockHTTPClient struct {
	PostArgsReceived struct {
		URL         string
		ContentType string
		Body        io.Reader
	}
	PostResponse struct {
		Response *http.Response
		Err      error
	}
}

func (m *MockHTTPClient) Post(url, contentType string, body io.Reader) (*http.Response, error) {
	args := &m.PostArgsReceived
	args.URL = url
	args.ContentType = contentType
	args.Body = body
	return m.PostResponse.Response, m.PostResponse.Err
}

func TestSearchRequestURL(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{}`))}

	scenarios := []struct {
		host             string
		index            string
		docType          string
		routing          string
		searchContextTTL string
		expectedURL      string
	}{
		{
			"http://localhost:9200",
			"",
			"",
			"",
			"",
			"http://localhost:9200/_search",
		},
		{
			"http://localhost:9200",
			"my_index",
			"",
			"",
			"",
			"http://localhost:9200/my_index/_search",
		},
		{
			"http://localhost:9200",
			"",
			"my_type",
			"",
			"",
			"http://localhost:9200/*/my_type/_search",
		},
		{
			"http://localhost:9200",
			"my_index",
			"my_type",
			"",
			"",
			"http://localhost:9200/my_index/my_type/_search",
		},
		{
			"http://localhost:9200",
			"my_index",
			"my_type",
			"my_routing",
			"",
			"http://localhost:9200/my_index/my_type/_search?routing=my_routing",
		},
		{
			"http://localhost:9200",
			"my_index",
			"my_type",
			"",
			"1m",
			"http://localhost:9200/my_index/my_type/_search?scroll=1m",
		},
		{
			"http://localhost:9200",
			"my_index",
			"my_type",
			"my_routing",
			"1m",
			"http://localhost:9200/my_index/my_type/_search?scroll=1m&routing=my_routing",
		},
	}

	for _, scenario := range scenarios {
		esClient := NewClient(mockHTTPClient, scenario.host, scenario.index, scenario.docType, scenario.routing, scenario.searchContextTTL)
		esClient.Search(map[string]interface{}{})

		if scenario.expectedURL != mockHTTPClient.PostArgsReceived.URL {
			t.Errorf("Expected url to be '%v', but got '%v'", scenario.expectedURL, mockHTTPClient.PostArgsReceived.URL)
		}
	}
}

func TestSearchWhenRequestFailed(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 500,
		Body:       ioutil.NopCloser(strings.NewReader(`{}`))}
	mockHTTPClient.PostResponse.Err = errors.New("request error")

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "")
	_, err := esClient.Search(map[string]interface{}{})

	if err == nil {
		t.Error("Expected search to fail")
	}

	if err.Error() != "request error" {
		t.Errorf("Search returned an unexpected error: %v", err)
	}
}

func TestSearchWhenResponseIsNotValid(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 500,
		Body:       ioutil.NopCloser(strings.NewReader(`{}`))}

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "")
	_, err := esClient.Search(map[string]interface{}{})

	if err == nil {
		t.Error("Expected search to fail")
	}

	if err.Error() != "Unexpected response received: 500" {
		t.Errorf("Search returned an unexpected error: %v", err)
	}
}

func TestSearchWhenAShardFailed(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	incompleteResponse := `
	{
		"_scroll_id": "scroll_id",
		"_shards": { "total": 2, "successful": 1, "failed": 1 },
		"hits": {
			"total": 1,
			"hits": [
			{ "_id": "id", "_source": { "field": "value" } }
			]
		}
	}
	`
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(incompleteResponse))}

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "")
	_, err := esClient.Search(map[string]interface{}{})

	if err == nil {
		t.Error("Expected search to fail")
	}

	if err.Error() != "Response incomplete (shards response: [total: 2, successful: 1, failed: 1])" {
		t.Errorf("Search returned an unexpected error: %v", err)
	}
}

func TestSearchWhenAShardIsMissing(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	incompleteResponse := `
	{
		"_scroll_id": "scroll_id",
		"_shards": { "total": 2, "successful": 1, "failed": 0 },
		"hits": {
			"total": 1,
			"hits": [
			{ "_id": "id", "_source": { "field": "value" } }
			]
		}
	}
	`
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(incompleteResponse))}

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "")
	_, err := esClient.Search(map[string]interface{}{})

	if err == nil {
		t.Error("Expected search to fail")
	}

	if err.Error() != "Response incomplete (shards response: [total: 2, successful: 1, failed: 0])" {
		t.Errorf("Search returned an unexpected error: %v", err)
	}
}

func TestSearch(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	successfulResponse := `
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
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(successfulResponse))}

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "")
	rawQuery := `{"query":{"match_all":{}}}`
	var query map[string]interface{}
	json.Unmarshal([]byte(rawQuery), &query)
	resp, err := esClient.Search(query)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	queryReceived, _ := ioutil.ReadAll(mockHTTPClient.PostArgsReceived.Body)

	if string(queryReceived) != rawQuery {
		t.Errorf("Wrong query performed. Expected: '%v', got '%v'", rawQuery, string(queryReceived))
	}

	if resp.Hits.Total != 1 {
		t.Errorf("Expected total to be equal %v, got %v", 1, resp.Hits.Total)
	}

	if resp.Hits.Hits[0].ID != "id" {
		t.Error("Unexpected document returned (id mismatch)")
	}

	if resp.Hits.Hits[0].Source["field"] != "value" {
		t.Error("Unexpected document returned (field mismatch)")
	}
}

func TestScrollURL(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{}`))}

	host := "http://localhost:9200"
	index := "my_index"
	docType := "my_type"
	routing := "my_routing"
	searchContextTTL := "1m"
	expectedURL := "http://localhost:9200/_search/scroll"

	esClient := NewClient(mockHTTPClient, host, index, docType, routing, searchContextTTL)
	esClient.Scroll("aScrollId")

	if expectedURL != mockHTTPClient.PostArgsReceived.URL {
		t.Errorf("Expected url to be '%v', but got '%v'", expectedURL, mockHTTPClient.PostArgsReceived.URL)
	}
}

func TestScrollWhenRequestFailed(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 500,
		Body:       ioutil.NopCloser(strings.NewReader(`{}`))}
	mockHTTPClient.PostResponse.Err = errors.New("request error")

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "")
	_, err := esClient.Scroll("aScrollId")

	if err == nil {
		t.Error("Expected scroll to fail")
	}

	if err.Error() != "request error" {
		t.Errorf("Scroll returned an unexpected error: %v", err)
	}
}

func TestScroll(t *testing.T) {
	mockHTTPClient := &MockHTTPClient{}
	successfulResponse := `
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
	mockHTTPClient.PostResponse.Response = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(successfulResponse))}

	esClient := NewClient(mockHTTPClient, "http://localhost:9200", "", "", "", "1m")
	resp, err := esClient.Scroll("aScrollId")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedRawBody := `{"scroll":"1m","scroll_id":"aScrollId"}`
	bodyReceived, _ := ioutil.ReadAll(mockHTTPClient.PostArgsReceived.Body)

	if string(bodyReceived) != expectedRawBody {
		t.Errorf("Wrong query performed. Expected: '%v', got '%v'", expectedRawBody, string(bodyReceived))
	}

	if resp.ScrollID != "scroll_id" {
		t.Errorf("Expected ScrollId to be '%v', got '%v'", "scroll_id", resp.ScrollID)
	}

	if resp.Hits.Total != 1 {
		t.Errorf("Expected total to be equal %v, got %v", 1, resp.Hits.Total)
	}

	if resp.Hits.Hits[0].ID != "id" {
		t.Error("Unexpected document returned (id mismatch)")
	}

	if resp.Hits.Hits[0].Source["field"] != "value" {
		t.Error("Unexpected document returned (field mismatch)")
	}
}
