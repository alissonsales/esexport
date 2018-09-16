// Package client implements methods to search and scroll documents from Elasticsearch
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/alissonsales/esexport/debug"
)

var debugCursors bool

func init() {
	debug.Init("ESEXPORTDEBUG")
}

// A HTTPClient is required to send HTTP requests to Elasticsearch
type HTTPClient interface {
	Post(string, string, io.Reader) (*http.Response, error)
}

// Client implements methods to use search and scroll documents from Elasticsearch
type Client struct {
	client           HTTPClient
	host             string
	index            string
	docType          string
	routing          string
	searchContextTTL string
}

// Hit represents a returned document from Elasticsearch
type Hit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source,omitempty"`
}

// Hits represents the hits part of a search response
type Hits struct {
	Total int   `json:"total"`
	Hits  []Hit `json:"hits"`
}

// Shards reprensets the _shards part of a search response
type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

// ESSearchResponse represents a search or scroll response from Elasticsearch
type ESSearchResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     Hits   `json:"hits"`
	Shards   Shards `json:"_shards"`
}

// NewClient returns a new Client
func NewClient(httpClient HTTPClient, host, index, docType, routing, searchContextTTL string) *Client {
	return &Client{httpClient, host, index, docType, routing, searchContextTTL}
}

// Search performs a search request using the given query
func (c *Client) Search(searchBody map[string]interface{}) (searchResponse *ESSearchResponse, err error) {
	jsonBody, err := json.Marshal(searchBody)

	if err != nil {
		return nil, err
	}

	url := c.searchURL()
	resp, err := c.client.Post(url, "application/json", bytes.NewReader(jsonBody))

	if err != nil {
		return nil, err
	}

	searchResponse, err = c.searchResponse(resp)

	return searchResponse, err
}

// Scroll performs a scroll request using the given scroll id
func (c *Client) Scroll(scrollID string) (scrollResponse *ESSearchResponse, err error) {
	scrollBody := map[string]interface{}{"scroll": c.searchContextTTL, "scroll_id": scrollID}
	jsonBody, err := json.Marshal(scrollBody)

	if err != nil {
		return nil, err
	}

	url := c.host + "/_search/scroll"
	resp, err := c.client.Post(url, "application/json", bytes.NewReader(jsonBody))

	if err != nil {
		return nil, err
	}

	scrollResponse, err = c.searchResponse(resp)

	return scrollResponse, err
}

func (c *Client) searchResponse(resp *http.Response) (searchResponse *ESSearchResponse, err error) {
	if resp.StatusCode != http.StatusOK {
		if r, e := ioutil.ReadAll(resp.Body); e == nil {
			fmt.Printf("Bad response content: %s\n", r)
		} else {
			fmt.Println("Error reading response:", e)
		}

		return nil, fmt.Errorf("Unexpected response received: %v", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("Error decoding response: %v", err)
	}

	if err := c.validateShardsResponse(searchResponse); err != nil {
		return nil, err
	}

	return searchResponse, err
}

func (c *Client) validateShardsResponse(searchResponse *ESSearchResponse) (err error) {
	// For details check:
	// https://github.com/elastic/elasticsearch-py/blob/2a96ce14f1ec81fe719bfaf1669dd2a94083f085/elasticsearch/helpers/__init__.py#L385
	// https://github.com/elastic/elasticsearch-py/issues/660
	shards := searchResponse.Shards

	if !(shards.Failed == 0 && (shards.Successful == shards.Total)) {
		err = fmt.Errorf("Response incomplete (shards response: [total: %d, successful: %d, failed: %d])", shards.Total, shards.Successful, shards.Failed)
	}

	return err
}

func (c *Client) searchURL() (url string) {
	urlParts := []string{c.host}

	if c.index != "" {
		urlParts = append(urlParts, c.index)
	} else if c.docType != "" {
		urlParts = append(urlParts, "*")
	}

	if c.docType != "" {
		urlParts = append(urlParts, c.docType)
	}

	urlParts = append(urlParts, "_search")

	queryParams := []string{}

	if c.searchContextTTL != "" {
		queryParams = append(queryParams, "scroll="+c.searchContextTTL)
	}

	if c.routing != "" {
		queryParams = append(queryParams, "routing="+c.routing)
	}

	url = strings.Join(urlParts, "/")

	if len(queryParams) > 0 {
		url = fmt.Sprintf("%s?%s", url, strings.Join(queryParams, "&"))
	}

	return url
}
