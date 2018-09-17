package cursor

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/alissonsales/esexport/client"
	"github.com/alissonsales/esexport/debug"
)

// ElasticsearchClient is used to search and scroll documents from Elasticsearch
type ElasticsearchClient interface {
	Scroll(scrollID string) (*client.ESSearchResponse, error)
	Search(searchBody map[string]interface{}) (*client.ESSearchResponse, error)
}

// SlicedScrollCursor implements a way to search and scroll documents from Elasticsearch using slices
type SlicedScrollCursor struct {
	client           ElasticsearchClient
	query            map[string]interface{}
	sliceID          int
	sliceMax         int
	sliceField       string
	Total            *int
	NumDocsRetrieved *int
	lastScrollID     string
}

// NewSlicedScrollCursor returns a SliceScrollCursor
func NewSlicedScrollCursor(client ElasticsearchClient, id, max int, field string, query map[string]interface{}) (*SlicedScrollCursor, error) {
	if max >= 2 && id >= max {
		return nil, errors.New("Max must be greater than id")
	}

	return &SlicedScrollCursor{client: client, query: query, sliceID: id, sliceMax: max, sliceField: field}, nil
}

// Next returns the next batch of results for the given query
//
// Returns an empty array if there are no more documents to be returned
func (ssc *SlicedScrollCursor) Next() (hits []client.Hit, err error) {
	if ssc.Total == nil {
		debug.Debug(func() {
			if jsonBody, err := json.Marshal(ssc.searchQuery()); err == nil {
				fmt.Printf("Slice %v query: %s\n", ssc.sliceID, jsonBody)
			}
		})
		hits, err = ssc.search()

		if ssc.Total != nil {
			debug.Debug(func() {
				fmt.Printf("Slice %v total: %v\n", ssc.sliceID, *ssc.Total)
			})
		}
	} else if !ssc.done() {
		hits, err = ssc.scroll(ssc.lastScrollID)
	}

	return hits, err
}

func (ssc *SlicedScrollCursor) search() (hits []client.Hit, err error) {
	resp, err := ssc.client.Search(ssc.searchQuery())

	if err != nil {
		return nil, err
	}

	ssc.Total = &resp.Hits.Total
	totalReturned := len(resp.Hits.Hits)
	ssc.NumDocsRetrieved = &totalReturned
	ssc.lastScrollID = resp.ScrollID

	return resp.Hits.Hits, err
}

func (ssc *SlicedScrollCursor) scroll(id string) (hits []client.Hit, err error) {
	resp, err := ssc.client.Scroll(id)

	if err != nil {
		return nil, err
	}

	updatedTotal := len(resp.Hits.Hits) + *ssc.NumDocsRetrieved
	ssc.NumDocsRetrieved = &updatedTotal
	ssc.lastScrollID = resp.ScrollID

	return resp.Hits.Hits, err
}

func (ssc *SlicedScrollCursor) done() bool {
	if *ssc.NumDocsRetrieved == *ssc.Total {
		return true
	}

	return false
}

func (ssc *SlicedScrollCursor) searchQuery() map[string]interface{} {
	query := make(map[string]interface{})

	for k, v := range ssc.query {
		query[k] = v
	}

	if ssc.sliceMax > 1 {
		slice := map[string]interface{}{}
		slice["id"] = ssc.sliceID
		slice["max"] = ssc.sliceMax

		if ssc.sliceField != "" {
			slice["field"] = ssc.sliceField
		}

		query["slice"] = slice
	}

	return query
}
