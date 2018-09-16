package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/alissonsales/esexport/client"
	"github.com/alissonsales/esexport/cursor"
	"github.com/alissonsales/esexport/debug"
)

const examples = `
Examples:
	esexport -sliceSize 2 -query '{"source":["false"], "size": 1000, "query":{"bool":{"filter":{"term":{"field":"value"}}}}}'
`

func init() {
	debug.Init("ESEXPORTDEBUG")
}

func main() {
	defer timeTrack(time.Now(), "esexport")

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	host := fs.String("host", "http://localhost:9200", "ES Host")
	query := fs.String("query", "{}", "Query to slice")
	routing := fs.String("routing", "", "Routing passed to the query")
	searchContextTTL := fs.String("searchContextTTL", "1m", "Search context TTL used to search and scroll")
	index := fs.String("index", "", "Index to search (will be appended on the search url)")
	docType := fs.String("type", "", "Document type (will be appended on the search url)")
	sliceSize := fs.Int("sliceSize", 1, "Number of slices")
	sliceField := fs.String("sliceField", "", "The field used to slice the query")
	output := fs.String("output", "", "Output file")

	fs.Usage = func() {
		fmt.Println("Usage: esexport [global flags]")
		fmt.Printf("\nglobal flags:\n")
		fs.PrintDefaults()

		fmt.Println(examples)
	}

	fs.Parse(os.Args[1:])

	httpClient := &http.Client{}
	esClient := client.NewClient(httpClient, *host, *index, *docType, *routing, *searchContextTTL)
	jsonQuery, err := jsonQuery(query)

	if err != nil {
		fmt.Println("Error parsing query:", err)
		os.Exit(1)
	}

	var outputFile *os.File

	if *output != "" {
		outputFile, err = os.OpenFile(*output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		defer outputFile.Close()

		if err != nil {
			fmt.Println("Error creating output file:", err)
			os.Exit(1)
		}
	}

	cursors := make([]*cursor.SlicedScrollCursor, *sliceSize)

	var wg sync.WaitGroup

	for i := range cursors {
		ssc, err := cursor.NewSlicedScrollCursor(esClient, i, *sliceSize, *sliceField, jsonQuery)

		if err != nil {
			fmt.Println("Error creating cursor:", err)
			os.Exit(1)
		}

		cursors[i] = ssc

		wg.Add(1)

		go func(cursor *cursor.SlicedScrollCursor, ID int) {
			defer timeTrack(time.Now(), fmt.Sprintf("\nCursor %v", ID))
			defer wg.Done()

			err = processCursor(cursor, outputFile)

			if err != nil {
				fmt.Printf("Error processing cursor %v: %v\n", ID, err)
			}
		}(ssc, i)
	}

	done := make(chan struct{})
	go printProgress(cursors, done)

	wg.Wait()
	done <- struct{}{}
	<-done

	fmt.Println("\r")
}

func jsonQuery(query *string) (map[string]interface{}, error) {
	var jsonQuery map[string]interface{}
	err := json.Unmarshal([]byte(*query), &jsonQuery)

	return jsonQuery, err
}

func processCursor(ssc *cursor.SlicedScrollCursor, outputFile *os.File) error {
	for {
		hits, err := ssc.Next()

		if err != nil {
			return err
		}

		if len(hits) == 0 {
			break
		}

		if outputFile != nil {
			err := writeHitsToFile(hits, outputFile)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func writeHitsToFile(hits []client.Hit, f *os.File) error {
	for _, hit := range hits {
		j, err := json.Marshal(hit)

		if err != nil {
			return err
		}

		if _, err := f.Write([]byte(string(j) + "\n")); err != nil {
			return err
		}
	}

	return nil
}

func printProgress(cursors []*cursor.SlicedScrollCursor, done chan struct{}) {
	var total *int
	var current *int
timer:
	for {
		select {
		case <-done:
			break timer
		default:
			time.Sleep(500 * time.Millisecond)
			current, total = processingProgress(cursors)

			if total == nil || current == nil {
				continue
			}
		}

		percent := 0.0

		if *total > 0 {
			percent = (float64(*current) / float64(*total)) * 100.0
		}

		fmt.Printf("Progress: [%d/%d] %.0f%%\r", *current, *total, percent)
	}

	if current != nil && total != nil {
		fmt.Printf("Progress: [%d/%d] %.0f%%\r", *current, *total, 100.0)
	}

	done <- struct{}{}
}

func processingProgress(cursors []*cursor.SlicedScrollCursor) (current, total *int) {
	t := 0
	c := 0

	for _, cursor := range cursors {
		if cursor.Total != nil && cursor.NumDocsRetrieved != nil {
			t += *cursor.Total
			c += *cursor.NumDocsRetrieved
		} else {
			return nil, nil
		}
	}

	return &c, &t
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	debug.Debug(func() { fmt.Printf("%s took %s\n", name, elapsed) })
}
