[![Build Status](https://travis-ci.org/alissonsales/esexport.svg?branch=master)](https://travis-ci.org/alissonsales/esexport)

# esexport

Command line tool to export documents from Elasticsearch using sliced scroll.

Also useful to test the speed of scrolling using different combinations of params:

* number of slices
* different slice fields
* query (size and source)

See [Debugging Cursors](#debugging-cursors).

# Installation

For now you need to install go and use `go get -u github.com/alissonsales/esexport`

TODO: provide binaries on /releases and homebrew formula

# Usage

```
Usage: esexport [global flags]

global flags:
  -host string
    	ES Host (default "http://localhost:9200")
  -index string
    	Index to search (will be appended on the search url)
  -output string
    	Output file
  -query string
    	Query to slice (default "{}")
  -routing string
    	Routing passed to the query
  -searchContextTTL string
    	Search context TTL used to search and scroll (default "1m")
  -sliceField string
    	The field used to slice the query
  -sliceSize int
    	Number of slices (default 1)
  -type string
    	Document type (will be appended on the search url)

Examples:
	esexport -sliceSize 2 -query '{"source":["false"], "size": 1000, "query":{"bool":{"filter":{"term":{"field":"value"}}}}}'
```

# Controlling search/scroll behaviour

There are no options to control:

* the number of documents returned per query
* the fields exported/retrieved

Add `_source` and `size` directly in your query body to control such things.

## Note

Sliced scrolls where introduced on Elasticsearch 5.

If you point to any cluster still running ES prior to 5 and try to use -sliceSize > 1 the program will fail.

Exporting documents from installations prior to 5 works just fine without the use of -sliceSize.

# Debugging cursors

Add `ESEXPORTDEBUG=1` to display debug information about the execution.

```
ESEXPORTDEBUG=1 esexport -query '{"_source":[false],"size": 1000}' -sliceSize 2 -output docs.out
Slice 1 query: {"_source":[false],"size":1000,"slice":{"id":1,"max":2}}
Slice 0 query: {"_source":[false],"size":1000,"slice":{"id":0,"max":2}}
Slice 1 total: 93113
Slice 0 total: 185582
Progress: [184000/278695] 66%
Cursor 1 took 7.699094263s
Progress: [276113/278695] 99%
Cursor 0 took 13.217493396s
Progress: [278695/278695] 100%
esexport took 13.560569369s
```

# Output

To control the fields returned just change your query "_source".

```
{"_id":"5af4fd9b020bbd8e0369683b","_source":{"group":10}}
{"_id":"5af4fd9b020bbd8e03696867","_source":{"group":4}}
{"_id":"5af4fd9b020bbd8e03696873","_source":{"group":1}}
{"_id":"5af4fd9b020bbd8e036968ab","_source":{"group":2}}
```
