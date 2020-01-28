# esmarc.py

esmarc is a python3 tool to read line-delimited MARC21 JSON from an elasticSearch index, perform a mapping and writes the output in a directory with a file for each mapping type.

dependencies:
python3-elasticsearch
efre-lod-elasticsearch-tools

run:

```
$ esmarc.py <OPTARG>
	-h, --help            show this help message and exit
	-host HOST            hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.
	-port PORT            Port of the ElasticSearch-node to use, default is 9200.
	-type TYPE            ElasticSearch Type to use
	-index INDEX          ElasticSearch Index to use
	-id ID                map single document, given by id
	-help                 print this help
	-prefix PREFIX        Prefix to use for output data
	-debug                Dump processed Records to stdout (mostly used for debug-purposes)
	-server SERVER        use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty.
	-pretty               output tabbed json
	-w W                  how many processes to use
	-idfile IDFILE        path to a file with IDs to process
	-query QUERY          prefilter the data based on an elasticsearch-query

```
