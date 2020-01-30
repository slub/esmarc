# Installation

run:
```
pip3 install . --user
```

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


<a name="entityfacts-bot.py"/>

# entityfacts-bot.py 

entityfacts-bot.py is a Python3 program that enrichs ("links") your data with more identifiers from entitiyfacts.  Prerequisits is that you have a field containing your GND-Identifier.


It connects to an elasticsearch node and outputs the enriched data, which can be put back to the index using esbulk.

## Usage

```
./entityfacts-bot.py
    -h, --help            show this help message and exit
    -host HOST            hostname or IP-Address of the ElasticSearch-node to use, default is localhost.
    -port PORT            Port of the ElasticSearch-node to use, default is 9200.
    -index INDEX          ElasticSearch Search Index to use
    -type TYPE            ElasticSearch Search Index Type to use
    -id ID                retrieve single document (optional)
    -searchserver SEARCHSERVER use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty
    -stdin                get data from stdin
    -pipeline             output every record (even if not enriched) to put this script into a pipeline

```


## Requirements

python3-elasticsearch

e.g. (ubuntu)
```
sudo apt-get install python3-elasticsearch
```
<a name="entityfacts-bot.py"/>

# wikidata.py 

wikidata.py is a Python3 program that enrichs ("links") your data with the wikidata-identifier from wikidata.  Prerequisits is that you have a field containing your GND-Identifier. Other identifiers are planned to be used in future.


It connects to an elasticsearch node and outputs the enriched data, which can be put back to the index using esbulk.

## Usage

```
./wikidata.py
    -h, --help      show this help message and exit
    -host HOST      hostname or IP-Address of the ElasticSearch-node to use, default is localhost.
    -port PORT      Port of the ElasticSearch-node to use, default is 9200.
    -index INDEX    ElasticSearch Search Index to use
    -type TYPE      ElasticSearch Search Index Type to use
    -id ID          retrieve single document (optional)
    -stdin          get data from stdin
    -pipeline       output every record (even if not enriched) to put this script into a pipeline
    -server SERVER  use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty
```

