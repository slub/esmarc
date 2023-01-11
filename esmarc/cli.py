import sys
import argparse
import urllib
import json
import elasticsearch
from esmarc.esmarc import main

def parse_cli_args():
    """
    Argument Parsing for cli
    """
    parser = argparse.ArgumentParser(
        description='Entitysplitting/Recognition and RDF Mapping of MARC-Records')
    parser.add_argument(
        '-host', type=str, help='hostname or IP-Address and of the ElasticSearch-node to use.')
    parser.add_argument('-type', type=str, default="_doc", help='ElasticSearch Type to use')
    parser.add_argument('-index', type=str, help='ElasticSearch Index to use')
    parser.add_argument(
        '-id', type=str, help='map single document, given by id')
    parser.add_argument('-help', action="store_true", help="print this help")
    parser.add_argument('-z', action="store_true",
                        help="use gzip compression on output data")
    parser.add_argument('-prefix', type=str, default="ldj/",
                        help='Prefix to use for output data')
    parser.add_argument('-debug', action="store_true",
                        help='Dump processed Records to stdout (mostly used for debug-purposes)')
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id syntax. overwrites host:port/index/id")
    parser.add_argument('-w', type=int, default=8,
                        help="how many processes to use, too many could overload the elasticsearch")
    parser.add_argument('-idfile', type=str,
                        help="path to a file with IDs to process")
    parser.add_argument('-query', type=json.loads, default={},
                        help='prefilter the data based on an elasticsearch-query')
    parser.add_argument('-base_id_src', type=str, default="https://opac.k10plus.de/DB=2.299/PPNSET?PPN=",
                        help="set up which base_id to use for sameAs. e.g. https://d-nb.info/gnd/xxx")
    parser.add_argument('-target_id', type=str, default="https://data.slub-dresden.de/",
                        help="set up which target_id to use for @id. e.g. http://data.finc.info")
#    parser.add_argument('-lookup_host',type=str,help="Target or Lookup Elasticsearch-host, where the result data is going to be ingested to. Only used to lookup IDs (PPN) e.g. http://192.168.0.4:9200")
    args = parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    return args


def cli():
    """
    function for feeding the main-function with commandline-arguments when calling esmarc as standalone program from shell
    """
    args = parse_cli_args()
    es_kwargs = {}                              # dict to collect kwargs for ESgenerator
    host = None
    _type = None
    id = None
    if args.server:
        _parsed_url = urllib.parse.urlparse(args.server)
        host = "{}://{}".format(_parsed_url.scheme,_parsed_url.netloc)
        slashsplit = urllib.parse.urlparse(args.server).path.split("/")
        _index = slashsplit[1]
        if len(slashsplit) >= 3 and slashsplit[2]:
            _type = slashsplit[2]
        if len(slashsplit) >= 4:
            _type = slashsplit[2]
            id = slashsplit[3]
    else:
        host = args.host
        _index = args.index
        _type = args.type
    elastic = elasticsearch.Elasticsearch(host)
    main(_index=_index, _type=_type, _id=id, _base_id_src=args.base_id_src, debug=args.debug, _target_id=args.target_id, z=args.z, elastic=elastic, query=args.query, idfile=args.idfile, prefix=args.prefix)


if __name__ == "__main__":
    cli()
