#!/usr/bin/env python3

import argparse
import json
import sys
import requests
from es2json import esgenerator, isint, litter, eprint


lookup_table_wpSites = {"cswiki": {
                                    "@id": "https://cs.wikipedia.org",
                                    "preferredName": "Wikipedia (Tschechisch)",
                                    "abbr": "cswiki"
                                    },
                        "dewiki": {
                                    "abbr": "dewiki",
                                    "preferredName": "Wikipedia (Deutsch)",
                                    "@id": "http://de.wikipedia.org"
                                    },
                        "plwiki": {
                                    "abbr": "plwiki",
                                    "preferredName": "Wikipedia (Polnisch)",
                                    "@id": "http://pl.wikipedia.org"
                                    },
                        "enwiki": {
                                    "abbr": "enwiki",
                                    "preferredName": "Wikipedia (Englisch)",
                                    "@id": "http://en.wikipedia.org"
                                    },
                        }


def get_wptitle(record):
    """
    gets an list of sameAs Links which must contain an wikidata-item,
    adds the german, english, polish and czech wikipedia sites
    """
    wd_uri = None
    wd_id = None
    for _id in [x["@id"] for x in record["sameAs"]]:
        if "wikidata" in _id:
            wd_uri = _id
            wd_id = wd_uri.split("/")[-1]
            break
    if not wd_id:
        return None
    
    headers = {
            'User-Agent': 'efre-lod-enrich-wikipedia-bot/0.1 '
                          '(https://github.com/slub/esmarc) '
                          'python-requests/2.22'
            }
    
    wd_response = requests.get("https://www.wikidata.org/w/api.php",headers=headers, params={'action':'wbgetentities', 'ids': wd_id, 'props': 'sitelinks', 'format': 'json'})
    if not wd_response.ok:
        eprint("wikipedia: Connection Error {status}: \'{message}\'"
               .format(status=wd_response.status_code, wd_response=data.content)
               )
    sites = wd_response.json()["entities"][wd_id]["sitelinks"]
    abbrevs = list(x["publisher"]["abbr"] for x in record["sameAs"])
    changed = False
    for site, info in sites.items():
        if site in lookup_table_wpSites:
            newSameAs = {"@id": lookup_table_wpSites[site]["@id"]+"/wiki/{title}".format(title=info["title"]),
                         "publisher": lookup_table_wpSites[site],
                         "isBasedOn": {
                             "@type": "Dataset",
                             "@id": wd_uri
                             }
                         }
            if site not in abbrevs:
                record["sameAs"].append(newSameAs)
                changed = True
    if changed:
        return record


def run():
    parser = argparse.ArgumentParser(description='enrich ES by WP!')
    parser.add_argument('-host', type=str, default="127.0.0.1",
                        help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port', type=int, default=9200,
                        help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index', type=str,
                        help='ElasticSearch Search Index to use')
    parser.add_argument('-type', type=str,
                        help='ElasticSearch Search Index Type to use')
    parser.add_argument(
        "-id", type=str, help="retrieve single document (optional)")
    parser.add_argument('-stdin', action="store_true",
                        help="get data from stdin")
    parser.add_argument('-pipeline', action="store_true",
                        help="output every record (even if not enriched) to put this script into a pipeline")
    # no, i don't steal the syntax from esbulk...
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty")
    args = parser.parse_args()
    if args.server:
        slashsplit = args.server.split("/")
        args.host = slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port = args.server.split(":")[2].split("/")[0]
        args.index = args.server.split("/")[3]
        if len(slashsplit) > 4:
            args.type = slashsplit[4]
        if len(slashsplit) > 5 and "?pretty" in args.server:
            args.pretty = True
            args.id = slashsplit[5].rsplit("?")[0]
        elif len(slashsplit) > 5:
                args.id = slashsplit[5]

    if args.stdin:
        iterable = sys.stdin
    else:
        es_query = {"query": {"match": {"sameAs.publisher.abbr.keyword": "WIKIDATA"}}}
        iterable = esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, id=args.id, headless=True, body=es_query)

    for rec_in in iterable:
        if args.stdin:
            rec_in=json.loads(rec_in)

        rec_out = get_wptitle(rec_in)

        if rec_out:
            print(json.dumps(rec_out, indent=None))
        elif args.pipeline:
            print(json.dumps(rec_in, indent=None))


if __name__ == "__main__":
    run()
