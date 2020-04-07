#!/usr/bin/env python3
""" Tool, to enrich elasticsearch data with existing wikipedia attributes
    connected to a record by an (already existing) wikidata-ID

    Currently sites from the german, english, polish, and czech wikipedia are
    enrichted.

    Input:
        elasticsearch index OR
        STDIN (as jsonl)

    Output:
        on STDOUT
"""
import argparse
import json
import sys
import requests
import urllib
from es2json import esgenerator, isint, eprint, litter

lookup_table_wpSites = {
        "cswiki": {
                    "@id": "https://cs.wikipedia.org",
                    "preferredName": "Wikipedia (Tschechisch)",
                    "abbr": "cswiki"
                    },
        "dewiki": {
                    "abbr": "dewiki",
                    "preferredName": "Wikipedia (Deutsch)",
                    "@id": "https://de.wikipedia.org"
                    },
        "plwiki": {
                    "abbr": "plwiki",
                    "preferredName": "Wikipedia (Polnisch)",
                    "@id": "https://pl.wikipedia.org"
                    },
        "enwiki": {
                    "abbr": "enwiki",
                    "preferredName": "Wikipedia (Englisch)",
                    "@id": "https://en.wikipedia.org"
                    },
        }


def get_wptitle(record):
    """
    * iterates through all sameAs Links to extract a wikipedia-link
    * enriches wikipedia sites if they are within lookup_table_wpSites
      (i.e. currently german, english, polish, czech)

    returns: None (if record has not been changed)
             enriched record (dict, if record has changed)
    """
    wp_uri = None
    wp_title = None
    cc = None  # countrycode
    changed = False
    retobj = {}
    for _id in [x["@id"] for x in record["sameAs"]]:
        if "wikipedia" in _id:
            wp_uri = _id
            wp_title = urllib.parse.unquote(wp_uri.split("/")[-1])
            cc = wp_uri.split("/")[2].split(".")[0]


            headers = {
                    'User-Agent': 'lod-enrich-wikipedia-attributes-bot/0.1'
                                   '(https://github.com/slub/esmarc) '
                                   'python-requests/2.22'
                    }
            url = "https://{}.wikipedia.org/w/api.php".format(cc)
            wd_response = requests.get(url,
                                       headers=headers,
                                       params={'action': 'query',
                                               'cllimit': 500,
                                               'clshow': '!hidden',
                                               'titles': wp_title,
                                               'prop': 'categories',
                                               'format': 'json'})

            if not wd_response.ok:
                eprint("wikipedia: Connection Error {status}: \'{message}\'"
                        .format(status=wd_response.status_code,
                                message=wd_response.content)
                        )
                return None
            ## related wikipedia links:
            _base = "https://{}.wikipedia.org/wiki/".format(cc)
            try:
                pages = wd_response.json()["query"]["pages"]
                for page_id, page_data in pages.items():
                    for category in page_data["categories"]:
                        _id = _base + "{}".format(category["title"])
                        _name = category["title"].split(":")[1]
                        obj = {"id": _id, "name": _name}
                        retobj[cc] = litter(retobj.get(cc),obj)
                        changed = True
            except KeyError:
                eprint("wikipedia: Data Error for Record:")
                eprint("{record}\'\n\'{wp_record}\'".format(record=record,
                                            wp_record=wd_response.content))
                return None
            
    ## list of all abbreviations for publisher in record's sameAs
    #abbrevs = list(x["publisher"]["abbr"] for x in record["sameAs"])

    #changed = False
    #for wpAbbr, info in sites.items():
        #if wpAbbr in lookup_table_wpSites:
            #wikip_url = lookup_table_wpSites[wpAbbr]["@id"] + "/wiki/{title}"\
                        #.format(title=info["title"])
            #newSameAs = {"@id": wikip_url,
                         #"publisher": lookup_table_wpSites[wpAbbr],
                         #"isBasedOn": {
                             #"@type": "Dataset",
                             #"@id": wp_uri
                             #}
                         #}
            #if wpAbbr not in abbrevs:
                #record["sameAs"].append(newSameAs)
                #changed = True
            #if not record.get("name"):
                #record["name"] = {}
            #cc = wpAbbr[:2]  # countrycode
            #if cc not in record["name"]:
                #record["name"][cc] = [info["title"]]
                #changed = True
            #if info["title"] not in record["name"][cc]:
                #record["name"][cc] = litter(record["name"][cc], info["title"])
                #changed = True
    if changed:
        record["wp_categories"] = retobj
        return record
    return None

def _make_parser():
    """ Generates argument parser with all necessarry parameters.
    :returns script's arguments (host, port, index, type, id,
             searchserver, server, stdin, pipeline)
    :rtype   argparse.ArgumentParser
    """

    p = argparse.ArgumentParser(description=__doc__,
                formatter_class=argparse.RawDescriptionHelpFormatter)   # noqa
    inputgroup = p.add_mutually_exclusive_group(required=True)
    inputgroup.add_argument('-server', type=str,                        # noqa
                   help="use http://host:port/index/type[/id]. "
                        "Defines the Elasticsearch node and its index "
                        "for the input data. The last part of the path [id] "
                        "is optional and can be used for retrieving a single "
                        "document")
    inputgroup.add_argument('-stdin', action="store_true",              # noqa
                   help="get data from stdin. Might be used with -pipeline.")
    p.add_argument('-pipeline', action="store_true",
                   help="output every record (even if not enriched) "
                        "to put this script into a pipeline")
    return p


_p = _make_parser()

# extend docstring by argparse's help output
# → needed for the documentation to show command line parameters
__doc__ = _p.format_help()


def run():

    args = _p.parse_args()
    if args.server:
        srv = urllib.parse.urlparse(args.server)
        host = srv.hostname
        port = srv.port
        splitpath = srv.path.split("/")
        index = splitpath[1]
        doc_type = splitpath[2]
        if len(splitpath) > 3:
            doc_id = splitpath[3]
        else:
            doc_id = None
    if args.stdin:
        iterable = sys.stdin
    else:
        es_query = {
            "query": {
                "match": {"sameAs.publisher.abbr.keyword": "WIKIDATA"}
                }
            }
        iterable = esgenerator(host=host, port=port,
                               index=index, type=doc_type, id=doc_id,
                               headless=True, body=es_query)

    for rec_in in iterable:
        if args.stdin:
            rec_in = json.loads(rec_in)

        rec_out = get_wptitle(rec_in)

        if rec_out:
            print(json.dumps(rec_out, indent=None))
        elif args.pipeline:
            print(json.dumps(rec_in, indent=None))


if __name__ == "__main__":
    run()
