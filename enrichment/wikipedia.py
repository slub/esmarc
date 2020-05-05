#!/usr/bin/env python3
"""
Tool, to enrich elasticsearch data with existing wikipedia sites connected
to a record by an (already existing) wikidata-ID

Currently sites from the de, en, pl, and cz wikipedia are enrichted.

Can be configured to overwrite certain data sources to update obsolete/false links.

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
from es2json import esgenerator, eprint, litter

# list of data source which should be updated if we get a new wikipedia-link
obsolete_isBasedOns = ['hub.culturegraph.org']

# lookup table of which wikipedias to enrich
lookup_table_wpSites = {
        "cswiki": {
                    "abbr": "cswiki",
                    "preferredName": "Wikipedia (Tschechisch)"
                    },
        "dewiki": {
                    "abbr": "dewiki",
                    "preferredName": "Wikipedia (Deutsch)"
                    },
        "plwiki": {
                    "abbr": "plwiki",
                    "preferredName": "Wikipedia (Polnisch)"
                    },
        "enwiki": {
                    "abbr": "enwiki",
                    "preferredName": "Wikipedia (Englisch)"
                    },
        }


def build_abbrevs(sameAsses):
    """
    builds a little helper dictionary with the abbreviations
    of the current record, so we can check from which publisher
    each abbreviation is originating, along with the position
    in the records sameAs array, of course this helper dicitionary
    needs to get updated each time the sameAs array of the record
    gets changed, because the position value of the abbreviations
    get changed. position value is needed for deletions, because we
    want to delete entityfacts wikipedia entries when we get the
    wikipedia entries by wikidata
    :returns helper dictionary
    :rtype dict
    """
    abbrevs = {}
    for n, sameAs in enumerate(sameAsses):
        abbr_url = urllib.parse.urlparse(sameAs["isBasedOn"]["@id"])
        abbr_host = abbr_url.hostname
        abbrevs[sameAs["publisher"]["abbr"]] = {}
        abbrevs[sameAs["publisher"]["abbr"]]["host"] = abbr_host
        abbrevs[sameAs["publisher"]["abbr"]]["pos"] = n
    return abbrevs


def get_wpinfo(record):
    """
    * iterates through all sameAs Links to extract a wikidata-ID
    * requests wikipedia sites connected to the wd-Id
    * enriches wikipedia sites if they are within lookup_table_wpSites
      (i.e. currently german, english, polish, czech)
    * if we get an new wikipedia link from wikidata, but we
      already got an old entry from other as obsolete defined sources,
      we delete the obsolete entry and append the new entry
    * enriches multilingual names if they are within lookup_table_wpSites

    :returns None (if record has not been changed)
             enriched record (dict, if record has changed)
    :rtype dict
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
    site_filter_param = '|'.join([x for x in lookup_table_wpSites])
    wd_response = requests.get("https://www.wikidata.org/w/api.php",
                               headers=headers,
                               params={'action': 'wbgetentities',
                                       'ids': wd_id,
                                       'props': 'sitelinks/urls',
                                       'format': 'json',
                                       'sitefilter': site_filter_param})

    if not wd_response.ok:
        eprint("wikipedia: Connection Error {status}: \'{message}\'"
               .format(status=wd_response.status_code,
                       message=wd_response.content)
               )
        return None

    # related wikipedia links:
    try:
        sites = wd_response.json()["entities"][wd_id]["sitelinks"]
    except KeyError:
        eprint("wikipedia: Data Error for Record:")
        eprint("\'{record}\'\n\'{wp_record}\'"
               .format(record=record, wp_record=wd_response.content))
        return None

    # list of all abbreviations for publisher in record's sameAs
    abbrevs = build_abbrevs(record["sameAs"])
    changed = False
    for wpAbbr, info in sites.items():
        if wpAbbr in lookup_table_wpSites:
            wikip_url = info["url"]
            newSameAs = {"@id": wikip_url,
                         "publisher": lookup_table_wpSites[wpAbbr],
                         "isBasedOn": {
                             "@type": "Dataset",
                             "@id": wd_uri
                             }
                         }
            # wikipedia sameAs link enrichment
            if wpAbbr not in abbrevs:
                record["sameAs"].append(newSameAs)
                changed = True
                abbrevs = build_abbrevs(record["sameAs"])
            elif abbrevs.get(wpAbbr) and abbrevs[wpAbbr]["host"] in obsolete_isBasedOns:
                del record["sameAs"][abbrevs[wpAbbr]["pos"]]
                record["sameAs"].append(newSameAs)
                abbrevs = build_abbrevs(record["sameAs"])
                changed = True

            # multilingual name object enrichment
            if not record.get("name"):
                record["name"] = {}
            cc = wpAbbr[:2]  # countrycode
            if cc not in record["name"]:
                record["name"][cc] = [info["title"]]
                changed = True
            if info["title"] not in record["name"][cc]:
                record["name"][cc] = litter(record["name"][cc], info["title"])
                changed = True
    if changed:
        return record


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

        rec_out = get_wpinfo(rec_in)

        if rec_out:
            print(json.dumps(rec_out, indent=None))
        elif args.pipeline:
            print(json.dumps(rec_in, indent=None))


if __name__ == "__main__":
    run()
