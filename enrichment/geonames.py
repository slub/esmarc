#!/usr/bin/python3

import argparse
import json
import sys
import requests
import elasticsearch
from es2json import esgenerator
from es2json import isint
from es2json import litter
from es2json import eprint
from es2json import isfloat
from es2json import isiter



def get_gnid(rec):
    """
    Use geonames API (slow and quota limit for free accounts)
    """
    if not any("http://www.geonames.org" in s for s in rec.get("sameAs")) and rec["geo"].get("latitude") and rec["geo"].get("longitude"):
        changed = False
        r = requests.get("http://api.geonames.org/findNearbyJSON?lat="+rec["geo"].get(
            "latitude")+"&lng="+rec["geo"].get("longitude")+"&username=slublod")
        if r.ok and isiter(r.json().get("geonames")):
            for geoNameRecord in r.json().get("geonames"):
                if rec.get("name") in geoNameRecord.get("name") or geoNameRecord.get("name") in rec.get("name"):  # match!
                    newSameAs = {'@id': "https://sws.geonames.org/"+str(geoNameRecord.get("geonameId"))+"/",
                                 'publisher': {'abbr': "geonames",
                                               'preferredName': "GeoNames",
                                               "isBasedOn": {"@type": "Dataset",
                                                             "@id": "https://sws.geonames.org/"+str(record.get("id"))+"/"
                                                             }
                                               }
                                 }
                    rec["sameAs"] = litter(rec.get("sameAs"), newSameAs)
                    changed = True
        else:
            if r.json().get("status").get("message").startswith("the hourly limit") or r.json().get("status").get("message").startswith("the daily limit"):
                eprint("Limit exceeded!\n")
                exit(0)
        if changed:
            return rec


def get_gnid_by_es(rec, host, port, index, typ):
    """
    Use local dump in Elasticsearch
    """
    if not any("http://www.geonames.org" in s for s in rec.get("sameAs")) and rec["geo"].get("latitude") and rec["geo"].get("longitude"):
        changed = False
        records = []
        searchbody = {"query": {"bool": {"filter": {"geo_distance": {"distance": "0.1km", "location": {
            "lat": float(rec["geo"].get("latitude")), "lon": float(rec["geo"].get("longitude"))}}}}}}
        try:
            for record in esgenerator(headless=True, host=host, port=port, index=index, type=typ, body=searchbody):
                if record.get("name") in rec.get("preferredName") or rec.get("preferredName") in record.get("name") or len(records) == 1 or rec.get("preferredName") in record.get("alternateName"):
                    newSameAs = {'@id': "https://sws.geonames.org/"+str(record.get("id"))+"/",
                                 'publisher': {'abbr': "geonames",
                                               'preferredName': "GeoNames",
                                               "isBasedOn": {"@type": "Dataset",
                                                             "@id": "https://sws.geonames.org/"+str(record.get("id"))+"/"
                                                             }
                                               }
                                 }
                    rec["sameAs"] = litter(rec.get("sameAs"), newSameAs)
                    changed = True
        except elasticsearch.exceptions.RequestError as e:
            eprint(e, json.dumps(searchbody, indent=4),
                   json.dumps(rec, indent=4))
            return

        if changed:
            return rec
        else:
            return None


def run():
    parser = argparse.ArgumentParser(description='enrich ES by GN!')
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
    parser.add_argument('-searchserver', type=str, default="http://127.0.0.1:9200/geonames/record",
                        help="search instance to use. default is -server e.g. http://127.0.0.1:9200")  # index with geonames_data
    args = parser.parse_args()
    tabbing = None
    if args.server:
        slashsplit = args.server.split("/")
        args.host = slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port = args.server.split(":")[2].split("/")[0]
        args.index = args.server.split("/")[3]
        if len(slashsplit) > 4:
            args.type = slashsplit[4]
        if len(slashsplit) > 5:
            if "?pretty" in args.server:
                tabbing = 4
                args.id = slashsplit[5].rsplit("?")[0]
            else:
                args.id = slashsplit[5]
    if args.searchserver:
        slashsplit = args.searchserver.split("/")
        search_host = slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            search_port = args.searchserver.split(":")[2].split("/")[0]
        search_index = args.searchserver.split("/")[3]
        if len(slashsplit) > 4:
            search_type = slashsplit[4]

    if args.stdin:
        for line in sys.stdin:
            rec = json.loads(line)
            newrec = None
            if rec.get("geo") and not "geonames" in str(rec["sameAs"]):
                newrec = get_gnid_by_es(
                    rec, search_host, search_port, search_index, search_type)
                if newrec:
                    rec = newrec
            if args.pipeline or newrec:
                print(json.dumps(rec, indent=tabbing))
    else:
        for rec in esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, headless=True, body={"query": {"bool": {"filter": {"bool": {"must_not": [{"prefix": {"sameAs.@id.keyword": "https://sws.geonames.org"}}, {"prefix": {"sameAs.@id.keyword": "http://sws.geonames.org"}}, {"prefix": {"sameAs.@id.keyword": "https://www.geonames.org"}}, {"prefix": {"sameAs.@id.keyword": "http://www.geonames.org"}}]}}, "must": {"exists": {"field": "geo"}}}}}):
            # newrec=get_gnid(rec)
            newrec = get_gnid_by_es(
                rec, search_host, search_port, search_index, search_type)
            if newrec:
                rec = newrec
            if args.pipeline or newrec:
                print(json.dumps(rec, indent=tabbing))


if __name__ == "__main__":
    run()
