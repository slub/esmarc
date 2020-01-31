#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Tool, to enrich elasticsearch data with existing GND-ID with data from
    [Entity Facts](https://hub.culturegraph.org/)

    Input:
        elasticsearch index OR
        STDIN (as jsonl)

    Output:
        on STDOUT
"""

import json
import argparse
import sys
import requests

from es2json import esgenerator
from es2json import isint
from es2json import litter
from es2json import eprint


def entityfacts(record, ef_instances):
    """ Function to harvest gnd entityfacts
    Look for connections to other entity providers in GND's
    entityfacts "sameAs" field


    :param record: json record probably containing GND entries
                   in their "sameAs" list field
    :type  record: json object

    :param ef_instances: entityfacts-URLs instances to query
    :type  ef_instances: list of strings

    :returns:
    :rtype:   json object
    """
    # abbreviations used by GND entityfacts and their
    # analoy in SLUB LOD context
    abbreviations = {
        "DNB": "https://data.slub-dresden.de/organizations/514366265",
        "VIAF": "https://data.slub-dresden.de/organizations/100092306",
        "LC": "https://data.slub-dresden.de/organizations/100822142",
        "DDB": "https://data.slub-dresden.de/organizations/824631854",
        "WIKIDATA": "https://www.wikidata.org/wiki/Q2013",
        "BNF": "https://data.slub-dresden.de/organizations/188898441",
        "KXP": "https://data.slub-dresden.de/organizations/103302212",
        "dewiki": None,
        "enwiki": None,
        "DE-611": "https://data.slub-dresden.de/organizations/103675612",
        "geonames": None,
        "ISNI": None,
        "filmportal.de": None,
        "ORCID": None,
        "Portraitindex": None,
        "ARCHIV-D": None,
        "DE-M512": None,
        "ADB": None,
        "NDB": None,
        "OEBL": "https://data.slub-dresden.de/organizations/102972389",
        "CH_HLS": None,
        "LAGIS": "https://data.slub-dresden.de/organizations/100482600",
        "WIKISOURCE": None,
        "DE-28": "https://data.slub-dresden.de/organizations/100874770",
        "OSTDEBIB": None,
        "PACELLI": None,
        "FFMPL": "https://data.slub-dresden.de/organizations/236770764",
        "epidat": "https://data.slub-dresden.de/organizations/103039031",
        "BIOKLASOZ": "https://data.slub-dresden.de/organizations/100832873",
        "HISTORICUMNET": "https://data.slub-dresden.de/organizations/102398704"
    }

    if not isinstance(record.get("sameAs"), list):
        return None

    for item in record.get("sameAs"):
        if "d-nb.info" in item["@id"] and len(item["@id"].split("/")) > 4:
            gnd_id = item["@id"].split("/")[-1]

    if not gnd_id:
        # no GND-ID - nothing to enrich
        return None

    old_rec_sameAs_len = len(str(record["sameAs"]))
    for url in ef_instances:
        r = requests.get(url + str(gnd_id))
        if r.ok:
            data = r.json()
        else:
            # WHAT TO DO HERE?
            eprint("entityfacts: could not request {}".
                   format(url + str(gnd_id)))
            continue

        sameAsses = []  # ba-dum-ts

        if data.get("_source"):
            # in Elasticsearch: data are in the "_source" field
            ef_sameAs = data.get("_source").get("sameAs")
        else:
            ef_sameAs = data.get("sameAs")

        if not ef_sameAs or not isinstance(ef_sameAs, list):
            continue

        for sameAs in ef_sameAs:
            id_ = sameAs.get("@id")

            # we can skip DNB-link as we already have it (and
            # used it to come here)
            if not id_ or id_.startswith("https://d-nb.info"):
                continue

            obj = {
                '@id': id_,
                'publisher': {
                    'abbr': sameAs["collection"]["abbr"],
                    'preferredName': sameAs["collection"]["name"]
                },
                'isBasedOn': {
                    '@type': "Dataset",
                    '@id': "http://hub.culturegraph.org/entityfacts/{}"
                           .format(gnd_id)
                }
            }
            # replace id with SLUB LOD id's listed in abbreviations
            if obj["publisher"]["abbr"] in abbreviations:
                slub_id = abbreviations[obj["publisher"]["abbr"]]
                if slub_id:
                    obj["publisher"]["@id"] = slub_id
            else:
                # unknown identifier, report into error log
                eprint("entityfacts: Abbr. {} not known [GND-ID: {}]"
                       .format(sameAs["collection"]["abbr"], gnd_id))
            sameAsses.append(obj)

        if sameAsses:
            record["sameAs"] = litter(record.get("sameAs"), sameAsses)
        break

    # compare length of transformed record, if the new entry is larger
    # than the old one, it was updated
    new_rec_sameAs_len = len(str(record["sameAs"]))
    if new_rec_sameAs_len > old_rec_sameAs_len:
        return record
    elif new_rec_sameAs_len < old_rec_sameAs_len:
        eprint("entityfacts: new record shorter than old one… "
               "[GND-ID: {}]".format(gnd_id))
        return None
    else:
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
                   help="use http://host:port/index/type. "
                        "Defines the Elasticsearch node and its index "
                        "for the input data")
    inputgroup.add_argument('-stdin', action="store_true",              # noqa
                   help="get data from stdin. Might be used with -pipeline.")
    p.add_argument('-pipeline', action="store_true",
                   help="output every record (even if not enriched) "
                        "to put this script into a pipeline")
    p.add_argument('-searchserver', type=str,
                   help="use http://host:port/index/type "
                        "to provide a local Elasticsearch instance "
                        "with entityfacts in the specified index")
    p.add_argument('-ignhub', action="store_true",
                   help="ignore hub.culturegraph.org. Here a local "
                        "searchserver must be provided.")
    return p


_p = _make_parser()

# extend docstring by argparse's help output
# → needed for the documentation to show command line parameters
__doc__ = _p.format_help()


def run():
    """
    :param args: argument object, which holds the configuration
    :type  args: argparse.Namespace

    :returns None
    :rtype   None

    """
    args = _p.parse_args()

    ef_instances = ["http://hub.culturegraph.org/entityfacts/"]

    if args.server:
        # overwrite args.host, args.port, args.index, [args.type]
        slashsplit = args.server.split("/")
        host = slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            port = args.server.split(":")[2].split("/")[0]
        index = args.server.split("/")[3]
        if len(slashsplit) > 4:
            type = slashsplit[4]

    if args.ignhub and args.searchserver:
        ef_instances = []

    if args.searchserver:
        slashsplit = args.searchserver.split("/")
        search_host = slashsplit[2].rsplit(":")[0]
        search_port = int(args.searchserver.split(":")[2].split("/")[0])
        search_index = args.searchserver.split("/")[3]
        if len(slashsplit) > 4:
            search_type = slashsplit[4] + "/"
        url = "http://{h}:{p}/{i}/{t}".format(
                h=search_host,
                p=search_port,
                i=search_index,
                t=search_type)
        # prepend searchserver to entityfacts instances to use local
        # search first
        ef_instances = [url] + ef_instances

    if args.stdin:
        iterate = sys.stdin
    else:
        # use Elasticsearch Server for iteration
        es_query = {"query": {
            "prefix": {"sameAs.@id.keyword": "https://d-nb.info"}
                             }
                    }
        iterate = esgenerator(host=host, port=port, index=index,
                              type=type, headless=True, body=es_query,
                              verbose=False)
    for rec_in in iterate:
        if args.stdin:
            rec_in = json.loads(rec_in)

        rec_out = entityfacts(rec_in, ef_instances)

        if rec_out:
            print(json.dumps(rec_out, indent=None))
        elif args.pipeline:
            print(json.dumps(rec_in, indent=None))


if __name__ == "__main__":
    run()
