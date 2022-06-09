#!/usr/bin/python3
# -*- coding: utf-8 -*-
from rdflib import URIRef
import traceback
from multiprocessing import Pool, current_process
import elasticsearch
import json
import argparse
import sys
import copy
import os.path
import re
import gzip
import datetime
import dateparser
import urllib
from es2json import ESGenerator, IDFile, ArrayOrSingleValue, eprint, eprintjs, litter, isint
from esmarc.swb_fix import marc2relation, map_entities, map_types, lookup_coll, lookup_ssg_fid, lookup_sameAs, footnotes_lookups

entities = None
base_id = None
target_id = None


def parse_cli_args():
    """
    Argument Parsing for cli
    """
    parser = argparse.ArgumentParser(
        description='Entitysplitting/Recognition of MARC-Records')
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


def main(elastic=None,
         _index="",
         _type="_doc",
         _id=None,
         z=False,
         prefix="ldj/",
         debug=False,
         w=8,
         idfile=None,
         query={},
         _base_id_src="http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
         _target_id="https://data.slub-dresden.de/"):
    """
    main function which can be called by other programs
    """
    global base_id
    global target_id
    base_id = _base_id_src
    target_id = _target_id
    if elastic and _index and (_id or debug):
        init_mp(prefix, z)
        with ESGenerator(es=elastic, index=_index, type_=_type, includes=get_source_include_str(), body=query, id_=_id, headless=True) as es2json_obj:
            for ldj in es2json_obj.generator():
                record = process_line(ldj, _index)
                if record:
                    for k in record:
                        print(json.dumps(record[k]))
    elif elastic and _index and not _id:
        setupoutput(prefix)
        pool = Pool(w, initializer=init_mp, initargs=(prefix, z))
        if idfile:
            es2json_obj = IDFile(es=elastic, index=_index, type_=_type, includes=get_source_include_str(), body=query, idfile=idfile)
        else:
            es2json_obj = ESGenerator(es=elastic, index=_index, type_=_type, includes=get_source_include_str(), body=query)
        for ldj in es2json_obj.generator():
            pool.apply_async(worker, args=(ldj, _index,))
        pool.close()
        pool.join()
    else:  # oh noes, no elasticsearch input-setup. exiting.
        eprint("No -host:port/-index or -server specified, exiting\n")
        exit(-1)


def uniq(lst):
    """
    return lst only with unique elements in it
    """
    last = object()
    for item in lst:
        if item == last:
            continue
        yield item
        last = item


def handlesex(record, key, entity):
    """
    return the determined sex (not gender), found in the MARC21 record
    """
    for v in key:
        marcvalue = getmarc(v, record, entity)
        if isinstance(marcvalue, list):
            marcvalue = marcvalue[0]
    if isint(marcvalue):
        marcvalue = int(marcvalue)
    if marcvalue == 0:
        return "Unknown"
    elif marcvalue == 1:
        return "Male"
    elif marcvalue == 2:
        return "Female"
    else:
        return None


def gnd2uri(string):
    """
    Transforms e.g. (DE-588)1231111151 to an URI .../1231111151
    """
    try:
        if isinstance(string, list):
            for n, uri in enumerate(string):
                string[n] = gnd2uri(uri)
            return string
        if string and "(DE-" in string:
            if isinstance(string, list):
                ret = []
                for st in string:
                    ret.append(gnd2uri(st))
                return ret
            elif isinstance(string, str):   # added .upper
                return uri2url("({})".format(string.split(')')[0][1:]), string.split(')')[1].upper())
    except:
        return


def uri2url(isil, num):
    """
    Transforms e.g. .../1231111151 to https://d-nb.info/gnd/1231111151,
    not only GNDs, also SWB, GBV, configureable over lookup_sameAs lookup table
    in swb_fix.py
    """
    if isil == "(DE-576)":
        return None
    if isil and num and isil in lookup_sameAs:
        return "{}{}".format(lookup_sameAs[isil]["@id"],num)


def id2uri(string, entity):
    """
    return an id based on base_id
    """
    global target_id
    if string.startswith(base_id):
        string = string.split(base_id)[1]
    # if entity=="resources":
    #    return "http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN="+string
    # else:
    if target_id and entity and string:
        return str(target_id+entity+"/"+string)


def getid(record, regex, entity):
    """
    wrapper function for schema.org/identifier mapping for id2uri
    """
    _id = getmarc(record, regex, entity)
    if _id:
        return id2uri(_id, entity)


def getisil(record, regex, entity):
    """
    get the ISIL of the record
    """
    isil = getmarc(record, regex, entity)
    if isinstance(isil, str) and "({})".format(isil) in lookup_sameAs:
        return isil
    elif isinstance(isil, list):
        for item in isil:
            if "({})".format(item) in lookup_sameAs:
                return item


def getnumberofpages(record, regex, entity):
    """
    get the number of pages and sanitizes the field into an atomar integer
    """
    nop = getmarc(record, regex, entity)
    try:
        if isinstance(nop, str):
            nop = [nop]
        if isinstance(nop, list):
            for number in nop:
                if "S." in number and isint(number.split('S.')[0].strip()):
                    nop = int(number.split('S.')[0])
                else:
                    nop = None
    except IndexError:
        pass
    except Exception as e:
        with open("error.txt", "a") as err:
            print(e, file=err)
    return nop


def getgenre(record, regex, entity):
    """
    gets the genre and builds a schema.org/genre node out of it
    """
    genre = getmarc(record, regex, entity)
    if genre:
        return {"@type": "Text",
                "Text": genre}


def getisbn(record, regex, entity):
    """
    gets the ISBN and sanitizes it
    """
    isbns = getmarc(record, regex, entity)
    if isinstance(isbns, str):
        isbns = [isbns]
    elif isinstance(isbns, list):
        for i, isbn in enumerate(isbns):
            if "-" in isbn:
                isbns[i] = isbn.replace("-", "")
            if " " in isbn:
                for part in isbn.rsplit(" "):
                    if isint(part):
                        isbns[i] = part
    if isbns:
        retarray = []
        for isbn in isbns:
            if len(isbn) == 10 or len(isbn) == 13:
                retarray.append(isbn)
        return retarray


def getmarc(record, regex, entity):
    """
    gets the in regex specified attribute from a Marc Record
    """
    if "+" in regex:
        marcfield = regex[:3]
        if marcfield in record:
            subfields = regex.split(".")[-1].split("+")
            data = None
            for array in record.get(marcfield):
                for k, v in array.items():
                    sset = {}
                    for subfield in v:
                        for subfield_code in subfield:
                            sset[subfield_code] = litter(
                                sset.get(subfield_code), subfield[subfield_code])
                    fullstr = ""
                    for sf in subfields:
                        if sf in sset:
                            if fullstr:
                                fullstr += ". "
                            if isinstance(sset[sf], str):
                                fullstr += sset[sf]
                            elif isinstance(sset[sf], list):
                                fullstr += ". ".join(sset[sf])
                    if fullstr:
                        data = litter(data, fullstr)
            if data:
                return ArrayOrSingleValue(data)
    else:
        ret = []
        if isinstance(regex, str):
            regex = [regex]
        for string in regex:
            if string[:3] in record:
                ret = litter(ret, ArrayOrSingleValue(
                    list(getmarcvalues(record, string, entity))))
        if ret:
            if isinstance(ret, list):  # simple deduplizierung via uniq()
                ret = list(uniq(ret))
            return ArrayOrSingleValue(ret)


def getmarcvalues(record, regex, entity):
    """
    generator object for getmarc(), using a hardcoded algorithm
    """
    if len(regex) == 3 and regex in record:
        yield record.get(regex)
    else:
        record = record.get(regex[:3])
        """
        beware! hardcoded traverse algorithm for marcXchange record encoded data !!!
        temporary workaround: http://www.smart-jokes.org/programmers-say-vs-what-they-mean.html
        """
        # = [{'__': [{'a': 'g'}, {'b': 'n'}, {'c': 'i'}, {'q': 'f'}]}]
        if isinstance(record, list):
            for elem in record:
                if isinstance(elem, dict):
                    for k in elem:
                        if isinstance(elem[k], list):
                            for final in elem[k]:
                                if regex[-1] in final:
                                    yield final.get(regex[-1])


def handle_about(jline, key, entity):
    """
    produces schema.org/about nodes based on RVK, DDC and GND subjects
    """
    ret = []
    for k in key:
        if k == "936" or k == "084":
            data = getmarc(jline, k, None)
            if isinstance(data, list):
                for elem in data:
                    ret.append(handle_single_rvk(elem))
            elif isinstance(data, dict):
                ret.append(handle_single_rvk(data))
        elif k == "082" or k == "083":
            data = getmarc(jline, k+"..a", None)
            if isinstance(data, list):
                for elem in data:
                    if isinstance(elem, str):
                        ret.append(handle_single_ddc(elem))
                    elif isinstance(elem, list):
                        for final_ddc in elem:
                            ret.append(handle_single_ddc(final_ddc))
            elif isinstance(data, dict):
                ret.append(handle_single_ddc(data))
            elif isinstance(data, str):
                ret.append(handle_single_ddc(data))
        elif k == "655":
            data = get_subfield(jline, k, entity)
            ret.append(data)
    if len(ret) > 0:
        return ret
    else:
        return None


def handle_single_ddc(data):
    """
    produces a about node based on DDC
    """
    return {"identifier": {"@type": "PropertyValue",
                           "propertyID": "DDC",
                           "value": data},
            "@id": "http://purl.org/NET/decimalised#c"+data[:3]}


def handle_single_rvk(data):
    """
    produces a about node based on RVK
    """
    sset = {}
    record = {}
    if "rv" in data:
        for subfield in data.get("rv"):
            for k, v in subfield.items():
                sset[k] = litter(sset.get(k), v)
        if "0" in sset and isinstance(sset["0"], str):
            sset["0"] = [sset.get("0")]
        if "0" in sset and isinstance(sset["0"], list):
            record["sameAs"] = []
            for elem in sset["0"]:
                if isinstance(elem, str):
                    sameAs = gnd2uri(elem)
                    if sameAs:
                        record["sameAs"].append(sameAs)
        if "a" in sset:
            record["@id"] = "https://rvk.uni-regensburg.de/api/json/ancestors/" + \
                sset.get("a").replace(" ", "%20")
            record["identifier"] = {"@type": "PropertyValue",
                                    "propertyID": "RVK",
                                    "value": sset.get("a")}
        if "b" in sset:
            record["name"] = sset.get("b")
        if "k" in sset:
            record["keywords"] = sset.get("k")
        return record


def relatedTo(jline, key, entity):
    """
    produces some relatedTo and other nodes based on GND-MARC21-Relator Codes
    """
    # e.g. split "551^4:orta" to 551 and orta
    marcfield = key[:3]
    data = []
    entityType = "persons"
    if marcfield in jline:
        for array in jline[marcfield]:
            for k, v in array.items():
                sset = {}
                node = {}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code] = litter(
                            sset.get(subfield_code), subfield[subfield_code])
                if sset.get("a") and not sset.get("t"):
                    node["name"] = sset.get("a")
                elif sset.get("a") and sset.get("t"):
                    node["name"] = sset.get("t")
                    node["author"] = sset.get("a")
                    entityType = "works"
                elif sset.get("t"):
                    node["name"] = sset.get("t")
                    entityType = "works"
                if isinstance(sset.get("9"), str) and sset.get("9") in marc2relation:
                    node["_key"] = marc2relation[sset["9"]]
                    if sset.get("0"):
                        uri = gnd2uri(sset.get("0"))
                        if isinstance(uri, str) and uri.startswith(base_id):
                            node["@id"] = id2uri(sset.get("0"), entityType)
                        elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"] = uri
                        elif isinstance(uri, str):
                            node["identifier"] = sset.get("0")
                        elif isinstance(uri, list):
                            node["sameAs"] = None
                            node["identifier"] = None
                            for elem in uri:
                                if elem and isinstance(elem, str) and elem.startswith(base_id):
                                    node["@id"] = id2uri(
                                        elem.split("=")[-1], entityType)
                                elif elem and isinstance(elem, str) and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"] = litter(
                                        node["sameAs"], elem)
                                else:
                                    node["identifier"] = litter(
                                        node["identifier"], elem)
                    data.append(node)
                elif isinstance(sset.get("9"), list):
                    for elem in sset["9"]:
                        if elem.startswith("v"):
                            for k, v in marc2relation.items():
                                if k.lower() in elem.lower():
                                    node["_key"] = v
                                    break
                        elif [x for x in marc2relation if x.lower() in elem.lower()]:
                            for x in marc2relation:
                                if x.lower() in elem.lower():
                                    node["_key"] = marc2relation[x]
                        elif not node.get("_key"):
                            node["_key"] = "relatedTo"
                        # eprint(elem,node)
                    if sset.get("0"):
                        uri = gnd2uri(sset.get("0"))
                        if isinstance(uri, str) and uri.startswith(base_id):
                            node["@id"] = id2uri(sset.get("0"), entityType)
                        elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"] = uri
                        elif isinstance(uri, str):
                            node["identifier"] = uri
                        elif isinstance(uri, list):
                            node["sameAs"] = None
                            node["identifier"] = None
                            for elem in uri:
                                if elem and elem.startswith(base_id):
                                    node["@id"] = id2uri(
                                        elem.split("=")[-1], entityType)
                                elif elem and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"] = litter(
                                        node["sameAs"], elem)
                                elif elem:
                                    node["identifier"] = litter(
                                        node["identifier"], elem)
                    data.append(node)

        if data:
            return ArrayOrSingleValue(data)


def get_subfield_if_4(jline, key, entity):
    """
    gets subfield of marc-Records and builds some nodes out of them if a clause is statisfied
    """
    # e.g. split "551^4:orta" to 551 and orta
    marcfield = key.rsplit("^")[0]
    subfield4 = key.rsplit("^")[1]
    data = []
    if marcfield in jline:
        for array in jline[marcfield]:
            for k, v in array.items():
                sset = {}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code] = litter(
                            sset.get(subfield_code), subfield[subfield_code])
                if sset.get("4") and subfield4 in sset.get("4"):
                    newrecord = copy.deepcopy(jline)
                    for i, subtype in enumerate(newrecord[marcfield]):
                        for elem in subtype.get("__"):
                            if elem.get("4") and subfield4 != elem["4"]:
                                del newrecord[marcfield][i]["__"]
                    data = litter(get_subfields(
                        newrecord, marcfield, entity), data)
    if data:
        return ArrayOrSingleValue(data)


def get_subfields(jline, key, entity):
    """
    wrapper-function for get_subfield for multi value:
    reads some subfield information and builds some nodes out of them, needs an entity mapping to work
    """
    data = []
    if isinstance(key, list):
        for k in key:
            data = litter(data, get_subfield(jline, k, entity))
        return ArrayOrSingleValue(data)
    elif isinstance(key, str):
        return get_subfield(jline, key, entity)
    else:
        return


def handleHasPart(jline, keys, entity):
    """
    adds the hasPart node if a record contains a pointer to another record, which is part of the original record
    """
    data = []
    for key in keys:
        if key == "700" and key in jline:
            for array in jline[key]:
                for k, v in array.items():
                    sset = {}
                    for subfield in v:
                        for subfield_code in subfield:
                            sset[subfield_code] = litter(
                                sset.get(subfield_code), subfield[subfield_code])
                    node = {}
                    if sset.get("t"):
                        entityType = "works"
                        node["name"] = sset["t"]
                        if sset.get("a"):
                            node["author"] = sset["a"]
                        if entityType == "resources" and sset.get("w") and not sset.get("0"):
                            sset["0"] = sset.get("w")
                        if sset.get("0"):
                            if isinstance(sset["0"], list) and entityType == "persons":
                                for n, elem in enumerate(sset["0"]):
                                    if elem and "DE-576" in elem:
                                        sset["0"].pop(n)
                            uri = gnd2uri(sset.get("0"))
                            if isinstance(uri, str) and uri.startswith(base_id) and not entityType == "resources":
                                node["@id"] = id2uri(uri, entityType)
                            elif isinstance(uri, str) and uri.startswith(base_id) and entityType == "resources":
                                node["sameAs"] = base_id + \
                                    id2uri(uri, entityType).split("/")[-1]
                            elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                                node["sameAs"] = uri
                            elif isinstance(uri, str):
                                node["identifier"] = uri
                            elif isinstance(uri, list):
                                node["sameAs"] = None
                                node["identifier"] = None
                                for elem in uri:
                                    if isinstance(elem, str) and elem.startswith(base_id):
                                        # if key=="830":  #Dirty Workaround for finc id
                                            # rsplit=elem.rsplit("=")
                                            # rsplit[-1]="0-"+rsplit[-1]
                                            # elem='='.join(rsplit)
                                        node["@id"] = id2uri(elem, entityType)
                                    elif isinstance(elem, str) and elem.startswith("http") and not elem.startswith(base_id):
                                        node["sameAs"] = litter(
                                            node["sameAs"], elem)
                                    elif elem:
                                        node["identifier"] = litter(
                                            node["identifier"], elem)

                    if node:
                        data = litter(data, node)
                        # data.append(node)
        else:
            node = getmarc(jline, key, entity)
            if node:
                data = litter(data, node)
    if data:
        return ArrayOrSingleValue(data)


def get_subfield(jline, key, entity):
    """
    reads some subfield information and builds some nodes out of them, needs an entity mapping to work
    """
    keymap = {"100": "persons",
              "700": "persons",
              "500": "persons",
              "711": "events",
              "110": "swb",
              "710": "swb",
              "551": "geo",
              "689": "topics",
              "550": "topics",
              "551": "geo",
              "655": "topics",
              "830": "resources"
              }
    entityType = keymap.get(key)
    data = []
    if key in jline:
        for array in jline[key]:
            for k, v in array.items():
                sset = {}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code] = litter(
                            sset.get(subfield_code), subfield[subfield_code])
                node = {}
                if sset.get("t"):  # if this field got an t, then its a Werktiteldaten, we use this field in another function then
                    continue
                for typ in ["D", "d"]:
                    if isinstance(sset.get(typ), str):  # http://www.dnb.de/SharedDocs/Downloads/DE/DNB/wir/marc21VereinbarungDatentauschTeil1.pdf?__blob=publicationFile Seite 14
                        node["@type"] = "http://schema.org/"
                        if sset.get(typ) in map_entities and sset.get(typ) in map_types:
                            node["@type"] += map_types.get(sset[typ])
                            entityType = map_entities.get(sset[typ])
                        else:
                            node.pop("@type")
                if entityType == "resources":
                    if sset.get("w") and not sset.get("0"):
                        sset["0"] = sset.get("w")
                    if sset.get("v"):
                        node["position"] = sset["v"]
                if sset.get("0"):
                    if isinstance(sset["0"], list) and entityType == "persons":
                        for n, elem in enumerate(sset["0"]):
                            if elem and "DE-576" in elem:
                                sset["0"].pop(n)
                    uri = gnd2uri(sset.get("0"))
                    if isinstance(uri, str) and uri.startswith(base_id) and entityType != "resources":
                        node["@id"] = id2uri(uri, entityType)
                    elif isinstance(uri, str) and uri.startswith(base_id) and entityType == "resources":
                        node["sameAs"] = base_id + \
                            id2uri(uri, entityType).split("/")[-1]
                    elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                        node["sameAs"] = uri
                    elif isinstance(uri, str):
                        node["identifier"] = uri
                    elif isinstance(uri, list):
                        node["sameAs"] = None
                        node["identifier"] = None
                        for elem in uri:
                            if isinstance(elem, str) and elem.startswith(base_id):
                                # if key=="830":  #Dirty Workaround for finc id
                                    # rsplit=elem.rsplit("=")
                                    # rsplit[-1]="0-"+rsplit[-1]
                                    # elem='='.join(rsplit)
                                node["@id"] = id2uri(elem, entityType)
                            elif isinstance(elem, str) and elem.startswith("http") and not elem.startswith(base_id):
                                node["sameAs"] = litter(node["sameAs"], elem)
                            elif elem:
                                node["identifier"] = litter(
                                    node["identifier"], elem)
                if isinstance(sset.get("a"), str) and len(sset.get("a")) > 1:
                    node["name"] = sset.get("a")
                elif isinstance(sset.get("a"), list):
                    for elem in sset.get("a"):
                        if len(elem) > 1:
                            node["name"] = litter(node.get("name"), elem)
                if sset.get("i"):
                    node["description"] = sset["i"]
                if sset.get("n") and entityType == "events":
                    node["position"] = sset["n"]
                if node:
                    data = litter(data, node)
        if data:
            return ArrayOrSingleValue(data)


def getsameAs(jline, keys, entity):
    """
    produces schema.org/sameAs node out of the MARC21-Record
    for KXP, DNB, RISM and others.
    """
    sameAs = []
    raw_data = set()
    data = []
    for key in keys:
        if key == "016":  # 016 has ISIL in 016$2 and ID in 016$a.
            marc_data = getmarc(jline, key, entity)
            if isinstance(marc_data,list):
                for indicator_level in marc_data:
                    for _ind in indicator_level:
                        sset = {}
                        for subfield_dict in indicator_level[_ind]:
                            for k,v in subfield_dict.items():
                                sset[k] = v
                    if sset.get("a") and sset.get("2"):
                        data = litter(data, "({}){}".format(sset["2"], sset["a"]))
        elif key == "035..a":  # 035$a has already both in $a, so we're fine
            data = litter(data, getmarc(jline, key, entity))
    if isinstance(data, str):
        data = [data]
    if isinstance(data, list):
        for elem in data:
            if elem[0:8] in lookup_sameAs:
                data = gnd2uri(elem)
                newSameAs = dict(lookup_sameAs[elem[0:8]])
                newSameAs["@id"] = data
                newSameAs["isBasedOn"] = {"@type": "Dataset", "@id": ""}
                sameAs.append(newSameAs)
    return sameAs


def handle_identifier(jline, key, entity):
    ids = []
    data = getmarc(jline, key, entity)
    for _id in data:
        id_obj = {"@type": "PropertyValue"}
        id_obj["propertyID"] = _id[1:7]
        id_obj["value"] = _id[8:]
        if "DE-627" in id_obj["propertyID"]:
            id_obj["name"] = "K10Plus-ID"
            ids.append(id_obj)
        elif "DE-576" in id_obj["propertyID"]:
            id_obj["name"] = "SWB-ID"
            ids.append(id_obj)
    return ids


def startDate(jline, key, entity):
    """
    calls marc_dates with the correct key (start) for a date-mapping
    produces an date-Object for the startDate-field
    """
    extra_key = ""
    if "^" in key:
        key_split = key.split("^")
        key = key_split[0]
        if "," in key_split[1]:
            extra_key = key_split[1].split(",")
        else:
            extra_key = key_split[1]
    return marc_dates(jline.get(key), entity, "startDate", extra_key)


def endDate(jline, key, entity):
    """
    calls marc_dates with the correct key (end) for a date-mapping
    produces an date-object for the endDate field

    """
    datekey_list = ""
    if "^" in key:
        key_split = key.split("^")
        key = key_split[0]
        if "," in key_split[1]:
            datekey_list = key_split[1].split(",")
        else:
            datekey_list = key_split[1]
    return marc_dates(jline.get(key), entity, "endDate", datekey_list)


def marc_dates(record, entity, event, datekey_list):
    """
    builds the date nodes based on the data which is sanitzed by dateToEvent, gets called by the deathDate/birthDate functions
    """
    dates = []
    if record:
        for indicator_level in record:
            for subfield in indicator_level:
                sset = {}
                for sf_elem in indicator_level.get(subfield):
                    for k, v in sf_elem.items():
                        if k == "a" or k == "4" or k == 'i':
                            sset[k] = litter(sset.get(k), ArrayOrSingleValue(v))
                if '4' in sset and sset['4'] in datekey_list:
                    dates.append(sset)
    if dates:
        exact_date_index = 0
        for n, date in enumerate(dates):
            if "exakt" in date['i'].lower():
                exact_date_index = n
            else:
                exact_date_index = 0
        if dates and dates[exact_date_index]['4'] in datekey_list:
            ret = {"@value": dateToEvent(dates[exact_date_index]['a'], event), "disambiguatingDescription": dates[exact_date_index]['i'], "description": dates[exact_date_index]['a']}
            if ret.get("@value"):
                return ret
            elif ret.get("description"):
                ret.pop("@value")
                if "-" in ret["description"]:
                    if event == "startDate" and ret["description"].split("-")[0]:
                        return ret
                    elif event == "endDate" and ret["description"].split("-")[1]:
                        return ret
    return None


def dateToEvent(date, schemakey):
    """
    return birthDate and deathDate schema.org attributes
    don't return deathDate if the person is still alive according to the data
    (determined if e.g. the date looks like "1979-")
    """
    date = ArrayOrSingleValue(date)
    if not date:
        return None
    if isinstance(date, list):
        ret = []
        for item in date:
            dateItem = dateToEvent(item, schemakey)
            if dateItem:
                ret.append(dateItem)
    if "[" in date and "]" in date:
        date = date.split("[")[1]
        date = date.split("]")[0]
    ddp = dateparser.date.DateDataParser()
    parsedDate = None
    strf_string = None
    ddp_obj = None
    if '-' in date:
        dates = date.split('-')
        if schemakey == "startDate":  # (start date)
            ddp_obj = ddp.get_date_data(dates[0])
            parsedDate = ddp_obj.date_obj
        elif schemakey == "endDate":  # (end Date)
            if len(dates) == 2 and dates[1]:
                ddp_obj = ddp.get_date_data(dates[1])
                parsedDate = ddp_obj.date_obj
            elif len(dates) == 1:
                return None  # still alive! congrats
    else:
        date = date.lower()
        ddp_obj = ddp.get_date_data(date)
        parsedDate = ddp_obj.date_obj
        # check if its not a date from the future and if the year has four digits
    if parsedDate and int(parsedDate.strftime("%Y")) < int(datetime.datetime.today().strftime("%Y")) and len(parsedDate.strftime("%Y")) == 4:
        strf_string = None
        if ddp_obj.period == "year":
            strf_string = "%Y"
        elif ddp_obj.period == "month":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "day":
            strf_string = "%Y-%m-%d"
        elif ddp_obj.period == "week":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "time":
            strf_string = "%Y-%m-%d"
        return parsedDate.strftime(strf_string)


def datePublished(jline, key, entity):
    """
    reads different MARC21 Fields to determine when the entity behind this record got published
    """
    fivethreethree = getmarc(jline, "533.__.d", entity)
    twosixfour = getmarc(jline, "264.*.c", entity)
    fivethreefour = getmarc(jline, "534.__.c", entity)
    zerozeroeight = getmarc(jline, "008", entity)
    if fivethreethree:
        return handle_260(fivethreethree)
    elif not fivethreethree and twosixfour:
        return handle_260(twosixfour)
    if not fivethreethree and not twosixfour and fivethreefour:
        return handle_260(zerozeroeight[7:11])


def dateOriginalPublished(jline, key, entity):
    """
    reads different MARC21 Fields to determine when the entity behind this record got published originally
    """
    fivethreethree = getmarc(jline, "533.__.d", entity)
    twosixfour = getmarc(jline, "264.*.c", entity)
    fivethreefour = getmarc(jline, "534.__.c", entity)
    if fivethreethree:
        return handle_260(twosixfour)
    if fivethreefour:
        return handle_260(fivethreefour)


def parseDate(toParsedDate):
    """
    use scrapehubs dateParser to get an Python dateobject out of pure MARC21-Rubbish
    """
    if isinstance(toParsedDate, list):
        toParsedDate = toParsedDate[0]
    if "[" in toParsedDate and "]" in toParsedDate:
        toParsedDate = toParsedDate.split("[")[1]
        toParsedDate = toParsedDate.split("]")[0]
    ddp = dateparser.date.DateDataParser()
    ddp_obj = ddp.get_date_data(toParsedDate.lower())
    parsedDate = ddp_obj.date_obj
    if parsedDate and int(parsedDate.strftime("%Y")) < int(datetime.datetime.today().strftime("%Y")) and len(parsedDate.strftime("%Y")) == 4:
        strf_string = None
        if ddp_obj.period == "year":
            strf_string = "%Y"
        elif ddp_obj.period == "month":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "day":
            strf_string = "%Y-%m-%d"
        elif ddp_obj.period == "week":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "time":
            strf_string = "%Y-%m-%d"
        return parsedDate.strftime(strf_string)


def handle_260(date):
    """
    parse the 264/260 field to a machine-readable format
    """
    if isinstance(date, list):
        ret = []
        for item in date:
            dateItem = handle_260(item)
            if dateItem:
                ret.append(dateItem)
        return ArrayOrSingleValue(ret)
    if not date:
        return None
    retObj = {"dateOrigin": date}
    if "-" in date:
        dateSplitField = date.split("-")
        if dateSplitField[0]:
            dateParsedEarliest = parseDate(dateSplitField[0])
            if dateParsedEarliest:
                retObj["dateParsedEarliest"] = dateParsedEarliest
        if dateSplitField[1]:
            dateParsedLatest = parseDate(dateSplitField[1])
            if dateParsedLatest:
                retObj["dateParsedLatest"] = dateParsedLatest
    else:
        parsedDate = parseDate(date)
        if parsedDate:
            retObj["dateParsed"] = parsedDate
    return retObj if retObj["dateOrigin"] else None


def getgeo(arr):
    """
    sanitzes geo-coordinate information from raw MARC21 values
    """
    for k, v in traverse(arr, ""):
        if isinstance(v, str):
            if '.' in v:
                return v


def getGeoCoordinates(record, key, entity):
    """
    get the geographic coordinates of an entity from the corresponding MARC21 authority Record
    """
    ret = {}
    for k, v in key.items():
        coord = getgeo(getmarc(record, v, entity))
        if coord:
            ret["@type"] = "GeoCoordinates"
            ret[k] = coord.replace("N", "").replace(
                "S", "-").replace("E", "").replace("W", "-")
    if ret:
        return ret


def getav_katalog(record, key, entity):
    """
    produce a link to katatalog.slub-dresden.de for availability information
    """
    retOffers = list()
    swb_ppn = getmarc(record, key[1], entity)
    branchCode = getmarc(record, key[0], entity)
    # eprint(branchCode,finc_id)
    if swb_ppn and isinstance(branchCode, str) and branchCode == "DE-14":
        branchCode = [branchCode]
    if swb_ppn and isinstance(branchCode, list):
        for bc in branchCode:
            if bc == "DE-14":
                retOffers.append({
                    "@type": "Offer",
                    "offeredBy": {
                        "@id": "https://data.slub-dresden.de/organizations/191800287",
                        "@type": "Library",
                        "name": "Sächsische Landesbibliothek – Staats- und Universitätsbibliothek Dresden",
                        "branchCode": "DE-14"
                    },
                    "availability": "https://katalog.slub-dresden.de/id/0-{}".format(swb_ppn)
                })
    if retOffers:
        return retOffers


def removeNone(obj):
    """
    sanitize target records from None Objects
    """
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
                         for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj


def removeEmpty(obj):
    """
    sanitize target records from empty Lists/Objects
    """
    if isinstance(obj, dict):
        toDelete = []
        for k, v in obj.items():
            if v:
                v = ArrayOrSingleValue(removeEmpty(v))
            else:
                toDelete.append(k)
        for key in toDelete:
            obj.pop(key)
        return obj
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, list):
        for elem in obj:
            if elem:
                elem = removeEmpty(elem)
            else:
                del elem
        return obj


def getName(record, key, entity):
    """
    get the name of the record
    """
    data = getAlternateNames(record, key, entity)
    if isinstance(data, list):
        data = " ".join(data)
    return data if data else None


def getAlternateNames(record, key, entity):
    """
    get the alternateName of the record
    """
    data = getmarc(record, key, entity)
    if isinstance(data, str):
        if data[-2:] == " /":
            data = data[:-2]
    elif isinstance(data, list):
        for n, i in enumerate(data):
            if i[-2:] == " /":
                data[n] = i[:-2]
    return data if data else None


def handle_preferredName_topic(record, key, entity):
    """
    get the preferredName of an Topic
    """
    preferredName = ""
    if record.get(key):
        for indicator_level in record[key]:
            for subfield in indicator_level:
                for sf_elem in indicator_level.get(subfield):
                    for k, v in sf_elem.items():
                        if k == "a":  # is always single valued  https://swbtools.bsz-bw.de/cgi-bin/help.pl?cmd=kat&val=150
                            preferredName = v
                        elif k=="x":  # repeatable
                            preferredName += " / {}".format(v)
                        elif k=="g":  # repeatable
                            preferredName += " ({})".format(v)
    if preferredName:
        return preferredName


def getpublisher(record, key, entity):
    """
    produces a Publisher-node out of two different MARC21-Fields
    """
    pub_name = getmarc(record, ["260..b", "264..b"], entity)
    pub_place = getmarc(record, ["260..a", "264..a"], entity)
    if pub_name or pub_place:
        data = {}
        if pub_name:
            if pub_name[-1] in [".", ",", ";", ":"]:
                data["name"] = pub_name[:-1].strip()
            else:
                data["name"] = pub_name
            data["@type"] = "Organization"
        if pub_place:
            if pub_place[-1] in [".", ",", ";", ":"] and isinstance(pub_place, str):
                data["location"] = {"name": pub_place[:-1].strip(),
                                    "type": "Place"}
            else:
                data["location"] = {"name": pub_place,
                                    "type": "Place"}
        return data


def get_physical(record, key, entity):
    """
    get the physical description of the entity
    """
    phys_map = {"extent": "300..a",
                "physical_details": "300..b",
                "dimensions": "300..c",
                "accompanying_material": "300..e",
                "reproduction_extent": "533..e"}
    data = {}
    for key, marc_key in phys_map.items():
         value = getmarc(record, marc_key, entity)
         if value:
             data[key] = value
    if data:
        return data


def get_collection(record, keys, entity):
    """
    get the collection description of the entity
    """
    data = []
    for key in keys:
        value = getmarc(record, key, "resources")
        if value:
            if isinstance(value, str):
                value = [value]
            for item in value:
                if key.startswith("084"):
                    if item in lookup_ssg_fid:
                        data.append({"preferredName": lookup_ssg_fid[item],
                                     "abbr": item})
                if key.startswith("935"):
                    if item in lookup_coll:
                        data.append({"preferredName": lookup_coll[item],
                                     "abbr": item})
    if data:
        return data


def single_or_multi(ldj, entity):
    """
    make Fields single or multi valued according to spec defined in the mapping table
    """
    for k in entities[entity]:
        for key, value in ldj.items():
            if key in k:
                if "single" in k:
                    ldj[key] = ArrayOrSingleValue(value)
                elif "multi" in k:
                    if not isinstance(value, list):
                        ldj[key] = [value]
    return ldj


def getentity(record):
    """
    get the entity type of the described Thing in the record, based on the map_entity table
    """
    zerosevenninedotb = getmarc(record, "079..b", None)
    if zerosevenninedotb in map_entities:
        return map_entities[zerosevenninedotb]
    elif not zerosevenninedotb:
        return "resources"  # Titeldaten ;)
    else:
        return


def getdateModified(record, key, entity):
    """
    get the DateModified field from the MARC21-Record,
    date of the last modification of the MARC21-Record
    """
    date = getmarc(record, key, entity)
    newdate = ""
    if date:
        for i in range(0, 13, 2):
            if isint(date[i:i+2]):
                newdate += date[i:i+2]
            else:
                newdate += "00"
            if i in (2, 4):
                newdate += "-"
            elif i == 6:
                newdate += "T"
            elif i in (8, 10):
                newdate += ":"
            elif i == 12:
                newdate += "Z"
        return newdate


def handle_dateCreated(record, key, entity):
    """
    get the dateCreated field from the MARC21-Record
    """
    date = getmarc(record,key, entity)
    YY = int(date[0:2])
    MM = int(date[2:4])
    DD = int(date[4:6])
    ## check if Year is a 19XX record
    if YY > int(datetime.datetime.now().date().strftime("%y")):
        return "19{:02d}-{:02d}-{:02d}".format(YY,MM,DD)
    else:
        return "20{:02d}-{:02d}-{:02d}".format(YY,MM,DD)


def geteditionStatement(record, key, entity):
    a = getmarc(record, "250..a", entity)
    b = getmarc(record, "250..b", entity)
    if a and b:
        return "{}, {}".format(a,b)


def geteditionSequence(record, key, entity):
    if key in record:
        for indicator_level in record[key]:
            if "0_" in indicator_level:
                for item in indicator_level["0_"]:
                    if "a" in item:
                        return item["a"]


def get_cartData(record, key, entity):
    scale = getmarc(record, "255..a", entity)
    projection = getmarc(record, "255..b", entity)
    coordinates = getmarc(record, "255..c", entity)

    data = {}
    if scale:
        data["scale"] = scale
    if projection:
        data["projection"] = projection
    if coordinates:
        data["coordinates"] = coordinates
    if data:
        return data


def get_footnotes(record, keys, entity):
    """
    get additionalInfo based on the footnotes found in the MARC21-Records
    """
    data = []
    all_subfieldsets = {}
    for key in keys:
        marc_data = getmarc(record, key, entity)
        if marc_data:
            all_subfieldsets[key] = []
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if isinstance(marc_data, list):
            for indicator_level in marc_data:
                for _ind in indicator_level:
                    sset = {}
                    for subfield_dict in indicator_level[_ind]:
                        for k,v in subfield_dict.items():
                            sset[k] = litter(sset.get(k),v)
                    all_subfieldsets[key].append(sset)
    all_subfieldsets = removeEmpty(all_subfieldsets)
    for key, rawDataArray in all_subfieldsets.items():
        for rawData in rawDataArray:
            item = {}
            item["@type"] = footnotes_lookups[key]["@type"]
            for k, v in rawData.items():
                if footnotes_lookups[key].get(k):
                    item[footnotes_lookups[key][k]] = v
                if k == '0':
                    if isinstance(v, str):
                        v = [v]
                    for _id in v:
                        if _id.startswith("(DE-627"):
                            item["@id"] = "https://data.slub-dresden.de/topics/{}".format(_id[8:])
                    item["sameAs"] = gnd2uri(v)
            if key == "937":
                if "d" in rawData or "e" in rawData or "f" in rawData:
                    item["@type"] = "instrumentationNote"
                    item["instrumentation"] = item.pop("description")
                concat_values = []
                for concat_key in ['a','b','c','d','e','f']:
                    if concat_key in rawData:
                        concat_values.append(rawData[concat_key])
                item["description"] = "; ".join(concat_values)
            if key == "502":
                concat_values = []
                for concat_key in ['a','b','c','d']:
                    if concat_key in rawData:
                        concat_values.append(rawData[concat_key])
                    item["description"] = ", ".join(concat_values)
            if len(item) > 1:
                data.append(item)
    return data


def gettitle(record, keys, entity):
    """
    create the title object out of different MARC21 Fields
    """
    title_obj = {}

    # Hauptsachtitel:
    marc_data = getmarc(record, "245", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if isinstance(marc_data, list):
        for indicator_level in marc_data:
            for _ind in indicator_level:
                sset = {}
                for subfield_dict in indicator_level[_ind]:
                    for k,v in subfield_dict.items():
                        sset[k] = litter(sset.get(k),v)
                title_obj["preferredName"] = ""
                if sset.get('a'):
                    title_obj["preferredName"] += sset['a']
                    title_obj["mainTitle"] = sset['a']
                if sset.get('b'):
                    title_obj["preferredName"] += " ; {}".format(sset['b'])
                    title_obj["subTitle"] = sset['b']
                if sset.get('c'):
                    title_obj["preferredName"] += " / {}".format(sset['c'])
                    title_obj["responsibilityStatement"] = sset['c']
                if sset.get('n') and sset.get('p'):
                    title_obj["partStatement"] = "{} / {}".format(sset['n'],sset['p'])

    # Paralleltitel
    marc_data = getmarc(record, "246", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == '31':
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    p_tit = {"preferredName": "{} / {}".format(sset['a'],sset['b']),
                             "mainTitle": sset['a'],
                             "subTitle": sset['b']}
                    title_obj["parallelTitles"] = litter(title_obj.get("parallelTitles"), p_tit)

    # Zusammenstellungen
    marc_data = getmarc(record, "249", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if isinstance(marc_data, list):
        for indicator_level in marc_data:
            for _ind in indicator_level:
                sset = {}
                for subfield_dict in indicator_level[_ind]:
                    for k,v in subfield_dict.items():
                        sset[k] = litter(sset.get(k),v)
                o_part_tit = {}
                if "a" in sset:
                    o_part_tit["mainTitle"] = sset["a"]
                if "b" in sset:
                    title_obj["subTitle"] = litter(title_obj.get("subTitle"), sset["b"])
                    o_part_tit["subTitle"] = sset["b"]
                if "v" in sset:
                    o_part_tit["responsibilityStatement"] = litter(o_part_tit.get("responsibilityStatement"), sset["v"])
                if "c" in sset:
                    o_part_tit["responsibilityStatement"] = litter(o_part_tit.get("responsibilityStatement"), sset["c"])
                    title_obj["responsibilityStatement"] = litter(title_obj.get("responsibilityStatement"), sset["c"])
                if "a" and "v" in sset:
                    o_part_tit["preferredName"] = "{} / {}".format(sset["a"],sset["v"])
                if o_part_tit:
                    title_obj["otherPartsTitle"] = litter(title_obj.get("otherPartsTitle"), o_part_tit)

    # Beigefügte Werke
    addInfo = {}
    marc_data = getmarc(record, "501.__.a", entity)
    if marc_data:
        addInfo["notes"] = marc_data
    marc_data = getmarc(record, "505", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == '80':
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    e_part_tit = {}
                    if 'a' in sset:
                        addInfo["notes"] = litter(addInfo.get("notes"), sset['a'])
                    if 't' in sset:
                        e_part_tit["preferredName"] = sset['t']
                    if 'r' in sset:
                        e_part_tit["contributor"] = sset['r']
                    if 'g' in sset:
                        e_part_tit["note"] = sset['g']
                    if e_part_tit:
                        addInfo["enclosedParts"] = litter(addInfo.get("enclosedParts"), e_part_tit)
    if addInfo:
        title_obj["additionalInfo"] = addInfo

    # Zeitschriftenkurztitel
    marc_data = getmarc(record, "210.10.a", entity)
    if marc_data:
        title_obj["shortTitle"] = marc_data
    var_titles = []
    marc_data = getmarc(record, "246", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == "1_":
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    var_title = {}
                    if "a" in sset:
                        var_title["preferredName"] = sset["a"]
                    if "i" in sset:
                        var_title["disambiguatingDescription"] = sset["i"]
                    if var_title:
                        var_titles.append(var_title)
    marc_data = getmarc(record, "246.33.a", entity)
    if marc_data:
        var_titles.append({"preferredName": marc_data})
    if var_titles:
        title_obj["varyingTitles"] = var_titles
    formerTitles = []
    marc_data = getmarc(record, "247", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == "10":
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    formerTitle = {}
                    if "a" in sset:
                        formerTitle["preferredName"] = sset["a"]
                    if "f" in sset:
                        formerTitle["disambiguatingDescription"] = sset["f"]
                    if formerTitle:
                        formerTitles.append(formerTitle)
    if formerTitles:
        title_obj["formerTitles"] = formerTitles

    # Werktitel
    uniformTitles = []
    for key in ["130", "240", "700", "710", "711", "730"]:
        marc_data = getmarc(record, key, entity)
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if marc_data:
            for indicator_level in marc_data:
                for indicator, subfields in indicator_level.items():
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    uniformTitle = {}
                    eprint(sset)
                    if "a" in sset and key in ["130", "240", "730"]:
                        uniformTitle["preferredName"] = sset["a"]
                    if "t" in sset and key in ["700", "710", "711"]:
                        uniformTitle["preferredName"] = sset["t"]
                    if not uniformTitle.get("preferredName"):
                        continue
                    if "0" in sset:
                        uniformTitle["sameAs"] = gnd2uri(sset["0"])
                        for n, sameAs in enumerate(uniformTitle["sameAs"]):
                            if not sameAs:
                                del uniformTitle["sameAs"][n]
                        if isinstance(uniformTitle["sameAs"], str):
                            uniformTitle["sameAs"] = [uniformTitle.pop("sameAs")]
                        if uniformTitle["sameAs"]:
                            for sameAs in uniformTitle["sameAs"]:
                                if isinstance(sameAs, str) and sameAs.startswith(base_id):
                                    uniformTitle["@id"] = id2uri(sameAs.split(base_id)[1], "works")
                    if uniformTitle:
                        uniformTitles.append(uniformTitle)
    if uniformTitles:
        title_obj["uniformTitles"] = uniformTitles

    if title_obj:
        return title_obj


def traverse(dict_or_list, path):
    """
    iterate through a python dict or list, yield all the keys/values
    """
    iterator = None
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list, list):
        iterator = enumerate(dict_or_list)
    elif isinstance(dict_or_list, str):
        strarr = []
        strarr.append(dict_or_list)
        iterator = enumerate(strarr)
    else:
        return
    if iterator:
        for k, v in iterator:
            yield path + str([k]), v
            if isinstance(v, (dict, list)):
                for k, v in traverse(v, path + str([k])):
                    yield k, v


def get_source_include_str():
    """
    get the source_include string when calling the ElasticsSearch Instance,
    so you just get the values you need and save bandwidth
    """
    items = set()
    items.add("079")
    for k, v in traverse(entities, ""):
        # eprint(k,v)
        if isinstance(v, str) and isint(v[:3]) and v not in items:
            items.add(v[:3])
    # eprint(_source)
    return list(items)


def process_field(record, value, entity):
    """
    process a single field according to the mapping
    """
    ret = []
    if isinstance(value, dict):
        for function, parameter in value.items():
            # Function with parameters defined in mapping
            ret.append(function(record, parameter, entity))
    elif isinstance(value, str):
        return value
    elif isinstance(value, list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record, elem, entity)))
    elif callable(value):
        # Function without paremeters defined in mapping
        return ArrayOrSingleValue(value(record, entity))
    if ret:
        return ArrayOrSingleValue(ret)


# processing a single line of json without whitespace
def process_line(jline, index):
    """
    process a record according to the mapping, calls process_field for every field and adds some context,
    """
    entity = getentity(jline)
    if entity:
        mapline = {}
        for sortkey, val in entities[entity].items():
            key = sortkey.split(":")[1]  # sortkey.split(":")[0] is: single or multi, key is the rest
            value = ArrayOrSingleValue(process_field(jline, val, entity))
            if value:
                if "related" in key and isinstance(value, dict) and "_key" in value:
                    dictkey = value.pop("_key")
                    mapline[dictkey] = litter(mapline.get(dictkey), value)
                elif "related" in key and isinstance(value, list) and any("_key" in x for x in value):
                    for elem in value:
                        if "_key" in elem:
                            relation = elem.pop("_key")
                            dictkey = relation
                            mapline[dictkey] = litter(
                                mapline.get(dictkey), elem)
                else:
                    mapline[key] = litter(mapline.get(key), value)
        if mapline:
            if index:
                mapline["isBasedOn"] = target_id+"source/" + \
                    index+"/"+getmarc(jline, "001", None)
            if isinstance(mapline.get("sameAs"), list):
                for n, sameAs in enumerate(mapline["sameAs"]):
                    mapline["sameAs"][n]["isBasedOn"]["@id"] = mapline["isBasedOn"]
                    if mapline["sameAs"][n].get("publisher") and mapline["sameAs"][n]["publisher"]["abbr"] == "BSZ":
                        mapline["sameAs"][n]["@id"] = "https://swb.bsz-bw.de/DB=2.1/PPNSET?PPN={}".format(getmarc(jline, "001", None))
            return {entity: single_or_multi(removeNone(removeEmpty(mapline)), entity)}


def setupoutput(prefix):
    """
    set up the output environment
    """
    if prefix:
        if not os.path.isdir(prefix):
            os.mkdir(prefix)
        if not prefix[-1] == "/":
            prefix += "/"
    else:
        prefix = ""
    for entity in entities:
        if not os.path.isdir(prefix+entity):
            os.mkdir(prefix+entity)


def init_mp(pr, z):
    """
    initialize the multiprocessing environment for every worker
    """
    global prefix
    global comp
    if not pr:
        prefix = ""
    elif pr[-1] != "/":
        prefix = pr+"/"
    else:
        prefix = pr
    comp = z


def worker(ldj, index):
    """
    worker function for multiprocessing
    """
    try:
        if isinstance(ldj, dict):
            ldj = [ldj]
        if isinstance(ldj, list):    # list of records
            for source_record in ldj:
                target_record = process_line(source_record.pop(
                    "_source"), index)
                if target_record:
                    for entity in target_record:
                        name = prefix+entity+"/" + \
                            str(current_process().name)+"-records.ldj"
                        if comp:
                            opener = gzip.open
                            name += ".gz"
                        else:
                            opener = open
                        with opener(name, "at") as out:
                            print(json.dumps(
                                target_record[entity], indent=None), file=out)
    except Exception:
        with open("errors.txt", 'a') as f:
            traceback.print_exc(file=f)


"""Mapping:
 a dict() (json) like table with function pointers
 entitites={"entity_types:{"single_or_multi:target":"string",
                           "single_or_multi:target":{function:"source"},
                           "single_or_multi:target:function}
                           }
 
"""

entities = {
    "resources": {   # mapping is 1:1 like works
        "single:@type": [URIRef(u'http://schema.org/CreativeWork')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "multi:identifier": {handle_identifier: ["035..a"]},
        "single:offers": {getav_katalog: ["924..b", "001"]},
        "single:_isil": {getisil: ["003", "852..a", "924..b"]},
        "single:_ppn": {getmarc: "001"},
        "single:_sourceID": {getmarc: "980..b"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["016", "035..a"]},
        "single:title": {gettitle: ["130", "210", "240", "245", "246", "247", "249", "501", "505", "700", "710", "711", "730"]},
        "single:preferredName": {getName: ["245..a", "245..b"]},
        "single:nameShort": {getAlternateNames: "245..a"},
        "single:nameSub": {getAlternateNames: "245..b"},
        "single:alternativeHeadline": {getAlternateNames: ["245..c"]},
        "multi:alternateName": {getAlternateNames: ["240..a", "240..p", "246..a", "246..b", "245..p", "249..a", "249..b", "730..a", "730..p", "740..a", "740..p", "920..t"]},
        "multi:author": {get_subfields: ["100", "110"]},
        "multi:contributor": {get_subfields: ["700", "710"]},
        "single:publisher": {getpublisher: ["260..a""260..b", "264..a", "264..b"]},
        "single:datePublished": {datePublished: ["008", "533", "534", "264"]},
        "single:dateOriginalPublished": {dateOriginalPublished: ["008", "533", "534", "264"]},
        "single:Thesis": {getmarc: ["502..a", "502..b", "502..c", "502..d"]},
        "multi:issn": {getmarc: ["022..a", "022..y", "022..z", "029..a", "490..x", "730..x", "773..x", "776..x", "780..x", "785..x", "800..x", "810..x", "811..x", "830..x"]},
        "multi:isbn": {getisbn: ["020..a", "022..a", "022..z", "776..z", "780..z", "785..z"]},
        "multi:genre": {getgenre: "655..a"},
        "multi:hasPart": {handleHasPart: ["700"]},
        "multi:isPartOf": {getmarc: ["773..t", "773..s", "773..a"]},
        "multi:partOfSeries": {get_subfield: "830"},
        "single:license": {getmarc: "540..a"},
        "multi:inLanguage": {getmarc: ["377..a", "041..a", "041..d", "130..l", "730..l"]},
        "single:numberOfPages": {getnumberofpages: ["300..a", "300..b", "300..c", "300..d", "300..e", "300..f", "300..g"]},
        "single:pageStart": {getmarc: "773..q"},
        "single:issueNumber": {getmarc: "773..l"},
        "single:volumeNumer": {getmarc: "773..v"},
        "multi:locationCreated": {get_subfield_if_4: "551^4:orth"},
        "multi:relatedTo": {relatedTo: "500..0"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "multi:description": {getmarc: ["520..a"]},
        "multi:mentions": {get_subfield: "689"},
        "multi:relatedEvent": {get_subfield: "711"},
        "single:physical_description": {get_physical: ["300","533"]},
        "multi:collection": {get_collection: ["084..a","935..a"]},
        "single:editionStatement": {geteditionStatement: "250"},
        "single:reproductionType": {getmarc: "533..a"},
        "single:editionSequence": {geteditionSequence: "362"},
        "single:cartographicData": {get_cartData: "255"},
        "multi:additionalInfo": {get_footnotes: ["242", "385", "500", "502", "508", "511", "515", "518", "521", "533", "535", "538", "546", "555", "561", "563", "937"]}
        },
    "works": {
        "single:@type": [URIRef(u'http://schema.org/CreativeWork')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "single:preferredName": {getName: ["100..t", "110..t", "130..t", "111..t", "130..a"]},
        "single:alternativeHeadline": {getmarc: ["245..c"]},
        "multi:alternateName": {getmarc: ["400..t", "410..t", "411..t", "430..t", "240..a", "240..p", "246..a", "246..b", "245..p", "249..a", "249..b", "730..a", "730..p", "740..a", "740..p", "920..t"]},
        "multi:author": {get_subfield: "500"},
        "multi:contributor": {get_subfield: "700"},
        "single:publisher": {getpublisher: ["260..a""260..b", "264..a", "264..b"]},
        "single:datePublished": {getmarc: ["130..f", "260..c", "264..c", "362..a"]},
        "single:Thesis": {getmarc: ["502..a", "502..b", "502..c", "502..d"]},
        "multi:issn": {getmarc: ["022..a", "022..y", "022..z", "029..a", "490..x", "730..x", "773..x", "776..x", "780..x", "785..x", "800..x", "810..x", "811..x", "830..x"]},
        "multi:isbn": {getmarc: ["020..a", "022..a", "022..z", "776..z", "780..z", "785..z"]},
        "single:genre": {getmarc: "655..a"},
        "single:hasPart": {getmarc: "773..g"},
        "single:isPartOf": {getmarc: ["773..t", "773..s", "773..a"]},
        "single:license": {getmarc: "540..a"},
        "multi:inLanguage": {getmarc: ["377..a", "041..a", "041..d", "130..l", "730..l"]},
        "single:numberOfPages": {getnumberofpages: ["300..a", "300..b", "300..c", "300..d", "300..e", "300..f", "300..g"]},
        "single:pageStart": {getmarc: "773..q"},
        "single:issueNumber": {getmarc: "773..l"},
        "single:volumeNumer": {getmarc: "773..v"},
        "single:locationCreated": {get_subfield_if_4: "551^orth"},
        "multi:relatedTo": {relatedTo: "500"},
        "single:dateOfEstablishment": {startDate: "548^datb,dats"},
        "single:dateOfTermination": {endDate: "548^datb,dats"}
    },
    "persons": {
        "single:@type": [URIRef(u'http://schema.org/Person')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: "100..a"},
        "single:gender": {handlesex: "375..a"},
        "multi:alternateName": {getmarc: ["400..a", "400..c"]},
        "multi:relatedTo": {relatedTo: "500..0"},
        "multi:hasOccupation": {get_subfield: "550"},
        "single:birthPlace": {get_subfield_if_4: "551^ortg"},
        "single:deathPlace": {get_subfield_if_4: "551^orts"},
        "single:workLocation": {get_subfield_if_4: "551^ortw"},
        "multi:honorificPrefix": [{get_subfield_if_4: "550^adel"}, {get_subfield_if_4: "550^akad"}],
        "single:birthDate": {startDate: "548"},
        "single:deathDate": {endDate: "548"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:periodOfActivityStart": {startDate: "548^datw,datz"},
        "single:periodOfActivityEnd": {endDate: "548^datw,datz"},
        "single:birthDate": {startDate: "548^datl,datx"},
        "single:deathDate": {endDate: "548^datl,datx"},
    },
    "organizations": {
        "single:@type": [URIRef(u'http://schema.org/Organization')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: "110..a+b"},
        "multi:alternateName": {getmarc: "410..a+b"},

        "single:additionalType": {get_subfield_if_4: "550^obin"},
        "single:parentOrganization": {get_subfield_if_4: "551^adue"},
        "single:location": {get_subfield_if_4: "551^orta"},
        "single:fromLocation": {get_subfield_if_4: "551^geoa"},
        "single:areaServed": {get_subfield_if_4: "551^geow"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:dateOfEstablishment": {startDate: "548^datb"},
        "single:dateOfTermination": {endDate: "548^datb"}
    },
    "geo": {
        "single:@type": [URIRef(u'http://schema.org/Place')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: "151..a"},
        "multi:alternateName": {getmarc: "451..a"},
        "single:description": {get_subfield: "551"},
        "single:geo": {getGeoCoordinates: {"longitude": ["034..d", "034..e"], "latitude": ["034..f", "034..g"]}},
        "single:adressRegion": {getmarc: "043..c"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:dateOfEstablishment": {startDate: "548^datb,dats"},
        "single:dateOfTermination": {endDate: "548^datb,dats"}
    },
    "topics": {
        "single:@type": [URIRef(u'http://schema.org/Thing')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "single:preferredName": {handle_preferredName_topic: "150"},
        "multi:alternateName": {getmarc: "450..a+x"},
        "single:description": {getmarc: "679..a"},
        "multi:additionalType": {get_subfield: "550"},
        "multi:location": {get_subfield_if_4: "551^orta"},
        "multi:fromLocation": {get_subfield_if_4: "551^geoa"},
        "multi:areaServed": {get_subfield_if_4: "551^geow"},
        "multi:contentLocation": {get_subfield_if_4: "551^punk"},
        "multi:participant": {get_subfield_if_4: "551^bete"},
        "multi:relatedTo": {get_subfield_if_4: "551^vbal"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:dateOfEstablishment": {startDate: "548^datb"},
        "single:dateOfTermination": {endDate: "548^datb"}
    },
    "events": {
        "single:@type": [URIRef(u'http://schema.org/Event')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: ["111..a"]},
        "multi:alternateName": {getmarc: ["411..a"]},
        "single:location": {get_subfield_if_4: "551^ortv"},
        "single:startDate": {startDate: "548^datv"},
        "single:endDate": {endDate: "548^datv"},
        "single:adressRegion": {getmarc: "043..c"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
}

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
