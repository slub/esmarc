from es2json import ArrayOrSingleValue, litter
import copy
from esmarc import globals
from esmarc.marc import get_subsets
from esmarc.id import id2uri, gnd2uri
from esmarc.lookup_tables.entities import map_entities, map_types


def get_subfield_if_4(jline, key, entity):
    """
    gets subfield of marc-Records and builds some nodes out of them if a clause is statisfied
    """
    # e.g. split "551^4:orta" to 551 and orta
    marcfield = key.rsplit("^")[0]
    subfield4 = key.rsplit("^")[1]
    data = []
    for sset in get_subsets(jline, marcfield, '*'):
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
    for sset in get_subsets(jline, key, '*'):
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
            if isinstance(uri, str) and uri.startswith(globals.base_id) and entityType != "resources":
                node["@id"] = id2uri(uri, entityType, globals.base_id)
            elif isinstance(uri, str) and uri.startswith(globals.base_id) and entityType == "resources":
                node["sameAs"] = globals.base_id + \
                    id2uri(uri, entityType, globals.base_id).split("/")[-1]
            elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(globals.base_id):
                node["sameAs"] = uri
            elif isinstance(uri, str):
                node["identifier"] = uri
            elif isinstance(uri, list):
                node["sameAs"] = None
                node["identifier"] = None
                for elem in uri:
                    if isinstance(elem, str) and elem.startswith(globals.base_id):
                        # if key=="830":  #Dirty Workaround for finc id
                            # rsplit=elem.rsplit("=")
                            # rsplit[-1]="0-"+rsplit[-1]
                            # elem='='.join(rsplit)
                        node["@id"] = id2uri(elem, entityType, globals.base_id)
                    elif isinstance(elem, str) and elem.startswith("http") and not elem.startswith(globals.base_id):
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
