from esmarc.marc import get_subsets
from esmarc.lookup_tables.classifications import classifications
from esmarc.lookup_tables.entities import map_fields, map_entities,  map_types_mentions
from es2json import eprint
from copy import deepcopy

def merge_entry(data, entry):
    if not data:
        return [entry]
    else:
        for n,item in enumerate(data):
            if entry["name"] == item["name"]:
                if entry["CategoryCodes"][0] not in item["CategoryCodes"]:
                    data[n]["CategoryCodes"].append(entry["CategoryCodes"][0])
                return data
        data.append(entry)
        return data


def get_class(record, keys, entity):
    """
    create the classification object out of different MARC21 Fields
    """
    data = []

# if we use a dict from lookup tables as a stencil for a data structure and
# not a reference, we need a complete copy of it and not just a reference.
# if we use a reference, we would modify the original data structure from
# the lookup table and make it garbage for further use. during runtime
# thats why we use deepcopy()

    for key_ind in keys:  # "084.__"
        key = key_ind.split(".")[0]  # "084"
        ind = key_ind.split(".")[1]  # "__"
        for sset in get_subsets(record, key, ind):
            if sset.get('a'):
                entry = None
                if key_ind in classifications:
                    entry = deepcopy(classifications[key_ind])
                    entry["CategoryCodes"][0]["codeValue"] = sset['a']
                elif sset.get('2') in classifications:
                    entry = deepcopy(classifications[sset['2']])
                    entry["CategoryCodes"][0]["codeValue"] = sset['a']
                if entry:
                    if entry["CategoryCodes"][0].get("@id"):
                        entry["CategoryCodes"][0]["@id"] += sset['a']
                    data = merge_entry(data,entry)
    return data if data else None


def get_mentions(record, keys, entity):
    """
    creates the mentions array out of different MARC21 Fields
    """
    data = []

    for key in keys:
        for sset in get_subsets(record, key, '*'):
            obj = {}
            if key == "689":
                if sset.get("5"):
                    continue
                if sset.get('A') and sset['A'] == 'z':
                    obj["@type"] = "ChronologicalSubject"
                elif sset.get('D'):
                    obj["@type"] = map_types_mentions[sset['D']]
                    if sset.get('0'):
                        if isinstance(sset['0'],str):
                            sset['0'] = [sset.pop('0')]
                        for item in sset['0']:
                            if item.startswith("(DE-627") and sset.get('D') in map_entities:
                                obj["@id"] = "https://data.slub-dresden.de/{}/{}".format(map_entities[sset['D']],item.split(")")[1])
                            if item.startswith("(DE-588"):
                                obj["sameAs"] = "https://d-nb.info/gnd/{}".format(item.split(")")[1])
            if key in map_fields:
                obj["@type"] = map_fields[key]["@type"]
                if key in ("610","611") and (sset.get("c") or sset.get("d")):
                    obj["@type"] = "Event"
                if sset.get('0'):
                    if isinstance(sset['0'],str):
                        sset['0'] = [sset.pop('0')]
                    for item in sset['0']:
                        if item.startswith("(DE-627"):
                            if key in ("610","611") and (sset.get("c") or sset.get("d")):
                                obj["@id"] = "https://data.slub-dresden.de/events/{}".format(item.split(")")[1])
                            elif map_fields[key].get("@id"):
                                obj["@id"] = "https://data.slub-dresden.de/{}/{}".format(map_fields[key]["@id"],item.split(")")[1])
                        if item.startswith("(DE-588"):
                            obj["sameAs"] = "https://d-nb.info/gnd/{}".format(item.split(")")[1])
            if sset.get('a') and isinstance(sset.get('a'), str):
                obj["preferredName"] = sset['a']
                obj["name"] = sset['a']
            if key.startswith("65") and sset.get('a') and isinstance(sset.get('a'), list):
                for item in sset['a']:
                    obj["preferredName"] = sset['a']
                    obj["name"] = sset['a']
                    if obj not in data:
                        data.append(deepcopy(obj))
                continue
            if key == "600":
                if sset.get('b'):
                    obj["preferredName"] += " {}".format(sset['b'])
                    obj["name"] += " {}".format(sset['b'])
                if sset.get('c'):
                    obj["preferredName"] += ", {}".format(sset['c'])
                    obj["name"] += ", {}".format(sset['c'])
                if sset.get('d'):
                    obj["preferredName"] += " ({})".format(sset['d'])
            if obj["@type"] == "Organisation":
                if sset.get('b'):
                    obj["preferredName"] += ", {}".format(sset['b'])
                    obj["name"] += ", {}".format(sset['b'])
                if sset.get('g'):
                    obj["preferredName"] += ", {}".format(sset['g'])
                if sset.get('e'):
                    obj["name"] += ", {}".format(sset['e'])
            if obj["@type"] == "Event":
                for char in ('n','d','c','e','g'):
                    if sset.get(char):
                        obj["preferredName"] += ", {}".format(sset[char])
            if key == "630" or (key == "689" and sset.get('D') and sset['D'] in ('g', 'u')):
                if sset.get("p"):
                    obj["preferredName"] += " / {}".format(sset['p'])
                    obj["name"] += " / {}".format(sset['p'])
                if sset.get("n"):
                    obj["preferredName"] += " <{}>".format(sset['n'])
                if sset.get("g"):
                    obj["preferredName"] += " <{}>".format(sset['g'])
            if sset.get('n') and (key in ("610","611","630") or (key == '689' and sset.get('D') and sset['D'] in ('b','u'))):
                obj['position'] = sset['n']
            if sset.get('d') and (key in ("600","610","611") or (key == '689' and sset.get('D') and sset['D'] in ('f','n','p'))):
                obj['date'] = sset['d']
            if sset.get('g') and (key in ("610","611","630","650") or (key == '689' and sset.get('D') and sset['D'] in ('b','f','s','u'))):
                obj['additionalInformation'] = sset['g']
            if obj not in data:
                data.append(obj)
    return data if data else None
