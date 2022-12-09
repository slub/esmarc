from esmarc.marc import getmarc, get_subsets
from esmarc.lookup_tables.classifications import classifications
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
    return None
