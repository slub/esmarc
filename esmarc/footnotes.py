from es2json import litter
from esmarc.id import gnd2uri
from esmarc.marc import get_subsets
from esmarc.helperfunc import removeEmpty
from esmarc.lookup_tables.footnotes import footnotes_lookups


def get_footnotes(record, keys, entity):
    """
    get additionalInfo based on the footnotes found in the MARC21-Records
    """
    data = []
    all_subfieldsets = {}
    for key in keys:
        all_subfieldsets[key] = []
        for sset in get_subsets(record, key, '*'):
            all_subfieldsets[key].append(sset)
    all_subfieldsets = removeEmpty(all_subfieldsets)
    for key, rawDataArray in all_subfieldsets.items():
        for rawData in rawDataArray:
            item = {}
            if footnotes_lookups.get(key):
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
                concat_values = []
                for concat_key in ['a','b','c','d','e','f']:
                    if concat_key in rawData:
                        if isinstance(rawData[concat_key],list):
                            for sub_item in rawData[concat_key]:
                                concat_values.append(sub_item)
                        else:
                            concat_values.append(rawData[concat_key])
                item["description"] = "; ".join(concat_values)
            if key == "502":
                concat_values = []
                for concat_key in ['a','b','c','d']:
                    if concat_key in rawData:
                        if isinstance(rawData[concat_key],list):
                            for sub_item in rawData[concat_key]:
                                concat_values.append(sub_item)
                        else:
                            concat_values.append(rawData[concat_key])
                    item["description"] = ", ".join(concat_values)
            if len(item) > 1:
                data.append(item)
    return data
