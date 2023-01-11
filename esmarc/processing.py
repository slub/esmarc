from es2json import ArrayOrSingleValue, litter
from esmarc import globals
from esmarc.helperfunc import removeEmpty, removeNone, single_or_multi
from esmarc.marc import getmarc, getentity


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
def process_line(jline, index, entities):
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
                mapline["isBasedOn"] = globals.target_id+"source/" + \
                    index+"/"+getmarc(jline, "001", None)
            if isinstance(mapline.get("sameAs"), list):
                for n, sameAs in enumerate(mapline["sameAs"]):
                    mapline["sameAs"][n]["isBasedOn"]["@id"] = mapline["isBasedOn"]
                    if mapline["sameAs"][n].get("publisher") and mapline["sameAs"][n]["publisher"]["abbr"] == "BSZ":
                        mapline["sameAs"][n]["@id"] = "https://swb.bsz-bw.de/DB=2.1/PPNSET?PPN={}".format(getmarc(jline, "001", None))
            return {entity: single_or_multi(removeNone(removeEmpty(mapline)), entities, entity)}
