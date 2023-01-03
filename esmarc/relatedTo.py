from es2json import litter, ArrayOrSingleValue
from esmarc.id import uri2url, gnd2uri, id2uri
from esmarc import globals
from esmarc.marc import getmarc
from esmarc.marc import get_subsets
from esmarc.namings import gettitle
from esmarc.lookup_tables.relatedTo import marc2relation

def get_seriesStatement(record, key, entity):
    """
    gets the information about series and hierachries, will only be filled if the resource isn't linked or isn't counted
    """
    ret_data_array = []
    for sset in get_subsets(record, key, "0_"):
        ret_object = {}
        ret_object["name"] = sset.get('a')
        ret_object["position"] = sset.get('v')
        if ret_object:
            ret_data_array.append(ret_object)
    return ret_data_array if ret_data_array else None


def get_partseries(record, keys, entity):
    """
    gets the information if the resource is part of a series
    """
    ret_data_array = []
    for key in keys:
        marc_data = getmarc(record,key,entity)
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if marc_data:
            for indicator_level in marc_data:
                for indicator, subfields in indicator_level.items():
                    if key == "776" and indicator != "1_":
                        continue
                    sset = {}
                    ret_object = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    if sset.get('w'):
                        if isinstance(sset['w'],str):
                            sset['w'] = [sset.pop('w')]
                        for item in sset['w']:
                            if item.startswith("(DE-627"):
                                ret_object["@id"] = "https://data.slub-dresden.de/resources/{}".format(item[8:])
                    if sset.get('a') and not sset.get('t'):
                        ret_object['name'] = sset['a']
                    elif sset.get('a') and sset.get('t'):
                        ret_object['name'] = "{} / {}".format(sset['t'],sset['a'])
                    elif sset.get('t') and not sset.get('a'):
                        ret_object['name'] = sset['t']
                    if sset.get("v"):
                        ret_object['position'] = sset['v']
                    elif sset.get('9') and not sset.get('v'):
                        ret_object['position'] = sset['9']
                    if ret_object:
                        ret_data_array.append(ret_object)
    return ret_data_array if ret_data_array else None


def get_ispartof(record, keys, entity):
    """
    gets the information if the resource is part of a another resource
    """
    ret_data_array = []
    for indicator in ("08","18"):
        for sset in get_subsets(record, "773", indicator):
            ret_object = {}
            if sset.get("w"):
                if isinstance(sset['w'],str):
                    sset['w'] = [sset.pop('w')]
                for item in sset['w']:
                    if item.startswith("(DE-627"):
                        ret_object["@id"] = "https://data.slub-dresden.de/resources/{}".format(item[8:])
            if sset.get('g') and isinstance(sset.get('g'), str):
                sset['g'] = [sset.pop("g")]
            if sset.get('g'):
                ret_object['position'] = ", ".join(sset.get('g'))
            if indicator == "08":
                if sset.get('a') and not sset.get('t'):
                    ret_object['name'] = sset['a']
                elif sset.get('a') and sset.get('t'):
                    ret_object['name'] = "{} / {}".format(sset['t'],sset['a'])
                elif sset.get('t') and not sset.get('a'):
                    ret_object['name'] = sset['t']
                if sset.get('d') and isinstance(sset.get('d'),str):
                    sset['d'] = [sset.pop("d")]
                if sset.get('d'):
                    ret_object['publisherNote'] = ", ".join(sset.get('d'))
                ret_object['displayLabel'] = sset.get('i')
            elif indicator == "18":
                title_obj = gettitle(record, ["130", "210", "240", "245", "246", "247", "249", "501", "505", "700", "710", "711", "730"], entity)
                ret_object["mainTitle"] = title_obj.get("mainTitle")
                if title_obj.get("partStatement") and isinstance(title_obj.get("partStatement"),list):
                    ret_object["partStatement"] = title_obj.get("partStatement")[0]
                ret_object["name"] = "{}. {}".format(title_obj.get("mainTitle"),ret_object.get("partStatement"))
            if ret_object.get("@id") and indicator in ("08","18"):
                ret_data_array.append(ret_object)
    return ret_data_array if ret_data_array else None


def get_relations(record, keys, entity):
    """
    get the relations between one resource to another
    """
    ret_data_array = []
    for sset in get_subsets(record, keys, '0*'):
        ret_object = {}
        if sset.get("w"):
            if isinstance(sset['w'],str):
                sset['w'] = [sset.pop('w')]
            for item in sset['w']:
                if item.startswith("(DE-627"):
                    ret_object["@id"] = "https://data.slub-dresden.de/resources/{}".format(item[8:])
        if sset.get('a') and not sset.get('t'):
            ret_object['name'] = sset['a']
        elif sset.get('a') and sset.get('t'):
            ret_object['name'] = "{} / {}".format(sset['t'],sset['a'])
        elif sset.get('t') and not sset.get('a'):
            ret_object['name'] = sset['t']
        if sset.get('i'):
            ret_object['relationType'] = sset['i']
        if ret_object:
            ret_data_array.append(ret_object)
    return ret_data_array if ret_data_array else None


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
                        if isinstance(uri, str) and uri.startswith(globals.base_id):
                            node["@id"] = id2uri(sset.get("0"), entityType, globals.base_id)
                        elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(globals.base_id):
                            node["sameAs"] = uri
                        elif isinstance(uri, str):
                            node["identifier"] = sset.get("0")
                        elif isinstance(uri, list):
                            node["sameAs"] = None
                            node["identifier"] = None
                            for elem in uri:
                                if elem and isinstance(elem, str) and elem.startswith(globals.base_id):
                                    node["@id"] = id2uri(
                                        elem.split("=")[-1], entityType, globals.base_id)
                                elif elem and isinstance(elem, str) and elem.startswith("http") and not elem.startswith(globals.base_id):
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
                        if isinstance(uri, str) and uri.startswith(globals.base_id):
                            node["@id"] = id2uri(sset.get("0"), entityType, globals.base_id)
                        elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(globals.base_id):
                            node["sameAs"] = uri
                        elif isinstance(uri, str):
                            node["identifier"] = uri
                        elif isinstance(uri, list):
                            node["sameAs"] = None
                            node["identifier"] = None
                            for elem in uri:
                                if elem and elem.startswith(globals.base_id):
                                    node["@id"] = id2uri(
                                        elem.split("=")[-1], entityType, globals.base_id)
                                elif elem and elem.startswith("http") and not elem.startswith(globals.base_id):
                                    node["sameAs"] = litter(
                                        node["sameAs"], elem)
                                elif elem:
                                    node["identifier"] = litter(
                                        node["identifier"], elem)
                    data.append(node)

        if data:
            return ArrayOrSingleValue(data)

