from es2json import ArrayOrSingleValue, litter
from esmarc.lookup_tables.entities import map_entities, map_types


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


def get_subsets(record, keys, inds):
    """
    turns:
    [{"a": "Normalrillen-Schallplatte"},{"0": "(DE-588)4135113-7"},{"0": "(DE-627)105667471"},{"0": "(DE-576)209658037"},{"2": "gnd-carrier"}]
    into:
    {"a": "Normalrillen-Schallplatte",
     "0": ["(DE-588)4135113-7", "(DE-627)105667471", "(DE-576)209658037"],
     "2": "gnd-carrier"}
     
     record: pure marc record
     keys: the keys to get, can be a single key or a list/set of keys
     inds: to specifiy indicators. for all indicators, use '*', for multiple distinguished indicators, use a list/set
           negate indicators with !
    """
    if isinstance(keys,str):
        keys = [keys]
    if isinstance(inds,str):
        inds = [inds]
    for key in keys:
        marc_data = getmarc(record, key, None)
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if marc_data:
            for indicator_level in marc_data:
                for indicator, subfields in indicator_level.items():
                    for ind in inds:
                        if (not ind[0]=='!' and ind[1:] != indicator) and ( ind == "*" or ind==indicator or (ind[1] == '*' and ind[0] == indicator[0])):
                            sset = {}
                            for subfield in subfields:
                                for k,v in subfield.items():
                                    sset[k] = litter(sset.get(k),v)
                            yield sset


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
