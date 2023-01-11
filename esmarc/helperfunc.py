from es2json import ArrayOrSingleValue, isint
import os


def setupoutput(entities, prefix):
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


def get_source_include_str(entities):
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


def single_or_multi(ldj, entities, entity):
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
