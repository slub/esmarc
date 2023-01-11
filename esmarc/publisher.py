from esmarc.marc import getmarc, get_subsets
from esmarc.lookup_tables.publisher import places

def getpublisher(record, keys, entity):
    """
    produces a Publisher-node out of two different MARC21-Fields
    """
    pub_obj = {}
    for sset in get_subsets(record, "533",  '*'):
        pub_obj['@type'] = "Organization"
        pub_obj['name'] = sset.get('c')
        pub_obj['location'] = {"@type": "Place"}
        pub_obj['location']['name'] = sset.get('b')
    if not pub_obj.get('name'):
        for sset in get_subsets(record,"264",'*'):
            pub_obj['@type'] = "Organization"
            pub_obj['name'] = sset.get('b')
            pub_obj['location'] = {"@type": "Place"}
            if sset.get('a') and isinstance(sset.get('a'),str):
                sset['a'] = [sset.pop('a')]
            if sset.get('a'):
                pub_obj['location']['name'] = ", ".join(sset.get('a'))
    return pub_obj if pub_obj.get('name') else None


def getoriginalPublisher(record, keys, entity):
    """
    produces a Publisher-node out of two different MARC21-Fields
    """
    pub_obj = {}
    if record.get("533") and record.get("264"):
        for sset in get_subsets(record,"264",'*'):
            pub_obj['@type'] = "Organization"
            pub_obj['name'] = sset.get('b')
            pub_obj['location'] = {"@type": "Place"}
            if sset.get('a') and isinstance(sset.get('a'),str):
                sset['a'] = [sset.pop('a')]
            if sset.get('a'):
                pub_obj['location']['name'] = ", ".join(sset.get('a'))
    return pub_obj if pub_obj.get('name') else None


def getPublishLocation(record, key, entity):
    """
    produeces a location node for the publication places
    """
    data = []
    for sset in get_subsets(record, key, '*'):
        obj = {}
        if sset.get('4'):
            obj["@type"] = places[sset['4']]
        obj["preferredName"] = sset.get('a')
        if sset.get('0') and isinstance(sset['0'],str):
            sset['0'] = [sset.pop('0')]
        if sset.get('0'):
            for item in sset['0']:
                if item.startswith("(DE-627"):
                    obj["@id"] = "https://data.slub-dresden.de/geo/{}".format(item.split(")")[1])
        if obj.get("preferredName"):
            data.append(obj)
    return data if data else None

