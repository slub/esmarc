from esmarc.marc import getmarc, get_subsets
from esmarc.lookup_tables.collections import lookup_coll, lookup_ssg_fid

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
    return retOffers if retOffers else None


def get_accessmode(record, key, entity):
    """
    get the accessMode (local, online) of the resource
    """
    data = getmarc(record, key, entity)
    if isinstance(data, str) and data[0:2] == "cr" or data[0:2] == "cz":
        return "online"
    else:
        return "local"


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
    return data if data else None


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
    return data if data else None


def get_usageInfo(record, keys, entity):
    """
    get the usage info of the resource
    """
    keymap = {"506": "accessState",
              "540": "licenceState"}
    data = []
    for key_ind in keys:
        key = key_ind.split('.')[0]
        ind = key_ind.split('.')[1]
        for sset in get_subsets(record, key, ind):
            retObject = {"@type": keymap[key]}
            if sset.get('a'):
                retObject["name"] = sset['a']
                retObject["alternateName"] = sset.get('f')
            elif sset.get('f'):
                retObject["name"] = sset['f']
            retObject["sameAs"] = sset.get('u')
            data.append(retObject)
    return data if data else None
