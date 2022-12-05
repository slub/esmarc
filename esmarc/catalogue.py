from esmarc.marc import getmarc
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
    if retOffers:
        return retOffers


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
    if data:
        return data


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
    if data:
        return data
