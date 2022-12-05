from esmarc.marc import getmarc
from es2json import litter
from esmarc.id import gnd2uri
from esmarc.lookup_tables.sameAs import lookup_sameAs


def getsameAs(jline, keys, entity):
    """
    produces schema.org/sameAs node out of the MARC21-Record
    for KXP, DNB, RISM and others.
    """
    sameAs = []
    raw_data = set()
    data = []
    for key in keys:
        if key == "016":  # 016 has ISIL in 016$2 and ID in 016$a.
            marc_data = getmarc(jline, key, entity)
            if isinstance(marc_data,list):
                for indicator_level in marc_data:
                    for _ind in indicator_level:
                        sset = {}
                        for subfield_dict in indicator_level[_ind]:
                            for k,v in subfield_dict.items():
                                sset[k] = v
                    if sset.get("a") and sset.get("2"):
                        data = litter(data, "({}){}".format(sset["2"], sset["a"]))
        elif key == "035..a":  # 035$a has already both in $a, so we're fine
            data = litter(data, getmarc(jline, key, entity))
    if isinstance(data, str):
        data = [data]
    if isinstance(data, list):
        for elem in data:
            if elem[0:8] in lookup_sameAs:
                data = gnd2uri(elem)
                newSameAs = dict(lookup_sameAs[elem[0:8]])
                newSameAs["@id"] = data
                newSameAs["isBasedOn"] = {"@type": "Dataset", "@id": ""}
                sameAs.append(newSameAs)
    return sameAs
