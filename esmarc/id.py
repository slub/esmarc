from es2json import litter
from esmarc import globals
from esmarc.marc import getmarc
from esmarc.lookup_tables.sameAs import lookup_sameAs
from esmarc.lookup_tables.identifiers import lookup_identifiers

def gnd2uri(string):
    """
    Transforms e.g. (DE-588)1231111151 to an URI .../1231111151
    """
    try:
        if isinstance(string, list):
            for n, uri in enumerate(string):
                string[n] = gnd2uri(uri)
            return string
        if string and "(DE-" in string:
            if isinstance(string, list):
                ret = []
                for st in string:
                    ret.append(gnd2uri(st))
                return ret
            elif isinstance(string, str):   # added .upper
                return uri2url("({})".format(string.split(')')[0][1:]), string.split(')')[1].upper())
    except:
        return


def uri2url(isil, num):
    """
    Transforms e.g. .../1231111151 to https://d-nb.info/gnd/1231111151,
    not only GNDs, also SWB, GBV, configureable over lookup_sameAs lookup table
    in swb_fix.py
    """
    if isil == "(DE-576)":
        return None
    if isil and num and isil in lookup_sameAs:
        return "{}{}".format(lookup_sameAs[isil]["@id"],num)


def id2uri(string, entity, baseid):
    """
    return an id based on base_id
    """
    if string.startswith(baseid):
        string = string.split(baseid)[1]
    # if entity=="resources":
    #    return "http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN="+string
    # else:
    if globals.target_id and entity and string:
        return str(globals.target_id+entity+"/"+string)


def getid(record, regex, entity):
    """
    wrapper function for schema.org/identifier mapping for id2uri
    """
    _id = getmarc(record, regex, entity)
    if _id:
        return id2uri(_id, entity, globals.base_id)


def getisil(record, regex, entity):
    """
    get the ISIL of the record
    """
    isil = getmarc(record, regex, entity)
    if isinstance(isil, str) and "({})".format(isil) in lookup_sameAs:
        return isil
    elif isinstance(isil, list):
        for item in isil:
            if "({})".format(item) in lookup_sameAs:
                return item


def handle_identifier(record, key, entity):
    ids = []
    marc_data = getmarc(record, key, entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if "2" in sset and "a" in sset:
                    if sset["2"] in lookup_identifiers:
                        id_obj = {}
                        id_obj["@id"] = "{}{}".format(lookup_identifiers[sset["2"]], sset["a"])
                        id_obj["@type"] = "PropertyValue"
                        id_obj["propertyID"] = sset["2"]
                        id_obj["value"] = sset["a"]
                        ids.append(id_obj)
    if ids:
        return ids


def get_identifiedby(record, keys, entity):
    """
    get various identifiers (ISBN,ISSN,ISMN,quotations,others) of the resource
    """
    data = []

    # ISBN
    isbn = {"@type": "ISBN"}
    marc_data = getmarc(record, "020", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("a"):
                    isbn["validValues"] = litter(isbn.get("validValues"), sset.get("a"))
                if sset.get("z"):
                    isbn["invalidValues"] = litter(isbn.get("invalidValues"), sset.get("z"))
    for key in ["770", "772", "773", "775", "776", "780", "785", "787"]:
        marc_data = getmarc(record, key, entity)
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if marc_data:
            for indicator_level in marc_data:
                for indicator, subfields in indicator_level.items():
                    if key == "776" and indicator not in ["08", "1_"]:
                        eprint(key,indicator)
                        continue
                    if key == "787" and indicator != "00":
                        continue
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    if "z" in sset:
                        isbn["relatedValues"] = litter(isbn.get("relatedValues"), sset.get("z"))
    for item in ("validValues","relatedValues","invalidValues"):
        if item in isbn:
            if isinstance(isbn[item],str):
                isbn[item] = [isbn.pop(item)]
            elif isinstance(isbn[item],list):
                isbn[item] = list(set(isbn.pop(item)))
    if isbn.get("validValues") or isbn.get("relatedValues") or isbn.get("invalidValues"):
        data.append(isbn)

    # ISSN
    issn = {"@type": "ISSN"}
    marc_data = getmarc(record, "022", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("a"):
                    issn["validValues"] = litter(issn.get("validValues"), sset.get("a"))
                if sset.get("y"):
                    issn["invalidValues"] = litter(issn.get("invalidValues"), sset.get("y"))
    for key in ["770", "772", "773", "775", "776", "780", "785", "787", "800", "810", "811", "830"]:
        marc_data = getmarc(record, key, entity)
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if marc_data:
            for indicator_level in marc_data:
                for indicator, subfields in indicator_level.items():
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    if "x" in sset:
                        issn["relatedValues"] = litter(issn.get("relatedValues"), sset.get("x"))
    for item in ("validValues","relatedValues","invalidValues"):
        if item in issn:
            if isinstance(issn[item],str):
                issn[item] = [issn.pop(item)]
            elif isinstance(issn[item],list):
                issn[item] = list(set(issn.pop(item)))
    if issn.get("validValues") or issn.get("relatedValues") or issn.get("invalidValues"):
        data.append(issn)

    # ISMN
    ismn = {"@type": "ISMN"}
    marc_data = getmarc(record, "024", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == "2_":
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    if sset.get("a"):
                        ismn["validValues"] = litter(ismn.get("validValues"), sset.get("a"))
                    if sset.get("z"):
                        ismn["invalidValues"] = litter(ismn.get("invalidValues"), sset.get("z"))
    for item in ("validValues","invalidValues"):
        if item in ismn:
            if isinstance(ismn[item],str):
                ismn[item] = [ismn.pop(item)]
            elif isinstance(ismn[item],list):
                ismn[item] = list(set(ismn.pop(item)))
    if ismn.get("validValues") or ismn.get("invalidValues"):
        data.append(ismn)

    # UPC
    upc =  {"@type": "UPC"}
    marc_data = getmarc(record, "024", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == "1_":
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    if sset.get("a"):
                        upc["validValues"] = litter(upc.get("validValues"), sset.get("a"))
    if "validValues" in upc:
        if isinstance(upc["validValues"],str):
            upc["validValues"] = [upc.pop("validValues")]
        elif isinstance(upc["validValues"],list):
            upc["validValues"] = list(set(upc.pop("validValues")))
    if upc.get("validValues"):
        data.append(upc)

    # EAN
    ean =  {"@type": "EAN"}
    marc_data = getmarc(record, "024", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == "3_":
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    if sset.get("a"):
                        ean["validValues"] = litter(ean.get("validValues"), sset.get("a"))
    if "validValues" in ean:
        if isinstance(ean["validValues"],str):
            ean["validValues"] = [ean.pop("validValues")]
        elif isinstance(ean["validValues"],list):
            ean["validValues"] = list(set(ean.pop("validValues")))
    if ean.get("validValues"):
        data.append(ean)

    # Unspecified
    marc_data = getmarc(record, "024", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                if indicator == "8_":
                    sset = {}
                    for subfield in subfields:
                        for k,v in subfield.items():
                            sset[k] = litter(sset.get(k),v)
                    n_a =  {"@type": "Unspecified Number"}
                    if sset.get("q"):
                        n_a["label"] = litter(n_a.get("label"), sset.get("q"))
                    if sset.get("a"):
                        n_a["validValues"] = litter(n_a.get("validValues"), sset.get("a"))
                    for item in ("validValues","label"):
                        if item in n_a:
                            if isinstance(n_a[item],str):
                                n_a[item] = [n_a.pop(item)]
                            elif isinstance(n_a[item],list):
                                n_a[item] = list(set(n_a.pop(item)))
                    if n_a.get("validValues") and n_a not in data:
                        data.append(n_a)

    # Order
    marc_data = getmarc(record, "028", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                order =  {"@type": "Order Number"}
                if sset.get("q"):
                    order["label"] = litter(order.get("label"), sset.get("q"))
                if sset.get("a"):
                    order["validValues"] = litter(order.get("validValues"), sset.get("a"))
                if sset.get("b"):
                    order["publisher"] = litter(order.get("publisher"), sset.get("b"))
                for item in ("validValues","label","publisher"):
                    if item in order:
                        if isinstance(order[item],str):
                            order[item] = [order.pop(item)]
                        elif isinstance(order[item],list):
                            order[item] = list(set(order.pop(item)))
                if order.get("validValues") and order not in data:
                    data.append(order)

    # Report
    marc_data = getmarc(record, "088", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                rep =  {"@type": "Report Number"}
                if sset.get("a"):
                    rep["validValues"] = litter(rep.get("validValues"), sset.get("a"))
                if "validValues" in rep:
                    if isinstance(rep["validValues"],str):
                        rep["validValues"] = [rep.pop("validValues")]
                    elif isinstance(rep["validValues"],list):
                        rep["validValues"] = list(set(rep.pop("validValues")))
                if rep.get("validValues") and rep not in data:
                    data.append(rep)

    # NBN
    nbn = {"@type": "NBN",
      "validValues": None}
    marc_data = getmarc(record, "015", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("2") and sset["2"] == "dnb" and sset.get("a"):
                    nbn["validValues"] = sset["a"]
                    if isinstance(nbn["validValues"],str):
                        nbn["validValues"] = [nbn.pop("validValues")]
                    if nbn not in data:
                        data.append(nbn)

    # vd16/17/18
    for item in ("16", "17", "18"):
        vd = {"@type": "VD-{}".format(item),
              "validValues": None}
        marc_data = getmarc(record, "024", entity)
        if isinstance(marc_data, dict):
            marc_data = [marc_data]
        if marc_data:
            for indicator_level in marc_data:
                for indicator, subfields in indicator_level.items():
                    if indicator == "7_":
                        sset = {}
                        for subfield in subfields:
                            for k,v in subfield.items():
                                sset[k] = litter(sset.get(k),v)
                        if item in ("17","18"):
                            if sset.get("z") and sset.get("2") and sset["2"] == "vd{}".format(item):
                                vd["invalidValues"] = sset["z"]
                                if isinstance(vd["invalidValues"],str):
                                    vd["invalidValues"] = [vd.pop("invalidValues")]
                        if sset.get("2") and sset["2"] == "vd{}".format(item) and sset.get("a"):
                            vd["validValues"] = sset["a"]
                            if isinstance(vd["validValues"],str):
                                vd["validValues"] = [vd.pop("validValues")]
                            if vd not in data:
                                data.append(vd)

    # Fingerprint Hash
    fp = {"@type": "Fingerprint Hash",
          "validValues": None}
    marc_data = getmarc(record, "026", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("e"):
                    fp["validValues"] = sset["e"]
                    if isinstance(fp["validValues"],str):
                        fp["validValues"] = [fp.pop("validValues")]
                    if fp not in data:
                        data.append(fp)

    # OCLC
    oclc = {"@type": "OCLC",
          "validValues": None}
    marc_data = getmarc(record, "035", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("a") and sset["a"].startswith("(OCoLC)"):
                    oclc["validValues"] = litter(oclc.get("validValues"),sset["a"].split(")")[1])
    if isinstance(oclc["validValues"],str):
        oclc["validValues"] = [oclc.pop("validValues")]
    if oclc not in data:
        data.append(oclc)

    # Bibliographic References
    bibref = {"@type": "Bibliografic References",
                "validValues": None}
    marc_data = getmarc(record, "510", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("a"):
                    bibref["validValues"] = litter(bibref.get("validValues"),sset["a"])
    if isinstance(bibref["validValues"],str):
        bibref["validValues"] = [bibref.pop("validValues")]
    if bibref.get("validValues") and bibref not in data:
        data.append(bibref)

    # CODEN
    coden = {"@type": "CODEN",
              "validValues": None}
    marc_data = getmarc(record, "030", entity)
    if isinstance(marc_data, dict):
        marc_data = [marc_data]
    if marc_data:
        for indicator_level in marc_data:
            for indicator, subfields in indicator_level.items():
                sset = {}
                for subfield in subfields:
                    for k,v in subfield.items():
                        sset[k] = litter(sset.get(k),v)
                if sset.get("a"):
                    coden["validValues"] = sset["a"]
                    if isinstance(coden["validValues"],str):
                        coden["validValues"] = [coden.pop("validValues")]
                    if coden not in data:
                        data.append(coden)

    return data if data else None
