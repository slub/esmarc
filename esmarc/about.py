from es2json import litter
from esmarc.id import gnd2uri
from esmarc.marc import getmarc
from esmarc.subfields import get_subfield


def handle_about(jline, key, entity):
    """
    produces schema.org/about nodes based on RVK, DDC and GND subjects
    """
    ret = []
    for k in key:
        if k == "936" or k == "084":
            data = getmarc(jline, k, None)
            if isinstance(data, list):
                for elem in data:
                    ret.append(handle_single_rvk(elem))
            elif isinstance(data, dict):
                ret.append(handle_single_rvk(data))
        elif k == "082" or k == "083":
            data = getmarc(jline, k+"..a", None)
            if isinstance(data, list):
                for elem in data:
                    if isinstance(elem, str):
                        ret.append(handle_single_ddc(elem))
                    elif isinstance(elem, list):
                        for final_ddc in elem:
                            ret.append(handle_single_ddc(final_ddc))
            elif isinstance(data, dict):
                ret.append(handle_single_ddc(data))
            elif isinstance(data, str):
                ret.append(handle_single_ddc(data))
        elif k == "655":
            data = get_subfield(jline, k, entity)
            ret.append(data)
    if len(ret) > 0:
        return ret
    else:
        return None


def handle_single_ddc(data):
    """
    produces a about node based on DDC
    """
    return {"identifier": {"@type": "PropertyValue",
                           "propertyID": "DDC",
                           "value": data},
            "@id": "http://purl.org/NET/decimalised#c"+data[:3]}


def handle_single_rvk(data):
    """
    produces a about node based on RVK
    """
    sset = {}
    record = {}
    if "rv" in data:
        for subfield in data.get("rv"):
            for k, v in subfield.items():
                sset[k] = litter(sset.get(k), v)
        if "0" in sset and isinstance(sset["0"], str):
            sset["0"] = [sset.get("0")]
        if "0" in sset and isinstance(sset["0"], list):
            record["sameAs"] = []
            for elem in sset["0"]:
                if isinstance(elem, str):
                    sameAs = gnd2uri(elem)
                    if sameAs:
                        record["sameAs"].append(sameAs)
        if "a" in sset:
            record["@id"] = "https://rvk.uni-regensburg.de/api/json/ancestors/" + \
                sset.get("a").replace(" ", "%20")
            record["identifier"] = {"@type": "PropertyValue",
                                    "propertyID": "RVK",
                                    "value": sset.get("a")}
        if "b" in sset:
            record["name"] = sset.get("b")
        if "k" in sset:
            record["keywords"] = sset.get("k")
        return record
