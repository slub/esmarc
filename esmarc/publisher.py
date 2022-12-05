from esmarc.marc import getmarc

def getpublisher(record, key, entity):
    """
    produces a Publisher-node out of two different MARC21-Fields
    """
    pub_name = getmarc(record, ["260..b", "264..b"], entity)
    pub_place = getmarc(record, ["260..a", "264..a"], entity)
    if pub_name or pub_place:
        data = {}
        if pub_name:
            if pub_name[-1] in [".", ",", ";", ":"]:
                data["name"] = pub_name[:-1].strip()
            else:
                data["name"] = pub_name
            data["@type"] = "Organization"
        if pub_place:
            if pub_place[-1] in [".", ",", ";", ":"] and isinstance(pub_place, str):
                data["location"] = {"name": pub_place[:-1].strip(),
                                    "type": "Place"}
            else:
                data["location"] = {"name": pub_place,
                                    "type": "Place"}
        return data
