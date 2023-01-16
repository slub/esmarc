from esmarc.marc import getmarc


def geteditionStatement(record, key, entity):
    """
    gets the information about the edition statements out of field 250$ab
    """
    a = getmarc(record, "250..a", entity)
    b = getmarc(record, "250..b", entity)
    if a and b:
        return "{}, {}".format(a,b)


def geteditionSequence(record, key, entity):
    """
    gets the information about the edition sequence out of field 362$a
    """
    if key in record:
        for indicator_level in record[key]:
            if "0_" in indicator_level:
                for item in indicator_level["0_"]:
                    if "a" in item:
                        return item["a"]


def get_reproductionSeriesStatement(record, key, entity):
    """
    get the reproductionSeriesStatement
    """
    data = getmarc(record, key, entity)
    return {"name": data} if data else None
