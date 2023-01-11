from esmarc.marc import getmarc
from es2json import isint

def handlesex(record, key, entity):
    """
    return the determined sex (not gender), found in the MARC21 record
    """
    for v in key:
        marcvalue = getmarc(v, record, entity)
        if isinstance(marcvalue, list):
            marcvalue = marcvalue[0]
    if isint(marcvalue):
        marcvalue = int(marcvalue)
    if marcvalue == 0:
        return "Unknown"
    elif marcvalue == 1:
        return "Male"
    elif marcvalue == 2:
        return "Female"
    else:
        return None
