from esmarc.marc import getmarc
from esmarc.helperfunc import traverse

def getgeo(arr):
    """
    sanitzes geo-coordinate information from raw MARC21 values
    """
    for k, v in traverse(arr, ""):
        if isinstance(v, str):
            if '.' in v:
                return v


def getGeoCoordinates(record, key, entity):
    """
    get the geographic coordinates of an entity from the corresponding MARC21 authority Record
    """
    ret = {}
    for k, v in key.items():
        coord = getgeo(getmarc(record, v, entity))
        if coord:
            ret["@type"] = "GeoCoordinates"
            ret[k] = coord.replace("N", "").replace(
                "S", "-").replace("E", "").replace("W", "-")
    if ret:
        return ret


def get_cartData(record, key, entity):
    """
    get the cartographic data out of the marc record
    """
    scale = getmarc(record, "255..a", entity)
    projection = getmarc(record, "255..b", entity)
    coordinates = getmarc(record, "255..c", entity)

    data = {}
    if scale:
        data["scale"] = scale
    if projection:
        data["projection"] = projection
    if coordinates:
        data["coordinates"] = coordinates
    if data:
        return data
