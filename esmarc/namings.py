from esmarc.marc import getmarc, get_subsets
from es2json import litter
from esmarc import globals
from esmarc.id import gnd2uri, id2uri
from esmarc.lookup_tables.roles import rolemapping, rolemapping_en


def getName(record, key, entity):
    """
    get the name of the record
    """
    data = getAlternateNames(record, key, entity)
    if isinstance(data, list):
        data = " ".join(data)
    return data if data else None


def getAlternateNames(record, key, entity):
    """
    get the alternateName of the record
    """
    data = getmarc(record, key, entity)
    if isinstance(data, str):
        if data[-2:] == " /":
            data = data[:-2]
    elif isinstance(data, list):
        for n, i in enumerate(data):
            if i[-2:] == " /":
                data[n] = i[:-2]
    return data if data else None


def handle_preferredName_topic(record, key, entity):
    """
    get the preferredName of an Topic
    """
    preferredName = ""
    if record.get(key):
        for indicator_level in record[key]:
            for subfield in indicator_level:
                for sf_elem in indicator_level.get(subfield):
                    for k, v in sf_elem.items():
                        if k == "a":  # is always single valued  https://swbtools.bsz-bw.de/cgi-bin/help.pl?cmd=kat&val=150
                            preferredName = v
                        elif k=="x":  # repeatable
                            preferredName += " / {}".format(v)
                        elif k=="g":  # repeatable
                            preferredName += " ({})".format(v)
    if preferredName:
        return preferredName


def handle_contributor(record, keys, entity):
    retObj = []
    for key in keys:
        for sset in get_subsets(record, key, '*'):
            order = None
            ret = {}
            if (key == "110" and not sset.get("c")) or (key == "710" and not sset.get("t")):
                ret["@type"] = "Organization"
                if sset.get("0"):
                    ret["@id"] = "https://data.slub-dresden.de/organizations/"
                order = ['a','b','g']
            elif (key == "110" and sset.get("c")) or (key == "711" and not sset.get("t")) or (key == "111"):
                ret["@type"] = "Event"
                ret["@id"] = "https://data.slub-dresden.de/events/"
                ret["name"] = ""
                order = ['a','n','d','c','e','g']
            elif (key == "100" or key == "700") and not sset.get("t"):
                ret["@type"] = "Person"
                if sset.get("0"):
                    ret["@id"] = "https://data.slub-dresden.de/persons/"
                ret["name"] = ""
                if sset.get('a') and isinstance(sset['a'],str):
                    ret["name"] += sset['a']
                elif sset.get('a') and isinstance(sset['a'],list):
                    ret["name"] += sset['a'][0]
                if sset.get('b') and isinstance(sset['b'],str):
                    ret["name"] += " " +sset['b']
                if sset.get('c') and isinstance(sset['c'],str):
                    ret["name"] += ", " +sset["c"]
                elif sset.get('c') and isinstance(sset['c'],list):
                    ret["name"] += ", " + ", ".join(sset['c'])
            if order:
                name = ""
                for char in order:
                    if char in sset:
                        if isinstance(sset[char],str):
                            sset[char] = sset.pop(char)
                        name += sset[char][0] + ", "
                ret["name"] = name[:-2]
            if sset.get("0"):
                if isinstance(sset["0"],str):
                    sset["0"] = [sset["0"]]
                if isinstance(sset["0"],list):
                    for item in sset["0"]:
                        if item.startswith("(DE-627)") and ret.get("@id"):
                            ret["@id"] += item.split(")")[1]
                        if item.startswith("(DE-588)"):
                            ret["sameAs"] = "https://d-nb.info/gnd/" + item.split(")")[1]
            if "4" in sset:
                if isinstance(sset["4"],str):
                    sset["4"] = [sset["4"]]
                if isinstance(sset["4"],list):
                    for item in sset["4"]:
                        if item in rolemapping_en:
                            role = {}
                            role["@type"] = "Role"
                            role["@id"] = "https://id.loc.gov/vocabulary/relators/{}".format(item)
                            role["name"] = rolemapping_en[item]
                            if role:
                                if not "roles" in ret:
                                    ret["roles"] = []
                                ret["roles"].append(role)
            retObj.append(ret)
    return retObj if retObj else None


def gettitle(record, keys, entity):
    """
    create the title object out of different MARC21 Fields
    """
    title_obj = {}

    v246_31_a = None
    v246_31_b = None
    # Paralleltitel
    for sset in get_subsets(record, "246", '31'):
        p_tit = {}
        if sset.get('a'):
            p_tit["mainTitle"] = sset['a']
            v246_31_a = sset['a']
        if sset.get('b'):
            p_tit["subTitle"] = sset['b']
            v246_31_b = sset['b']
        if sset.get('a') and sset.get('b'):
            p_tit["preferredName"] = "{} : {}".format(sset['a'],sset['b'])
        if p_tit:
            title_obj["parallelTitles"] = litter(title_obj.get("parallelTitles"), p_tit)

    # Hauptsachtitel:
    for sset in get_subsets(record, "245",'*'):
        title_obj["preferredName"] = ""
        if sset.get('a'):
            title_obj["preferredName"] += sset['a']
            title_obj["mainTitle"] = sset['a']
        if sset.get('b'):
            if v246_31_a and v246_31_a in sset['b']:
                sset['b'] = sset.pop('b').replace(' = {}'.format(v246_31_a), "")
            if v246_31_b and v246_31_b in sset['b']:
                sset['b'] = sset.pop('b').replace(' : {}'.format(v246_31_b), "")
            title_obj["preferredName"] += " : {}".format(sset['b'])
            title_obj["subTitle"] = sset['b']
        if sset.get('n'):
            title_obj["partStatement"] = []
            if isinstance(sset['n'], str):
                sset['n'] = [sset.pop('n')]
            title_obj["partStatement"] = sset['n']
        if sset.get('p'):
            if isinstance(sset['p'],str):
                sset['p'] = [sset.pop('p')]
            if not title_obj.get("partStatement"):
                title_obj["partStatement"] = []
                for item in sset["p"]:
                    title_obj["partStatement"].append("")
            else:
                while len(title_obj["partStatement"]) < len(sset["p"]):
                    title_obj["partStatement"].append("")
            for n, item in enumerate(sset['p']):
                title_obj["partStatement"][n] += " {}".format(item)
        if title_obj.get("partStatement"):
            for item in title_obj["partStatement"]:
                title_obj["preferredName"] += ". "
                title_obj["preferredName"] += item
        if sset.get('c'):
            title_obj["preferredName"] += " / {}".format(sset['c'])
            title_obj["responsibilityStatement"] = sset['c']


    # Zusammenstellungen
    for sset in get_subsets(record, "249",'*'):
        o_part_tit = {}
        if sset.get('a'):
            o_part_tit["mainTitle"] = sset["a"]
        if sset.get('b'):
            o_part_tit["subTitle"] = sset["b"]
        if sset.get('v'):
            if not o_part_tit.get("responsibilityStatement"):
                o_part_tit["responsibilityStatement"] = sset["v"]
        if sset.get('c'):
            if not o_part_tit.get("responsibilityStatement"):
                o_part_tit["responsibilityStatement"] = sset["c"]
            else:
                o_part_tit["responsibilityStatement"] += ", {}".format(sset["c"])
        if sset.get('a'):
            if isinstance(sset['a'], list):
                o_part_tit["preferredName"] = "{}".format(" ; ".join(sset["a"]))
            else:
                o_part_tit["preferredName"] = sset['a']
        if sset.get('v'):
            o_part_tit["preferredName"] += " / {}".format(sset["v"])
        if o_part_tit:
            title_obj["otherPartsTitle"] = litter(title_obj.get("otherPartsTitle"), o_part_tit)

    # Beigefügte Werke
    addInfo = {}
    for sset in get_subsets(record, "501",'__'):
        if sset.get('a'):
            addInfo["notes"] = sset['a']
    for sset in get_subsets(record, "505",'80'):
        e_part_tit = {}
        if sset.get('a'):
            addInfo["notes"] = litter(addInfo.get("notes"), sset['a'])
        if sset.get('t'):
            e_part_tit["preferredName"] = sset['t']
        if sset.get('r'):
            e_part_tit["contributor"] = sset['r']
        if sset.get('g'):
            e_part_tit["note"] = sset['g']
        if e_part_tit:
            addInfo["enclosedParts"] = litter(addInfo.get("enclosedParts"), e_part_tit)
    if addInfo:
        title_obj["additionalInfo"] = addInfo

    # Zeitschriftenkurztitel
    for sset in get_subsets(record, "210",'10'):
        if sset.get('a'):
            title_obj["shortTitle"] = sset['a']
    var_titles = []
    for sset in get_subsets(record, "246",'1_'):
        var_title = {}
        if sset.get('a'):
            var_title["preferredName"] = sset["a"]
        if sset.get('i'):
            var_title["disambiguatingDescription"] = sset["i"]
        if var_title:
            var_titles = litter(var_titles, var_title)
    for sset in get_subsets(record, "246",'33'):
        var_title = {}
        if sset.get('a'):
            var_title = litter(var_title, sset['a'])
        if var_title:
            var_titles = litter(var_titles, {"preferredName": var_title})
    if var_titles:
        title_obj["varyingTitles"] = var_titles
    formerTitles = []
    for sset in get_subsets(record, "247",'10'):
        formerTitle = {}
        if sset.get('a'):
            formerTitle["preferredName"] = sset["a"]
        if sset.get('f'):
            formerTitle["disambiguatingDescription"] = sset["f"]
        if formerTitle:
            formerTitles.append(formerTitle)
    if formerTitles:
        title_obj["formerTitles"] = formerTitles

    # Werktitel
    uniformTitles = []
    uni_keys = ["130", "240", "700", "710", "711", "730"]
    for key in uni_keys:
        for sset in get_subsets(record, key, '*'):
            uniformTitle = {}
            if sset.get('a') and key in ["130", "240", "730"]:
                uniformTitle["preferredName"] = sset["a"]
            if sset.get('t') and key in ["700", "710", "711"]:
                uniformTitle["preferredName"] = sset["t"]
            if not uniformTitle.get("preferredName"):
                continue
            if sset.get('0'):
                uniformTitle["sameAs"] = gnd2uri(sset["0"])
                for n, sameAs in enumerate(uniformTitle["sameAs"]):
                    if not sameAs:
                        del uniformTitle["sameAs"][n]
                if isinstance(uniformTitle["sameAs"], str):
                    uniformTitle["sameAs"] = [uniformTitle.pop("sameAs")]
                if uniformTitle["sameAs"]:
                    for sameAs in uniformTitle["sameAs"]:
                        if isinstance(sameAs, str) and sameAs.startswith(globals.base_id):
                            uniformTitle["@id"] = id2uri(sameAs.split(globals.base_id)[1], "works", globals.base_id)
            if uniformTitle:
                uniformTitles.append(uniformTitle)
    if uniformTitles:
        title_obj["uniformTitles"] = uniformTitles

    if title_obj:
        return title_obj
