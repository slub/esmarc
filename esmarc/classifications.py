from esmarc.marc import getmarc


def get_class(record, keys, entity):
    """
    create the classification object out of different MARC21 Fields
    """
    data = []

    def merge_entry(data, entry):
        if not data:
            return [entry]
        else:
            for n,item in enumerate(data):
                if entry["name"] == item["name"]:
                    for sub_item in data[n]["CategoryCodes"]:
                        if isinstance(sub_item, dict):
                            if entry["CategoryCodes"][0]["codeValue"] == sub_item["codeValue"]:
                                return data
                        elif isinstance(sub_item,list):
                            for sub_sub_item in sub_item:
                                if entry["CategoryCodes"][0]["codeValue"] == sub_sub_item["codeValue"]:
                                    return data
                    data[n]["CategoryCodes"].append(entry["CategoryCodes"][0])
                    return data
            data.append(entry)
            return data
        
    for key in keys:
        marc_data = getmarc(record, key, entity)
        if marc_data:
            if isinstance(marc_data, dict):
                marc_data = [marc_data]
            if key == "050":
                entry = {"@type": "CategoryCodeSet",
                         "@id": "https://id.loc.gov/authorities/classification",
                         "name": "Library of Congress Classification",
                         "alternateName": "LCC",
                         "sameAs": "https://wikidata.org/wiki/Q621080",
                         "CategoryCodes": []}
                for item in marc_data:
                    if "_0" in item and "a" in item["_0"][0]:
                        sub_entry = {"@type": "CategoryCode",
                                     "@id": "https://id.loc.gov/authorities/classification/",
                                     "codeValue": None}
                        sub_entry["@id"] += item["_0"][0]['a']
                        sub_entry["codeValue"] = item["_0"][0]['a']
                        entry["CategoryCodes"].append(sub_entry)
                if entry["CategoryCodes"]:
                    data.append(entry)
            if key == "082":
                for item in marc_data:
                    if "0_" in item and "a" in item["0_"][0]:
                        entry = {"@type": "CategoryCodeSet",
                                 "name": "Dewey Decimal Classification",
                                 "alternateName": "DDC",
                                 "sameAs": "http://www.wikidata.org/wiki/Q48460",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                  "codeValue": item["0_"][0]['a']}]}
                        data = merge_entry(data, entry)
                    if "04" in item and "a" in item["04"][0]:
                        entry = {"@type": "CategoryCodeSet",
                                 "name": "DDC-Sachgruppen der DNB ab 2004",
                                 "alternateName": ["Sachgruppen der DNB ab 2004", "SDNB ab 2004", "Systematik der Deutschen Nationalbibliografie ab 2004", "DNB-Sachgruppen ab 2004", "Sachgruppen der Deutschen Nationalbibliografie ab 2004"],
                                 "sameAs": "https://www.wikidata.org/wiki/Q67011877",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "codeValue": item["04"][0]['a']}]}
                        data = merge_entry(data, entry)
            if key == "084":
                for item in marc_data:
                    ind_object = {}
                    for k,v in item.items():
                        ind_object[k] = {}
                        for items in v:
                            for subf, val in items.items():
                                ind_object[k][subf] = val
                    if "__" in ind_object and "a" in ind_object["__"] and ind_object["__"].get("2") == "ssgn":
                        entry = {"@type": "CategoryCodeSet",
                                 "name": "Sondersammelgebiets-Nummer",
                                 "alternateName": ["SSG", "SSGN"],
                                 "sameAs": "https://www.wikidata.org/wiki/Q71786666",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "codeValue": ind_object["__"]['a']}]}
                        data = merge_entry(data, entry)
                    if "__" in ind_object and "a" in ind_object["__"] and ind_object["__"].get("2") == "sdnb":
                        entry = {"@type": "CategoryCodeSet",
                                 "name": "Sachgruppen der DNB bis 2003",
                                 "alternateName": ["SDNB bis 2003", "Systematik der Deutschen Nationalbibliografie bis 2003", "DNB-Sachgruppen bis 2003", "Sachgruppen der Deutschen Nationalbibliografie bis 2003"],
                                 "sameAs": "https://www.wikidata.org/wiki/Q113660734",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "codeValue": ind_object["__"]['a']}]}
                        data = merge_entry(data, entry)
                    if "__" in ind_object and "a" in ind_object["__"] and ind_object["__"].get("2") == "fid":
                        entry = {"@type": "CategoryCodeSet",
                                 "name":  "Kennzeichen der DFG geförderten Fachinformationsdienste für die Wissenschaft",
                                 "alternateName": ["FID", "FID Kennzeichen"],
                                 "sameAs": "http://wikis.sub.uni-hamburg.de/webis/index.php/Webis_-_Sammelschwerpunkte_an_deutschen_Bibliotheken",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "codeValue": ind_object["__"]['a']}]}
                        data = merge_entry(data, entry)
                    if "__" in ind_object and "a" in ind_object["__"] and ind_object["__"].get("2") == "fid":
                        entry = {"@type": "CategoryCodeSet",
                                 "name":  "Kennzeichen der DFG geförderten Fachinformationsdienste für die Wissenschaft",
                                 "alternateName": ["FID", "FID Kennzeichen"],
                                 "sameAs": "http://wikis.sub.uni-hamburg.de/webis/index.php/Webis_-_Sammelschwerpunkte_an_deutschen_Bibliotheken",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "codeValue": ind_object["__"]['a']}]}
                        data = merge_entry(data, entry)
                    if "__" in ind_object and "a" in ind_object["__"] and ind_object["__"].get("2") == "bkl":
                        entry = {"@type": "CategoryCodeSet",
                                 "@id": "http://uri.gbv.de/terminology/bk/",
                                 "name":  "Basisklassifikation",
                                 "alternateName": ["BKL", "BK"],
                                 "sameAs": "https://www.wikidata.org/wiki/Q29938469",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "@id": "http://uri.gbv.de/terminology/bk/{}".format(ind_object["__"]['a']),
                                                    "codeValue": ind_object["__"]['a']}]}
                        data = merge_entry(data, entry)
                    if "__" in ind_object and "a" in ind_object["__"] and ind_object["__"].get("2") == "rvk":
                        entry = {"@type": "CategoryCodeSet",
                                 "@id": "https://rvk.uni-regensburg.de/regensburger-verbundklassifikation-online",
                                 "name":  "Regensburger Verbundklassifikation",
                                 "alternateName": ["RVK", "Regensburger Systematik", "RVKO", "Regensburg RVK", "Regensburg Classification" ],
                                 "sameAs": "http://www.wikidata.org/wiki/Q2137453",
                                 "CategoryCodes": [{"@type": "CategoryCode",
                                                    "@id": "https://rvk.uni-regensburg.de/regensburger-verbundklassifikation-online#notation/{}".format(ind_object["__"]['a']),
                                                    "codeValue": ind_object["__"]['a']}]}
                        data = merge_entry(data, entry)
    return data if data else None
