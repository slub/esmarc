from esmarc.marc import getmarcvalues
from esmarc.lookup_tables.language import language_iso_label_lookups, language_k10plus_to_iso_lookups


def get_language(record, key, entity):
    """
    get the language of the bibliographic record and make an RDF Object out of it using lookups
    """
    marc_data = list(getmarcvalues(record, key, entity))
    lang_data = []
    for lang_code in marc_data:
        if language_k10plus_to_iso_lookups.get(lang_code):
            lcode = language_k10plus_to_iso_lookups[lang_code]
        else:
            lcode = lang_code
        if lcode in language_iso_label_lookups:
            lang_object = {"@type": "CategoryCode",
                           "@id": "http://id.loc.gov/vocabulary/iso639-2/{}".format(lcode),
                           "codeValue": lcode,
                           "name": {
                            "en": language_iso_label_lookups[lcode]["en"],
                            "de": language_iso_label_lookups[lcode]["de"]
                            },
                           "inCodeSet": "http://id.loc.gov/vocabulary/iso639-2"
                           }
            if lang_object not in lang_data:
                lang_data.append(lang_object)
    # Special case for 'language'
    if key[-1] == 'a' and not lang_data:
        return {
            "@type": "CategoryCode",
            "@id":"http://id.loc.gov/vocabulary/iso639-2/und",
            "codeValue": "und",
            "name": {
                "en": "Undetermined",
                "de": "Nicht zu entscheiden"         },
            "inCodeSet": "http://id.loc.gov/vocabulary/iso639-2"
            }
    return lang_data if lang_data else None

