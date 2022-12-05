from rdflib import URIRef
import traceback
from multiprocessing import Pool, current_process
import json
import gzip
from es2json import ESGenerator, IDFile, eprint

from esmarc import globals
from esmarc.processing import process_line
from esmarc.marc import getmarc
from esmarc.gender import handlesex
from esmarc.id import getid, getisil, handle_identifier, get_identifiedby
from esmarc.nop import getnumberofpages
from esmarc.genre import getgenre
from esmarc.subfields import get_subfield, get_subfield_if_4
from esmarc.about import handle_about
from esmarc.sameAs import getsameAs
from esmarc.relatedTo import relatedTo, get_ispartof, get_partseries, get_relations, get_seriesStatement
from esmarc.dates import handle_dateCreated, getdateModified, datePublished, dateOriginalPublished, startDate, endDate
from esmarc.coordinates import getGeoCoordinates, get_cartData
from esmarc.helperfunc import get_source_include_str, setupoutput
from esmarc.namings import getName, handle_preferredName_topic, handle_contributor, gettitle
from esmarc.catalogue import getav_katalog, get_accessmode, get_physical, get_collection
from esmarc.publisher import getpublisher
from esmarc.classifications import  get_class
from esmarc.language import get_language
from esmarc.footnotes import get_footnotes
from esmarc.editions import get_reproductionSeriesStatement, geteditionSequence, geteditionStatement


"""Mapping:
 a dict() (json) like table with function pointers
 entitites={"entity_types:{"single_or_multi:target":"string",
                           "single_or_multi:target":{function:"source"},
                           "single_or_multi:target:function}
                           }
 
"""

entities = {
    "resources": {   # mapping is 1:1 like works
        "single:@type": [URIRef(u'http://schema.org/CreativeWork')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "multi:identifier": {handle_identifier: "024"},
        "single:offers": {getav_katalog: ["924..b", "001"]},
        "single:_isil": {getisil: ["003", "852..a", "924..b"]},
        "single:_ppn": {getmarc: "001"},
        "single:_sourceID": {getmarc: "980..b"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["016", "035..a"]},
        "single:title": {gettitle: ["130", "210", "240", "245", "246", "247", "249", "501", "505", "700", "710", "711", "730"]},
        "single:preferredName": {getName: ["245..a", "245..b"]},
        "multi:contributor":  {handle_contributor: ["100", "110", "111", "700", "710", "711"]},
        "single:publisher": {getpublisher: ["260..a""260..b", "264..a", "264..b"]},
        "single:datePublished": {datePublished: ["008", "533", "534", "264"]},
        "single:dateOriginalPublished": {dateOriginalPublished: ["008", "533", "534", "264"]},
        "single:Thesis": {getmarc: ["502..a", "502..b", "502..c", "502..d"]},
        "multi:genre": {getgenre: "655..a"},
        "single:license": {getmarc: "540..a"},
        "single:numberOfPages": {getnumberofpages: ["300..a", "300..b", "300..c", "300..d", "300..e", "300..f", "300..g"]},
        "single:pageStart": {getmarc: "773..q"},
        "single:issueNumber": {getmarc: "773..l"},
        "single:volumeNumer": {getmarc: "773..v"},
        "multi:locationCreated": {get_subfield_if_4: "551^4:orth"},
        "multi:relatedTo": {relatedTo: "500..0"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "multi:description": {getmarc: ["520..a"]},
        "multi:mentions": {get_subfield: "689"},
        "multi:relatedEvent": {get_subfield: "711"},
        "single:physical_description": {get_physical: ["300","533"]},
        "multi:collection": {get_collection: ["084..a","935..a"]},
        "single:editionStatement": {geteditionStatement: "250"},
        "single:reproductionType": {getmarc: "533..a"},
        "single:editionSequence": {geteditionSequence: "362"},
        "single:cartographicData": {get_cartData: "255"},
        "multi:additionalInfo": {get_footnotes: ["242", "385", "500", "502", "508", "511", "515", "518", "521", "533", "535", "538", "546", "555", "561", "563", "937"]},
        "multi:classifications": {get_class: ["050", "082", "084"]},
        "single:accessMode": {get_accessmode: "007"},
        "multi:identifiedBy": {get_identifiedby: ["015", "020", "022", "024", "026", "028", "030", "035", "088", "510", "770", "772", "773", "775", "776", "780", "785", "787", "800", "810", "811", "811", "830"]},
        "multi:language": {get_language: "041..a"},
        "multi:originalLanguage": {get_language: "041..h"},
        "multi:seriesStatement": {get_seriesStatement: "490"},
        "multi:partOfSeries": {get_partseries: ["776", "800", "810", "811", "830"]},
        "multi:isPartOf": {get_ispartof: ["245", "773"]},
        "multi:reproductionSeriesStatement": {get_reproductionSeriesStatement: "533..f"},
        "multi:relations":  {get_relations: ["770", "772", "775", "776", "780", "785", "787"]}
        },
    "works": {
        "single:@type": [URIRef(u'http://schema.org/CreativeWork')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "single:preferredName": {getName: ["100..t", "110..t", "130..t", "111..t", "130..a"]},
        "single:alternativeHeadline": {getmarc: ["245..c"]},
        "multi:alternateName": {getmarc: ["400..t", "410..t", "411..t", "430..t", "240..a", "240..p", "246..a", "246..b", "245..p", "249..a", "249..b", "730..a", "730..p", "740..a", "740..p", "920..t"]},
        "multi:author": {get_subfield: "500"},
        "multi:contributor": {get_subfield: "700"},
        "single:publisher": {getpublisher: ["260..a""260..b", "264..a", "264..b"]},
        "single:datePublished": {getmarc: ["130..f", "260..c", "264..c", "362..a"]},
        "single:Thesis": {getmarc: ["502..a", "502..b", "502..c", "502..d"]},
        "multi:issn": {getmarc: ["022..a", "022..y", "022..z", "029..a", "490..x", "730..x", "773..x", "776..x", "780..x", "785..x", "800..x", "810..x", "811..x", "830..x"]},
        "multi:isbn": {getmarc: ["020..a", "022..a", "022..z", "776..z", "780..z", "785..z"]},
        "single:genre": {getmarc: "655..a"},
        "single:hasPart": {getmarc: "773..g"},
        "single:isPartOf": {getmarc: ["773..t", "773..s", "773..a"]},
        "single:license": {getmarc: "540..a"},
        "multi:inLanguage": {getmarc: ["377..a", "041..a", "041..d", "130..l", "730..l"]},
        "single:numberOfPages": {getnumberofpages: ["300..a", "300..b", "300..c", "300..d", "300..e", "300..f", "300..g"]},
        "single:pageStart": {getmarc: "773..q"},
        "single:issueNumber": {getmarc: "773..l"},
        "single:volumeNumer": {getmarc: "773..v"},
        "single:locationCreated": {get_subfield_if_4: "551^orth"},
        "multi:relatedTo": {relatedTo: "500"},
        "single:dateOfEstablishment": {startDate: "548^datb,dats"},
        "single:dateOfTermination": {endDate: "548^datb,dats"}
    },
    "persons": {
        "single:@type": [URIRef(u'http://schema.org/Person')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: "100..a"},
        "single:gender": {handlesex: "375..a"},
        "multi:alternateName": {getmarc: ["400..a", "400..c"]},
        "multi:relatedTo": {relatedTo: "500..0"},
        "multi:hasOccupation": {get_subfield: "550"},
        "single:birthPlace": {get_subfield_if_4: "551^ortg"},
        "single:deathPlace": {get_subfield_if_4: "551^orts"},
        "single:workLocation": {get_subfield_if_4: "551^ortw"},
        "multi:honorificPrefix": [{get_subfield_if_4: "550^adel"}, {get_subfield_if_4: "550^akad"}],
        "single:birthDate": {startDate: "548"},
        "single:deathDate": {endDate: "548"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:periodOfActivityStart": {startDate: "548^datw,datz"},
        "single:periodOfActivityEnd": {endDate: "548^datw,datz"},
        "single:birthDate": {startDate: "548^datl,datx"},
        "single:deathDate": {endDate: "548^datl,datx"},
    },
    "organizations": {
        "single:@type": [URIRef(u'http://schema.org/Organization')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: "110..a+b"},
        "multi:alternateName": {getmarc: "410..a+b"},

        "single:additionalType": {get_subfield_if_4: "550^obin"},
        "single:parentOrganization": {get_subfield_if_4: "551^adue"},
        "single:location": {get_subfield_if_4: "551^orta"},
        "single:fromLocation": {get_subfield_if_4: "551^geoa"},
        "single:areaServed": {get_subfield_if_4: "551^geow"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:dateOfEstablishment": {startDate: "548^datb"},
        "single:dateOfTermination": {endDate: "548^datb"}
    },
    "geo": {
        "single:@type": [URIRef(u'http://schema.org/Place')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: "151..a"},
        "multi:alternateName": {getmarc: "451..a"},
        "single:description": {get_subfield: "551"},
        "single:geo": {getGeoCoordinates: {"longitude": ["034..d", "034..e"], "latitude": ["034..f", "034..g"]}},
        "single:adressRegion": {getmarc: "043..c"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:dateOfEstablishment": {startDate: "548^datb,dats"},
        "single:dateOfTermination": {endDate: "548^datb,dats"}
    },
    "topics": {
        "single:@type": [URIRef(u'http://schema.org/Thing')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "single:preferredName": {handle_preferredName_topic: "150"},
        "multi:alternateName": {getmarc: "450..a+x"},
        "single:description": {getmarc: "679..a"},
        "multi:additionalType": {get_subfield: "550"},
        "multi:location": {get_subfield_if_4: "551^orta"},
        "multi:fromLocation": {get_subfield_if_4: "551^geoa"},
        "multi:areaServed": {get_subfield_if_4: "551^geow"},
        "multi:contentLocation": {get_subfield_if_4: "551^punk"},
        "multi:participant": {get_subfield_if_4: "551^bete"},
        "multi:relatedTo": {get_subfield_if_4: "551^vbal"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "single:dateOfEstablishment": {startDate: "548^datb"},
        "single:dateOfTermination": {endDate: "548^datb"}
    },
    "events": {
        "single:@type": [URIRef(u'http://schema.org/Event')],
        "single:@context": "https://raw.githubusercontent.com/slub/esmarc/master/conf/context.jsonld",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:_ppn": {getmarc: "001"},
        "single:dateModified": {getdateModified: "005"},
        "single:dateCreated": {handle_dateCreated: ["008"]},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:preferredName": {getName: ["111..a"]},
        "multi:alternateName": {getmarc: ["411..a"]},
        "single:location": {get_subfield_if_4: "551^ortv"},
        "single:startDate": {startDate: "548^datv"},
        "single:endDate": {endDate: "548^datv"},
        "single:adressRegion": {getmarc: "043..c"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
}


def main(elastic=None,
         _index="",
         _type="_doc",
         _id=None,
         z=False,
         prefix="ldj/",
         debug=False,
         w=8,
         idfile=None,
         query={},
         _base_id_src="http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
         _target_id="https://data.slub-dresden.de/"):
    """
    main function which can be called by other programs
    """
    globals.initialize()
    globals.base_id = _base_id_src
    globals.target_id = _target_id
    if elastic and _index and (_id or debug):
        init_mp(prefix, z)
        with ESGenerator(es=elastic, index=_index, type_=_type, includes=get_source_include_str(entities), body=query, id_=_id, headless=True) as es2json_obj:
            for ldj in es2json_obj.generator():
                record = process_line(ldj, _index)
                if record:
                    for k in record:
                        print(json.dumps(record[k]))
    elif elastic and _index and not _id:
        setupoutput(entities, prefix)
        pool = Pool(w, initializer=init_mp, initargs=(prefix, z))
        if idfile:
            es2json_obj = IDFile(es=elastic, index=_index, type_=_type, includes=get_source_include_str(entities), body=query, idfile=idfile)
        else:
            es2json_obj = ESGenerator(es=elastic, index=_index, type_=_type, includes=get_source_include_str(entities), body=query)
        for ldj in es2json_obj.generator():
            pool.apply_async(worker, args=(ldj, _index,))
        pool.close()
        pool.join()
    else:  # oh noes, no elasticsearch input-setup. exiting.
        eprint("No -host:port/-index or -server specified, exiting\n")
        exit(-1)


def init_mp(pr, z):
    """
    initialize the multiprocessing environment for every worker
    """
    global prefix
    global comp
    if not pr:
        prefix = ""
    elif pr[-1] != "/":
        prefix = pr+"/"
    else:
        prefix = pr
    comp = z


def worker(ldj, index):
    """
    worker function for multiprocessing
    """
    try:
        if isinstance(ldj, dict):
            ldj = [ldj]
        if isinstance(ldj, list):    # list of records
            for source_record in ldj:
                target_record = process_line(source_record.pop(
                    "_source"), index, entities)
                if target_record:
                    for entity in target_record:
                        name = prefix+entity+"/" + \
                            str(current_process().name)+"-records.ldj"
                        if comp:
                            opener = gzip.open
                            name += ".gz"
                        else:
                            opener = open
                        with opener(name, "at") as out:
                            print(json.dumps(
                                target_record[entity], indent=None), file=out)
    except Exception:
        with open("errors.txt", 'a') as f:
            traceback.print_exc(file=f)
