from es2json import litter, ArrayOrSingleValue, isint
import dateparser
import datetime
from esmarc.marc import getmarc


def startDate(jline, key, entity):
    """
    calls marc_dates with the correct key (start) for a date-mapping
    produces an date-Object for the startDate-field
    """
    extra_key = ""
    if "^" in key:
        key_split = key.split("^")
        key = key_split[0]
        if "," in key_split[1]:
            extra_key = key_split[1].split(",")
        else:
            extra_key = key_split[1]
    return marc_dates(jline.get(key), entity, "startDate", extra_key)


def endDate(jline, key, entity):
    """
    calls marc_dates with the correct key (end) for a date-mapping
    produces an date-object for the endDate field

    """
    datekey_list = ""
    if "^" in key:
        key_split = key.split("^")
        key = key_split[0]
        if "," in key_split[1]:
            datekey_list = key_split[1].split(",")
        else:
            datekey_list = key_split[1]
    return marc_dates(jline.get(key), entity, "endDate", datekey_list)


def marc_dates(record, entity, event, datekey_list):
    """
    builds the date nodes based on the data which is sanitzed by dateToEvent, gets called by the deathDate/birthDate functions
    """
    dates = []
    if record:
        for indicator_level in record:
            for subfield in indicator_level:
                sset = {}
                for sf_elem in indicator_level.get(subfield):
                    for k, v in sf_elem.items():
                        if k == "a" or k == "4" or k == 'i':
                            sset[k] = litter(sset.get(k), ArrayOrSingleValue(v))
                if '4' in sset and sset['4'] in datekey_list:
                    dates.append(sset)
    if dates:
        exact_date_index = 0
        for n, date in enumerate(dates):
            if "exakt" in date['i'].lower():
                exact_date_index = n
            else:
                exact_date_index = 0
        if dates and dates[exact_date_index]['4'] in datekey_list:
            ret = {"@value": dateToEvent(dates[exact_date_index]['a'], event), "disambiguatingDescription": dates[exact_date_index]['i'], "description": dates[exact_date_index]['a']}
            if ret.get("@value"):
                return ret
            elif ret.get("description"):
                ret.pop("@value")
                if "-" in ret["description"]:
                    if event == "startDate" and ret["description"].split("-")[0]:
                        return ret
                    elif event == "endDate" and ret["description"].split("-")[1]:
                        return ret
    return None


def dateToEvent(date, schemakey):
    """
    return birthDate and deathDate schema.org attributes
    don't return deathDate if the person is still alive according to the data
    (determined if e.g. the date looks like "1979-")
    """
    date = ArrayOrSingleValue(date)
    if not date:
        return None
    if isinstance(date, list):
        ret = []
        for item in date:
            dateItem = dateToEvent(item, schemakey)
            if dateItem:
                ret.append(dateItem)
    if "[" in date and "]" in date:
        date = date.split("[")[1]
        date = date.split("]")[0]
    ddp = dateparser.date.DateDataParser()
    parsedDate = None
    strf_string = None
    ddp_obj = None
    if '-' in date:
        dates = date.split('-')
        if schemakey == "startDate":  # (start date)
            ddp_obj = ddp.get_date_data(dates[0])
            parsedDate = ddp_obj.date_obj
        elif schemakey == "endDate":  # (end Date)
            if len(dates) == 2 and dates[1]:
                ddp_obj = ddp.get_date_data(dates[1])
                parsedDate = ddp_obj.date_obj
            elif len(dates) == 1:
                return None  # still alive! congrats
    else:
        date = date.lower()
        ddp_obj = ddp.get_date_data(date)
        parsedDate = ddp_obj.date_obj
        # check if its not a date from the future and if the year has four digits
    if parsedDate and int(parsedDate.strftime("%Y")) < int(datetime.datetime.today().strftime("%Y")) and len(parsedDate.strftime("%Y")) == 4:
        strf_string = None
        if ddp_obj.period == "year":
            strf_string = "%Y"
        elif ddp_obj.period == "month":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "day":
            strf_string = "%Y-%m-%d"
        elif ddp_obj.period == "week":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "time":
            strf_string = "%Y-%m-%d"
        return parsedDate.strftime(strf_string)


def datePublished(jline, key, entity):
    """
    reads different MARC21 Fields to determine when the entity behind this record got published
    """
    fivethreethree = getmarc(jline, "533.__.d", entity)
    twosixfour = getmarc(jline, "264.*.c", entity)
    fivethreefour = getmarc(jline, "534.__.c", entity)
    zerozeroeight = getmarc(jline, "008", entity)
    if fivethreethree:
        return handle_260(fivethreethree)
    elif not fivethreethree and twosixfour:
        return handle_260(twosixfour)
    if not fivethreethree and not twosixfour and fivethreefour:
        return handle_260(zerozeroeight[7:11])


def dateOriginalPublished(jline, key, entity):
    """
    reads different MARC21 Fields to determine when the entity behind this record got published originally
    """
    fivethreethree = getmarc(jline, "533.__.d", entity)
    twosixfour = getmarc(jline, "264.*.c", entity)
    fivethreefour = getmarc(jline, "534.__.c", entity)
    if fivethreethree:
        return handle_260(twosixfour)
    if fivethreefour:
        return handle_260(fivethreefour)


def parseDate(toParsedDate):
    """
    use scrapehubs dateParser to get an Python dateobject out of pure MARC21-Rubbish
    """
    if isinstance(toParsedDate, list):
        toParsedDate = toParsedDate[0]
    if "[" in toParsedDate and "]" in toParsedDate:
        toParsedDate = toParsedDate.split("[")[1]
        toParsedDate = toParsedDate.split("]")[0]
    ddp = dateparser.date.DateDataParser()
    ddp_obj = ddp.get_date_data(toParsedDate.lower())
    parsedDate = ddp_obj.date_obj
    if parsedDate and int(parsedDate.strftime("%Y")) < int(datetime.datetime.today().strftime("%Y")) and len(parsedDate.strftime("%Y")) == 4:
        strf_string = None
        if ddp_obj.period == "year":
            strf_string = "%Y"
        elif ddp_obj.period == "month":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "day":
            strf_string = "%Y-%m-%d"
        elif ddp_obj.period == "week":
            strf_string = "%Y-%m"
        elif ddp_obj.period == "time":
            strf_string = "%Y-%m-%d"
        return parsedDate.strftime(strf_string)


def handle_260(date):
    """
    parse the 264/260 field to a machine-readable format
    """
    if isinstance(date, list):
        ret = []
        for item in date:
            dateItem = handle_260(item)
            if dateItem:
                ret.append(dateItem)
        return ArrayOrSingleValue(ret)
    if not date:
        return None
    retObj = {"dateOrigin": date}
    if "-" in date:
        dateSplitField = date.split("-")
        if dateSplitField[0]:
            dateParsedEarliest = parseDate(dateSplitField[0])
            if dateParsedEarliest:
                retObj["dateParsedEarliest"] = dateParsedEarliest
        if dateSplitField[1]:
            dateParsedLatest = parseDate(dateSplitField[1])
            if dateParsedLatest:
                retObj["dateParsedLatest"] = dateParsedLatest
    else:
        parsedDate = parseDate(date)
        if parsedDate:
            retObj["dateParsed"] = parsedDate
    return retObj if retObj["dateOrigin"] else None


def getdateModified(record, key, entity):
    """
    get the DateModified field from the MARC21-Record,
    date of the last modification of the MARC21-Record
    """
    date = getmarc(record, key, entity)
    newdate = ""
    if date:
        for i in range(0, 13, 2):
            if isint(date[i:i+2]):
                newdate += date[i:i+2]
            else:
                newdate += "00"
            if i in (2, 4):
                newdate += "-"
            elif i == 6:
                newdate += "T"
            elif i in (8, 10):
                newdate += ":"
            elif i == 12:
                newdate += "Z"
        return newdate


def handle_dateCreated(record, key, entity):
    """
    get the dateCreated field from the MARC21-Record
    """
    date = getmarc(record,key, entity)
    YY = int(date[0:2])
    MM = int(date[2:4])
    DD = int(date[4:6])
    ## check if Year is a 19XX record
    if YY > int(datetime.datetime.now().date().strftime("%y")):
        return "19{:02d}-{:02d}-{:02d}".format(YY,MM,DD)
    else:
        return "20{:02d}-{:02d}-{:02d}".format(YY,MM,DD)
