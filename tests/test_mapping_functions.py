from esmarc.classifications import get_mentions
from esmarc.esmarc import entities
import json

def dumpstr(dic,ind=None):
    # Function to dump the results/expectiations thru, with sort_keys=True the keys get sorted in the Dict, in the end we compare the strings
    return json.dumps(dic,sort_keys=True,indent=ind)


import difflib

red = lambda text: f"\033[38;2;255;0;0m{text}\033[38;2;255;255;255m"
green = lambda text: f"\033[38;2;0;255;0m{text}\033[38;2;255;255;255m"
blue = lambda text: f"\033[38;2;0;0;255m{text}\033[38;2;255;255;255m"
white = lambda text: f"\033[38;2;255;255;255m{text}\033[38;2;255;255;255m"

def get_edits_string(old, new):
    result = ""
    codes = difflib.SequenceMatcher(a=old, b=new).get_opcodes()
    for code in codes:
        if code[0] == "equal": 
            result += white(old[code[1]:code[2]])
        elif code[0] == "delete":
            result += red(old[code[1]:code[2]])
        elif code[0] == "insert":
            result += green(new[code[3]:code[4]])
        elif code[0] == "replace":
            result += (red(old[code[1]:code[2]]) + green(new[code[3]:code[4]]))
    return result


def run_func(test_field, ids,entity):
    for record_id in ids:
        raw = None
        processed = None
        expected = None
        keys = None
        function = None
        with open("in/{}".format(record_id)) as inp:
            raw = json.load(inp)
        with open("out/{}/{}".format(test_field,record_id)) as inp:
            expected = dumpstr(json.load(inp))
        for lod_field_descriptor, mapping in entities[entity].items():
            lod_field = lod_field_descriptor.split(":")[1]
            if test_field == lod_field:
                for func, param in mapping.items():
                    function = func
                    keys = param
                break
        if not keys:
            assert False
        processed = dumpstr({test_field: function(raw, keys, entity)})
        if not processed == expected:
            print(get_edits_string(dumpstr(json.loads(expected),ind=4),dumpstr(json.loads(processed),ind=4)))
            #print(dumpstr(json.loads(expected),ind=4))
            #print(dumpstr(json.loads(processed),ind=4))
        assert processed == expected


def test_mentions():
    run_func("mentions",["1131213920","1503793168","024629014","218401159"],"resources")


def test_publisher():
    run_func("publisher",["1405783028","1384819908","1191100251"],"resources")


def test_orgpublisher():
    run_func("originalPublisher",["1405783028","1384819908"],"resources")


def test_location():
    run_func("location",["1405783028","1384819908","1191100251"],"resources")


def test_cartographicData():
    run_func("cartographicData",["71691803X"],"resources")

