from esmarc.classifications import get_mentions
from esmarc.esmarc import entities
import json

def dumpstr(dic,ind=None):
    # Function to dump the results/expectiations thru, with sort_keys=True the keys get sorted in the Dict, in the end we compare the strings
    return json.dumps(dic,sort_keys=True,indent=ind)


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
        for lod_field, mapping in entities[entity].items():
            if test_field in lod_field:
                for func, param in mapping.items():
                    function = func
                    keys = param
                break
        if not keys:
            assert False
        processed = dumpstr({test_field: function(raw, keys, entity)})
        if not processed == expected:
            print(dumpstr(json.loads(expected),ind=4))
            print(dumpstr(json.loads(processed),ind=4))
        assert processed == expected


def test_mentions():
    run_func("mentions",["1131213920","1503793168","024629014"],"resources")
