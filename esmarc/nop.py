from esmarc.marc import getmarc
from es2json import isint


def getnumberofpages(record, regex, entity):
    """
    get the number of pages and sanitizes the field into an atomar integer
    """
    nop = getmarc(record, regex, entity)
    try:
        if isinstance(nop, str):
            nop = [nop]
        if isinstance(nop, list):
            for number in nop:
                if "S." in number and isint(number.split('S.')[0].strip()):
                    nop = int(number.split('S.')[0])
                else:
                    nop = None
    except IndexError:
        pass
    except Exception as e:
        with open("errors.txt", "a") as err:
            print(e, file=err)
    return nop
