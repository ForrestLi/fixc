from typing import Dict
import re
import datetime as dt

def search_from_file(inf, string):
    with open(inf, 'r') as f:
        if string in f.read():
            return True
    return False


def fix_time_now(fmt='%Y%m%d-%H:%M:%S.%f', *, us=False):
    t = dt.datetime.now().strftime(fmt)
    if not us:
        t = t[:-3]  # millisec instead of microsec
    return t.encode()


def iter_rawmsg(fixmsg, *, delim=b'\x01'):
    for t, _, v in (x.group(0).partition(b'=')
                    for x in re.finditer(b'([^\\' + delim + b']+)', fixmsg)):
        yield t, v


def validate_d(d: Dict):
    assert all(isinstance(k, int) and isinstance(v, bytes)
               for k, v in d.items())


def ch_delim(msg, delim=b'\x01', new_delim=b'^'):
    """
    change fix messages delimiter
    """
    return re.sub(delim, new_delim, msg)
