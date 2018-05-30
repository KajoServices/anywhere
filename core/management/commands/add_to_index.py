"""
Adding Twitter posts to index in ES.
"""
import sys
import os
import optparse
import json

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../'))

from dataman.normalizer import TweetNormalizer
from dataman.elastic import ensure_mapping, create_or_update_index


def add_file_to_index(filename, **kwargs):
    """
    Add contents of a JSON file (exported tweets).

    :filename: str
    """
    with open(filename, 'r') as f:
        data = json.load(f)
        f.close()

    startfrom = kwargs.get('startfrom', 0)
    n_records = kwargs.get('n_records', None)
    if n_records is None:
        n_records = len(data) - startfrom
    if n_records <= 0:
        raise Exception("`Number of records to process` (n_records) must be positive integer > 0!")

    ensure_mapping()

    for rec in data[startfrom: startfrom+n_records]:
        norm = TweetNormalizer(rec)
        doc = norm.normalize()
        try:
            res = create_or_update_index(rec['tweetid'], doc)
        except Exception as err:
            print('ERROR: %s (%s)' % (err, rec['tweetid']))
        else:
            print('%s: %s' % (rec['tweetid'], res))


def main(*args, **kwargs):
    if not args:
        raise Exception("Filename is required!")

    add_file_to_index(args[0], **kwargs)


if __name__ == '__main__':
    cmdparser = optparse.OptionParser(usage="usage: python %prog [OPTIONS] filename")
    cmdparser.add_option("-s", "--startfrom",
                         action="store",
                         dest="startfrom",
                         default=0,
                         type=int,
                         help="Index to start from [default \'%default\']")
    cmdparser.add_option("-n", "--n_records",
                         action="store",
                         dest="n_records",
                         type=int,
                         help="Number of records to process.")
    opts, args = cmdparser.parse_args()
    main(*args, **opts.__dict__)
