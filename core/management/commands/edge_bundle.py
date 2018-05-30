"""
Adding Twitter posts to index in ES.
"""
import sys
import os
import optparse
import json

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../'))

from analytics.semantic import get_graph


def main(*args, **kwargs):
    if not args:
        raise Exception("Term is required!")

    term = args[0].strip()
    graph = get_graph(term)
    filename = term + ".json"
    try:
        with open(filename, "w") as fp:
            json.dump(graph, fp, indent=4)
            fp.close()
    except Exception as err:
        print(err)
    else:
        print("[.] Done: {}".format(filename))


if __name__ == '__main__':
    cmdparser = optparse.OptionParser(usage="usage: python %prog term")
    opts, args = cmdparser.parse_args()
    main(*args, **opts.__dict__)
