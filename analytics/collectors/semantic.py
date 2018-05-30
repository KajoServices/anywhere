import json
from dataman.elastic import search, termvectors


def get_children(term):
    children = []
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "tokens": term
                        }
                    },
                    {
                        "range": {
                            "flood_probability": {
                                "gt": 0.6,
                                "lte": 1.
                            }
                        }
                    }
                ]
            }
        }
    }
    response = search(query)
    terms = [term]
    for x in response["hits"]["hits"]:
        resp = termvectors(
            x['_id'],
            fields=['tokens'],
            field_statistics=False,
            term_statistics=True,
            offsets=False,
            positions=False
            )
        for name, stat in resp['term_vectors']['tokens']['terms'].items():
            # Control repeated items.
            if name in terms:
                continue
            terms.append(name)

            children.append({
                "name": name,
                "size": stat['ttf']
                })
    return sorted(children, key=lambda x: x["size"], reverse=True)


def get_graph(term):
    children = get_children(term)
    for child in children:
        child["children"] = get_children(child["name"])

    return children
