#!/usr/bin/env python

from crawl import crawl

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
    from lib import variables
    from lib import elastic
else:
    from ..lib import variables
    from ..lib import elastic


def sync():
    v = variables.load_variables_from_disk('../variables.json')
    elastic.create_raw_index(v)
    es = elastic.elastic_connection(v)
    elastic_conf = v.get("elastic")
    elastic.backup_indices(es, [elastic_conf['raw_index']])

    crawl("climate")

if __name__ == "__main__":
    sync()


"""
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "metadataIdentifier": "43ba4d57-6290-43e4-941e-6c4eab713eda"
          }
        }
      ]
    }
  },
  "_source": {
    "excludes": [
      "*.data"
    ]
  }
}
"""