#!/usr/bin/env python

# get all documents from sdi and
import json

print("PACKAGE")
print("----------------------------------------------------------------")
print(__package__)

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from lib import variables
    from lib import rabbitmq
else:
    from ..lib import variables
    from ..lib import rabbitmq


from elasticsearch import Elasticsearch
from crawlers.registry import get_site_crawler, get_doc_crawler


def send_to_rabbitmq(v, raw_doc):
    rabbitmq_config = v.get("rabbitmq")
    rabbitmq.send_to_rabbitmq(raw_doc, rabbitmq_config)


def crawl(site, app):
    v = variables.load_variables_from_disk("../variables.json", app)
    sites = v.get("Sites")
    site_config_variable = sites.get(site, None)
    site_config = v.get(site_config_variable)
    crawl_type = site_config.get("type")
    parse_all_documents = get_site_crawler(crawl_type)
    crawl_doc = get_doc_crawler(crawl_type)
    parse_all_documents(v, site, site_config, crawl_doc, send_to_rabbitmq)


if __name__ == "__main__":
    crawl("sdi")
