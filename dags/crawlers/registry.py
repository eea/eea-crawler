from urllib.parse import urlparse

SITE_CRAWLERS = {}
DOC_CRAWLERS = {}

def register_site_crawler(name):
    def wrapper(wrapped):
        SITE_CRAWLERS[name] = wrapped
        return wrapped

    return wrapper

def register_doc_crawler(name):
    def wrapper(wrapped):

        DOC_CRAWLERS[name] = wrapped
        return wrapped

    return wrapper

from crawlers.crawlers import *

def get_site_crawler(type):
    return SITE_CRAWLERS.get(type, SITE_CRAWLERS["sdi"])


def get_doc_crawler(type):
    return DOC_CRAWLERS.get(type, DOC_CRAWLERS["sdi"])

