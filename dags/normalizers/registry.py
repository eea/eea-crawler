from urllib.parse import urlparse

FACETS_SITE_RULES = {}
NLP_SITE_RULES = {}


def register_facets_normalizer(name):
    def wrapper(wrapped):

        FACETS_SITE_RULES[name] = wrapped
        return wrapped

    return wrapper


def register_nlp_preprocessor(name):
    def wrapper(wrapped):

        NLP_SITE_RULES[name] = wrapped
        return wrapped

    return wrapper


from normalizers.sites import *


def get_facets_normalizer(url):
    name = urlparse(url).netloc
    return FACETS_SITE_RULES.get(name, FACETS_SITE_RULES["default"])


def get_nlp_preprocessor(url):
    name = urlparse(url).netloc
    return NLP_SITE_RULES.get(name, NLP_SITE_RULES["default"])
