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


def get_facets_normalizer(site_id="eea"):
    return FACETS_SITE_RULES.get(site_id, FACETS_SITE_RULES["eea"])


def get_nlp_preprocessor(site_id="eea"):
    return NLP_SITE_RULES.get(site_id, NLP_SITE_RULES["eea"])
