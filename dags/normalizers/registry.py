from urllib.parse import urlparse
from tasks.helpers import find_site_by_url
from lib.variables import get_variable


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


def get_name(url, site_map=None, variables={}, site_id=""):
    site = find_site_by_url(url, site_map, variables)
    if not site:
        return site_id

    sites = get_variable("Sites", variables)
    site_config = get_variable(sites[site], variables)
    name = site_config["url"].split("://")[-1].strip("/")
    return name


def get_facets_normalizer(url, site_map=None, variables={}, site_id=""):
    name = get_name(url, site_map, variables, site_id)
    return FACETS_SITE_RULES.get(name, FACETS_SITE_RULES["www.eea.europa.eu"])


def get_nlp_preprocessor(url, site_map=None, variables={}, site_id=""):
    name = get_name(url, site_map, variables, site_id)
    return NLP_SITE_RULES.get(name, NLP_SITE_RULES["www.eea.europa.eu"])
