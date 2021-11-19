from urllib.parse import urlparse
from tasks.helpers import find_site_by_url
from airflow.models import Variable


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


def get_name(url):
    site = find_site_by_url(url)
    sites = Variable.get("Sites", deserialize_json=True)
    site_config = Variable.get(sites[site], deserialize_json=True)
    name = site_config["url"].split("://")[-1].strip("/")
    return name


def get_facets_normalizer(url):

    name = get_name(url)
    return FACETS_SITE_RULES.get(name, FACETS_SITE_RULES["www.eea.europa.eu"])


def get_nlp_preprocessor(url):
    name = get_name(url)
    return NLP_SITE_RULES.get(name, NLP_SITE_RULES["www.eea.europa.eu"])
