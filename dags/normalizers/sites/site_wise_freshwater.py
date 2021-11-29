from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer
from normalizers.lib.nlp import common_preprocess


@register_facets_normalizer("water.europa.eu/freshwater")
def normalize_industry(doc, config):
    normalized_doc = common_normalizer(doc, config)

    normalized_doc[
        "cluster_name"
    ] = "WISE Freshwater (water.europa.eu/freshwater)"

    return normalized_doc


@register_nlp_preprocessor("water.europa.eu/freshwater")
def preprocess_industry(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
