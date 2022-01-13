from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer, add_counts
from normalizers.lib.nlp import common_preprocess


@register_facets_normalizer("www.eea.europa.eu")
def normalize_eea_europa_eu(doc, config):
    normalized_doc = common_normalizer(doc, config)

    normalized_doc["cluster_name"] = "eea"

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("www.eea.europa.eu")
def preprocess_eea_europa_eu(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
