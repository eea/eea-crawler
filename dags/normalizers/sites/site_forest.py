from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer
from normalizers.lib.nlp import common_preprocess


@register_facets_normalizer("forest.eea.europa.eu")
def normalize_forest(doc, config):
    normalized_doc = common_normalizer(doc, config)

    normalized_doc["cluster_name"] = "FISE (forest.eea.europa.eu)"

    return normalized_doc


@register_nlp_preprocessor("forest.eea.europa.eu")
def preprocess_forest(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
