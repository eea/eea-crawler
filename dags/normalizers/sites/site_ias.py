from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer
from normalizers.lib.nlp import common_preprocess


@register_facets_normalizer("ias.eea.europa.eu")
def normalize_ias(doc, config):
    normalized_doc = common_normalizer(doc, config)

    normalized_doc["cluster_name"] = "IAS (ias.eea.europa.eu)"

    return normalized_doc


@register_nlp_preprocessor("ias.eea.europa.eu")
def preprocess_ias(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
