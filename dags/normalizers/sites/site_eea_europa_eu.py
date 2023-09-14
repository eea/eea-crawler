from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer, add_counts
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("eea")
def normalize_eea_europa_eu(doc, config):
    logger.info("NORMALIZE EEA")
    if doc["raw_value"].get("@type", None) is None:
        return None
    if doc["raw_value"]["@type"] == "Plone Site":
        return None
    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None
    if normalized_doc["language"] == 'en' and doc["raw_value"].get("@type", None) == 'helpcenter_faq':
        return None

    if doc["raw_value"].get("@type", None) == 'Term':
        normalized_doc['term_description'] = doc["raw_value"].get('description', None)
        normalized_doc['term_source'] = doc["raw_value"].get('source', None)
    normalized_doc["cluster_name"] = "eea"

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("eea")
def preprocess_eea_europa_eu(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
