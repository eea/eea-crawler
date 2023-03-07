from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer, add_counts
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)

def add_topic(doc):
    topics = doc.get("raw_value",{}).get("topics", {}) or []
    print("TOPICS")
    print (topics)
    return [topic["title"] for topic in topics]

@register_facets_normalizer("eea_en")
def normalize_eea_europa_eu(doc, config):
    logger.info("NORMALIZE EEA EN")


    if doc["raw_value"].get("@type", None) is None:
        return None
    if doc["raw_value"]["@type"] == "Plone Site":
        return None

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    doc_loc = urlparse(normalized_doc["id"]).path
    doc_loc_parts = doc_loc.strip("/").split("/")
    if "sandbox" in doc_loc_parts:
        return None


    normalized_doc["cluster_name"] = "eea_en"

    normalized_doc["topic"] = add_topic(doc)
    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("eea")
def preprocess_eea_europa_eu(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
