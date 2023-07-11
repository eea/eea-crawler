from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    common_normalizer,
    check_blacklist_whitelist,
    find_ct_by_rules,
    add_counts,
    get_page_title
)
from normalizers.lib.nlp import common_preprocess

import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("discomap")
def normalize_industry(doc, config):
    logger.info("NORMALIZE DISCOMAP")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))
    ct_normalize_config = config["site"].get("normalize", {})

    doc["raw_value"]["@type"] = "Page"
    doc["raw_value"]["review_state"] = "published"
    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc['title'] = get_page_title(doc)
    normalized_doc["cluster_name"] = "discomap"



    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("discomap")
def preprocess_industry(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
