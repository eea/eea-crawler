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
    get_page_title,
    check_readingTime,

)
from normalizers.lib.nlp import common_preprocess

import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("land")
def normalize_copernicus_land(doc, config):
    logger.info("NORMALIZE LAND")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "copernicus_land"

    #normalized_doc['title'] = get_page_title(doc)

    normalized_doc = check_readingTime(normalized_doc, config)

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("land")
def preprocess_copernicus_land(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
