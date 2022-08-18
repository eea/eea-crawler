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
)
from normalizers.lib.nlp import common_preprocess

import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("industry")
def normalize_industry(doc, config):
    logger.info("NORMALIZE INDUSTRY")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))
    ct_normalize_config = config["site"].get("normalize", {})

    if not check_blacklist_whitelist(
        doc,
        ct_normalize_config.get("blacklist", []),
        ct_normalize_config.get("whitelist", []),
    ):
        logger.info("blacklisted")
        return None
    logger.info("whitelisted")

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "industry"

    doc_loc = urlparse(normalized_doc["id"]).path

    ct = find_ct_by_rules(
        doc_loc,
        ct_normalize_config.get("location_rules", []),
        ct_normalize_config.get("location_rules_fallback", "fallback"),
    )
    logger.info(ct)
    normalized_doc["objectProvides"] = ct
    normalized_doc["topic"] = "Industry"

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("industry")
def preprocess_industry(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
