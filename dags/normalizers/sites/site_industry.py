from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    common_normalizer,
    check_blacklist_whitelist,
    find_ct_by_rules,
)
from normalizers.lib.nlp import common_preprocess

import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("industry.eea.europa.eu")
def normalize_industry(doc, config):
    logger.info("NORMALIZE INDUSTRY")
    logger.info(doc["raw_value"]["@id"])
    logger.info(doc["raw_value"]["@type"])
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

    normalized_doc["cluster_name"] = "Industry (industry.eea.europa.eu)"

    doc_loc = urlparse(normalized_doc["id"]).path

    ct = find_ct_by_rules(
        doc_loc,
        ct_normalize_config.get("location_rules", []),
        ct_normalize_config.get("location_rules_fallback", "fallback"),
    )
    logger.info(ct)
    normalized_doc["objectProvides"] = ct

    return normalized_doc


@register_nlp_preprocessor("industry.eea.europa.eu")
def preprocess_industry(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
