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


@register_facets_normalizer("water.europa.eu/freshwater")
def normalize_industry(doc, config):
    logger.info("NORMALIZE FRESHWATER")
    logger.info(doc["raw_value"]["@id"])
    logger.info(doc["raw_value"]["@type"])
    logger.info(doc)
    ct_normalize_config = config["site"].get("normalize", {})

    if not check_blacklist_whitelist(
        doc,
        ct_normalize_config.get("blacklist", []),
        ct_normalize_config.get("whitelist", []),
    ):
        logger.info("blacklisted")
        return None
    logger.info("whitelisted")

    if doc["raw_value"]["@type"] == "File":
        if doc["raw_value"]["file"]["content-type"] != "application/pdf":
            logger.info("file, but not pdf")
            return None
    if doc["raw_value"]["@type"] == "country_profile":
        doc["raw_value"]["spatial"] = doc["raw_value"]["title"]
    normalized_doc = common_normalizer(doc, config)

    logger.info("TYPES:")
    logger.info(normalized_doc["objectProvides"])
    if normalized_doc["objectProvides"] == "Webpage":
        logger.info("CHECK LOCATION:")
        doc_loc = urlparse(normalized_doc["id"]).path
        logger.info(doc_loc)
        ct = find_ct_by_rules(
            doc_loc,
            ct_normalize_config.get("location_rules", []),
            ct_normalize_config.get("location_rules_fallback", "Webpage"),
        )
        logger.info(ct)
        normalized_doc["objectProvides"] = ct

    normalized_doc[
        "cluster_name"
    ] = "WISE Freshwater (water.europa.eu/freshwater)"

    return normalized_doc


@register_nlp_preprocessor("water.europa.eu/freshwater")
def preprocess_industry(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
