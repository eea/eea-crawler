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


@register_facets_normalizer("wise_freshwater")
def normalize_freshwater(doc, config):
    logger.info("NORMALIZE FRESHWATER")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))
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

    if doc["raw_value"]["@type"] == "country_profile":
        doc["raw_value"]["spatial"] = doc["raw_value"]["title"]

    doc["raw_value"]["themes"] = ["water"]

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

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
    if "Data set" in normalized_doc["objectProvides"]:
        if len(normalized_doc["objectProvides"]) == 1:
            normalized_doc["objectProvides"] = ["Webpage"]
        else:
            normalized_doc["objectProvides"].remove("Webpage")
    print("OBJECT PROVIDES")
    print(normalized_doc["objectProvides"])
    normalized_doc["cluster_name"] = "wise-freshwater"

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("wise_freshwater")
def preprocess_freshwater(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
