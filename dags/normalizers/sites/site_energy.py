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


@register_facets_normalizer("climate-energy.eea.europa.eu")
def normalize_energy(doc, config):
    logger.info("NORMALIZE ENERGY")
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

    if doc["raw_value"]["@type"] == "File":
        if doc["raw_value"]["file"]["content-type"] != "application/pdf":
            logger.info("file, but not pdf")
            return None

    normalized_doc = common_normalizer(doc, config)

    logger.info("CHECK LOCATION:")
    doc_loc = urlparse(normalized_doc["id"]).path
    logger.info(doc_loc)
    ct = find_ct_by_rules(
        doc_loc,
        ct_normalize_config.get("location_rules", []),
        ct_normalize_config.get("location_rules_fallback", "Webpage"),
    )
    if ct[0] == "Country fact sheet":
        normalized_doc["spatial"] = doc["raw_value"]["title"]

    if doc["raw_value"].get("resource_type", {}).get("token", "") == "Data":
        ct = ["Dashboard"]

    if (
        doc_loc.strip("/").split("/")[0] == "topics"
        and doc_loc.strip("/").split("/")[-1] == "intro"
    ):
        ct = ["Topic page", "Webpage"]
    logger.info(ct)
    normalized_doc["objectProvides"] = ct

    normalized_doc["cluster_name"] = "Energy (climate-energy.eea.europa.eu)"
    normalized_doc["topic"] = "Energy"

    return normalized_doc


@register_nlp_preprocessor("climate-energy.eea.europa.eu")
def preprocess_energy(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
