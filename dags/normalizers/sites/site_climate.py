from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    common_normalizer,
    check_blacklist_whitelist,
)
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("climate-adapt.eea.europa.eu")
def normalize_climate(doc, config):
    logger.info("NORMALIZE CLIMATE")
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

    normalized_doc = common_normalizer(doc, config)

    normalized_doc[
        "cluster_name"
    ] = "Climate-adapt (climate-adapt.eea.europa.eu)"
    normalized_doc["topic"] = "Climate change adaptation"

    return normalized_doc


@register_nlp_preprocessor("climate-adapt.eea.europa.eu")
def preprocess_climate(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
