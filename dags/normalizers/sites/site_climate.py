from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    common_normalizer,
    check_blacklist_whitelist,
    add_counts,
)
from normalizers.lib.nlp import common_preprocess
import logging
from datetime import date, timedelta

logger = logging.getLogger(__file__)


@register_facets_normalizer("climate")
def normalize_climate(doc, config):
    logger.info("NORMALIZE CLIMATE")
    logger.info(f"RS: {doc['raw_value'].get('review_state')}")
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

    doc["raw_value"]["themes"] = ["climate-change-adaptation"]
    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "cca"

    # if doc["raw_value"].get("review_state") == "archived":
    #     # raise Exception("review_state")
    #     expires = date.today() - timedelta(days=2)
    #     normalized_doc["expires"] = expires.isoformat()
    #     logger.info("RS EXPIRES")

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("climate")
def preprocess_climate(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
