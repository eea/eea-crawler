from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    common_normalizer,
    check_blacklist_whitelist,
    add_counts,
    check_readingTime,
)
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("ias")
def normalize_ias(doc, config):
    logger.info("NORMALIZE IAS")
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

    doc["raw_value"]["themes"] = ["biodiversity"]

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    doc_loc = urlparse(normalized_doc["id"]).path
    if (
        doc_loc.strip("/").split("/")[0] == "products"
        and doc_loc.strip("/").split("/")[1] == "european-statistics"
    ):
        if not normalized_doc.get("title", "").startswith("Map"):
            normalized_doc["objectProvides"] = "Dashboard"
        else:
            normalized_doc["objectProvides"] = "Map (interactive)"

    normalized_doc["cluster_name"] = "ias"

    normalized_doc = check_readingTime(normalized_doc, config)

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("ias")
def preprocess_ias(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
