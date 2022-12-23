from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    update_from_theme_taxonomy,
    common_normalizer,
    check_blacklist_whitelist,
    add_counts,
)
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("eionet")
def normalize_eionet(doc, config):
    logger.info("NORMALIZE EIONET")
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

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "etc"

    doc_loc = urlparse(normalized_doc["id"]).path
    doc_loc_parts = doc_loc.strip("/").split("/")
    if doc_loc_parts[0] == "etcs" and len(doc_loc_parts) > 1:
        if doc_loc_parts[1] == "etc-atni":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["air"]

        if doc_loc_parts[1] == "etc-bd":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["biodiversity"]

        if doc_loc_parts[1] == "etc-cca":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["climate-change-adaptation"]

        if doc_loc_parts[1] == "etc-cme":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["climate", "energy"]

        if doc_loc_parts[1] == "etc-icm":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["water", "coast_sea"]

        if doc_loc_parts[1] == "etc-uls":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["landuse", "soil"]

        if doc_loc_parts[1] == "etc-wmge":
            normalized_doc["cluster_name"] = doc_loc_parts[1]
            normalized_doc["topic"] = ["waste"]

    normalized_doc["topic"] = update_from_theme_taxonomy(
        normalized_doc["topic"],
        config.get("full_config", {}).get("theme_taxonomy", {}),
    )

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("eionet")
def preprocess_eionet(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
