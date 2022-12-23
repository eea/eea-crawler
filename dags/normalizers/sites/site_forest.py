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


@register_facets_normalizer("fise")
def normalize_forest(doc, config):
    logger.info("NORMALIZE FISE")
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

    if doc["raw_value"]["@type"] in [
        "basic_data_factsheet",
        "european_union_factsheet",
        "country_biodiversity_factsheet",
        "country_bioeconomy_factsheet",
        "country_climate_factsheet",
        "country_vitality_factsheet",
    ]:
        if doc["raw_value"]["parent"]["title"] != "Regions":
            doc["raw_value"]["spatial"] = doc["raw_value"]["parent"]["title"]

    doc["raw_value"]["themes"] = ["biodiversity"]
    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "fise"

    doc_loc = urlparse(normalized_doc["id"]).path
    if normalized_doc["objectProvides"] == "Webpage":
        ct = find_ct_by_rules(
            doc_loc,
            ct_normalize_config.get("location_rules", []),
            ct_normalize_config.get("location_rules_fallback", "fallback"),
        )
        if len(ct) == 1 and ct[0] == "Webpage":
            logger.info("HERE2")
            if doc_loc.strip("/").find("topics") == 0:
                ct = ["Webpage", "Topic page"]
                if normalized_doc.get("places", "unknown") == "unknown":
                    normalized_doc["places"] = "EU27"

        normalized_doc["objectProvides"] = ct
    if normalized_doc["objectProvides"] == "Country fact sheet":
        if doc_loc.strip("/").find("countries/regions/european-union") == 0:
            normalized_doc["objectProvides"] = ["Dashboard"]
        else:
            normalized_doc["objectProvides"] = [
                "Country fact sheet",
                "Dashboard",
            ]

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("fise")
def preprocess_forest(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
