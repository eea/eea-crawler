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
    get_page_title,
    check_readingTime,
)
from normalizers.lib.nlp import common_preprocess

import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("discomap")
def normalize_industry(doc, config):
    logger.info("NORMALIZE DISCOMAP")
    logger.info(doc)
    logger.info(doc["raw_value"].get("id", ""))
    loc = doc["raw_value"].get("id", "")
    logger.info(doc["raw_value"].get("@type", ""))
    ct_normalize_config = config["site"].get("normalize", {})

    doc["raw_value"]["@type"] = "Page"
    doc["raw_value"]["review_state"] = "published"
    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc['title'] = get_page_title(doc)
    normalized_doc["cluster_name"] = "discomap"

    normalized_doc["issued"] = doc["raw_value"].get("modified")

    if loc.startswith("https://discomap.eea.europa.eu/climatechange"):
        normalized_doc["topic"] = ["Climate change adaptation", "Climate change mitigation"]

        if loc.strip("/") in ["https://discomap.eea.europa.eu/climatechange/?page=Home", "https://discomap.eea.europa.eu/climatechange"]:
            normalized_doc["objectProvides"] = ["Webpage"]
        else:
            normalized_doc["objectProvides"] = ["Map (interactive)"]

    if loc.startswith("https://discomap.eea.europa.eu/atlas"):
        normalized_doc["topic"] = ["Environmental health impacts"]
        if loc.strip("/") in ["https://discomap.eea.europa.eu/atlas/?page=Learn-more", "https://discomap.eea.europa.eu/atlas/?page=Home", "https://discomap.eea.europa.eu/atlas"]:
            normalized_doc["objectProvides"] = ["Webpage"]
        else:
            normalized_doc["objectProvides"] = ["Map (interactive)"]
    normalized_doc = check_readingTime(normalized_doc, config)

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("discomap")
def preprocess_industry(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
