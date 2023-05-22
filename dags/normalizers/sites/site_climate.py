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
    publication_date = doc["raw_value"].get("publication_date", None)
    cca_published = doc["raw_value"].get("cca_published", None)
    cca_sectors = doc["raw_value"].get("sectors", [])
    cca_impacts = doc["raw_value"].get("climate_impacts", [])
    cca_elements = doc["raw_value"].get("elements", [])
    cca_origin_websites = doc["raw_value"].get("origin_website", [])
    cca_funding_programme = doc["raw_value"].get("funding_programme", None)
    cca_geographic = doc["raw_value"].get("geographic", None)
    ct_normalize_config = config["site"].get("normalize", {})
    logger.info("DATES:")
    logger.info(cca_published)
    logger.info(publication_date)
    _id = doc["raw_value"].get("@id", "")


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

    if normalized_doc.get("issued", None) is None:
        if cca_published is not None:
            normalized_doc["issued"] = cca_published
        else:
            if publication_date is not None:
                normalized_doc["issued"] = publication_date

    normalized_doc["cca_adaptation_sectors"] = [sector['title'] for sector in cca_sectors]
    normalized_doc["cca_climate_impacts"] = [sector['title'] for sector in cca_impacts]
    normalized_doc["cca_adaptation_elements"] = [sector['title'] for sector in cca_elements] if cca_elements else []
    if isinstance(cca_funding_programme, str):
        normalized_doc["cca_funding_programme"] = cca_funding_programme
    else:
        normalized_doc["cca_funding_programme"] = cca_funding_programme['title'] if cca_funding_programme else None
    normalized_doc["cca_origin_websites"] = [cca_origin_website['title'] for cca_origin_website in cca_origin_websites] if cca_origin_websites else []
    if cca_geographic:
        if 'countries' in cca_geographic:
            normalized_doc["cca_geographic_countries"] = [country for country in cca_geographic['countries']]
        if 'transnational_region' in cca_geographic:
            normalized_doc["cca_geographic_transnational_region"] = [country for country in cca_geographic['transnational_region']]
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
