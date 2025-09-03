import logging

from normalizers.lib.nlp import common_preprocess
from normalizers.lib.normalizers import (add_counts, check_blacklist_whitelist,
                                         common_normalizer, check_readingTime, apply_norm_obj)
from normalizers.registry import (register_facets_normalizer,
                                  register_nlp_preprocessor)

# from datetime import date  # , timedelta
# from urllib.parse import urlparse


logger = logging.getLogger(__file__)


def vocab_to_list(vocab, attr="title"):
    return [term[attr] for term in vocab] if vocab else []


def vocab_to_term(term):
    return term['title'] if term else None


@register_facets_normalizer("fise_resource")
def normalize_fise(doc, config):
    logger.info("NORMALIZE FISE")
    logger.info(f"RS: {doc['raw_value'].get('review_state')}")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))
    logger.info(doc["raw_value"].get("resource_type", ""))
    logger.info(doc)

    portal_type = doc["raw_value"].get("resource_type", "")
    publication_year = doc["raw_value"].get("publishing_year", None)
    uid = doc["raw_value"].get("UID", None)
    created = doc["raw_value"].get("created", None)
    keywords = doc["raw_value"].get("subjects", [])
    # countries = doc["raw_value"].get("country", [])

    ct_normalize_config = config["site"].get("normalize", {})

    logger.info("DATES:")
    logger.info(created)
    logger.info(publication_year)

    _id = doc["raw_value"].get("@id", "")

    if not check_blacklist_whitelist(
        doc,
        ct_normalize_config.get("blacklist", []),
        ct_normalize_config.get("whitelist", []),
    ):
        logger.info("blacklisted")
        return None

    logger.info("whitelisted")

    doc_out = common_normalizer(doc, config)
    logger.info("DOC OUT")
    logger.info(doc_out)

    if not doc_out:
        return None

    raw_geo_coverage = doc["raw_value"].get(
        "geo_coverage", {}).get("geolocation", [])

    doc_out["country"] = [geolocation.get(
        'label') for geolocation in raw_geo_coverage]
    logger.info("country")
    logger.info(doc_out["country"])

    doc_out["uid"] = uid
    doc_out["created"] = created

    doc_out["cluster_name"] = "fise_sdi"
    doc_out["objectProvides"] = [portal_type]
    doc_out["keywords"] = keywords

    # FORCED VALUES
    doc_out["update_frequency_value"] = 'As needed'
    doc_out = check_readingTime(doc_out, config)

    doc_out = apply_norm_obj(doc_out, config.get(
        "normalizers", {}).get("normObj", {}))
    doc_out = add_counts(doc_out)
    logger.info(doc_out)
    logger.info("NORMALIZE FISE END")
    return doc_out


@register_nlp_preprocessor("fise_resource")
def preprocess_fise(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
