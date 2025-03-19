from urllib.parse import urlparse
import trafilatura
import re

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    common_normalizer,
    check_blacklist_whitelist,
    find_ct_by_rules,
    add_counts,
    check_readingTime,
)
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


@register_facets_normalizer("wise_freshwater")
def normalize_freshwater(doc, config):
    logger.info("NORMALIZE FRESHWATER")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))
    # logger.info(doc)
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
    if "Measure" in normalized_doc["objectProvides"] or "Source" in normalized_doc["objectProvides"] or "Case study" in normalized_doc["objectProvides"] or "chemical" in normalized_doc["objectProvides"]:
        print(
            "exclude_from_globalsearch by objectProvides in [Measure, Source, Case study, chemical]")
        normalized_doc['exclude_from_globalsearch'] = ['True']
    print("OBJECT PROVIDES")
    print(normalized_doc["objectProvides"])


#    if type(doc["raw_value"].get("biophysical_impacts", {}).get("value")) is list:
    dbi = doc["raw_value"].get("biophysical_impacts", {}) or {}
    if type(dbi.get("value")) is list:
        normalized_doc['biophysical_impacts'] = [
            val.get('name') for val in doc["raw_value"]["biophysical_impacts"]["value"]]
        normalized_doc['biophysical_impacts'] = normalized_bep(
            normalized_doc['biophysical_impacts'], 'BP')
        print("biophysical_impacts end: {}".format(
            normalized_doc['biophysical_impacts']))

#    if type(doc["raw_value"].get("ecosystem_services", {}).get("value")) is list:
    des = doc["raw_value"].get("ecosystem_services", {}) or {}
    if type(des.get("value")) is list:
        normalized_doc['ecosystem_services'] = [
            val.get('name') for val in doc["raw_value"]["ecosystem_services"]["value"]]
        normalized_doc['ecosystem_services'] = normalized_bep(
            normalized_doc['ecosystem_services'], 'ES')
        print("ecosystem_services end: {}".format(
            normalized_doc['ecosystem_services']))

#    if type(doc["raw_value"].get("policy_objectives", {}).get("value")) is list:
    dpo = doc["raw_value"].get("policy_objectives", {}) or {}
    if type(dpo.get("value")) is list:
        normalized_doc['policy_objectives'] = [
            val.get('name') for val in doc["raw_value"]["policy_objectives"]["value"]]
        normalized_doc['policy_objectives'] = normalized_bep(
            normalized_doc['policy_objectives'], 'PO')
        print("policy_objectives end: {}".format(
            normalized_doc['policy_objectives']))
    lr = doc["raw_value"].get("legislative_reference")
    if type(lr) is list:
        if len(lr) > 0:
            if type(lr[0]) is str:
                normalized_doc['legislative_reference'] = lr
            else:
                normalized_doc['legislative_reference'] = [
                    val.get('title') for val in lr]
    if type(doc["raw_value"].get("category")) is list:
        normalized_doc['category'] = doc["raw_value"].get("category")
    normalized_doc['measure_sector'] = doc["raw_value"].get("measure_sector")

    normalized_doc["cluster_name"] = "wise-freshwater"

    normalized_doc["wise_country"] = normalized_doc.get("country")
    if normalized_doc.get('country'):
        del (normalized_doc["country"])
    normalized_doc = check_readingTime(normalized_doc, config)
    normalized_doc = add_counts(normalized_doc)
    print("MS pre:")
    print(doc.get("raw_value", {}).get('measure_summary', {}))
    if 'Measure' in normalized_doc["objectProvides"] and doc.get("raw_value", {}).get('measure_summary', {}).get('data', None):
        normalized_doc['description'] = trafilatura.extract(
            doc["raw_value"]['measure_summary']['data'] + "<p></p>")
    print("MS post:")
    print(normalized_doc['description'])
    return normalized_doc


def normalized_bep(data, prefix):
    # biophysical_impacts
    # ecosystem_services
    # policy_objectives
    if type(data) is not list:
        return data
    if len(prefix) < 1:
        return data
    response = []
    pattern = prefix+"\d+\s-\s(.+)"
    for item in data:
        if isinstance(item, str):
            match = re.fullmatch(pattern, item)
            if match:
                response.append(match.group(1).strip())
            else:
                response.append(item)
        else:
            response.append(item)
    return response


@register_nlp_preprocessor("wise_freshwater")
def preprocess_freshwater(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
