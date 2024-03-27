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
    check_readingTime
)
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


wm_spm_extra_fields = ["title",
    "sector",
    "code",
    "use",
    "origin",
    "nature",
    "status",
    "impacts",
    "impacts_further_details",
    "water_body_cat",
    "spatial_scope",
    "country_coverage",
    "measure_purpose",
    "measure_type",
    "measure_location",
    "measure_response",
    "measure_additional_info",
    "pressure_type",
    "pressure_name",
    "ranking",
    "season",
    "approaching_areas",
    "areas_to_be_avoided",
    "descriptors",
    "ecological_impacts",
    "future_scenarios",
    "effect_on_hydromorphology",
    "ktms_it_links_to",
    "links_to_existing_policies",
    "msfd_spatial_scope",
    "mspd_implementation_status",
    "nature_of_physical_modification",
    "source",
    "authority",
    "general_view",
    "ports",
    "future_expectations",
    "safety_manner",
    "objective",
    "categories",
    "precautionary_areas",
    "priority_areas",
    "relevant_targets",
    "relevant_features_from_msfd_annex_iii",
    "region",
    "shipping_tackled",
    "traffic_separation_scheme",
    "type_of_pressure"]

@register_facets_normalizer("wise_marine")
def normalize_marine(doc, config):
    logger.info("NORMALIZE MARINE")
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

    if doc["raw_value"]["@type"] == "country_factsheet":
        doc["raw_value"]["spatial"] = doc["raw_value"]["title"]

    doc["raw_value"]["themes"] = ["water", "coast_sea"]

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    logger.info("TYPES:")
    logger.info(normalized_doc["objectProvides"])
    if (
        normalized_doc["objectProvides"] == "Webpage"
        or normalized_doc["objectProvides"] == "Country fact sheet"
    ):
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
    if "Shipping and Ports Measure" in normalized_doc["objectProvides"]:
        normalized_doc['exclude_from_globalsearch'] = ['True']
        if normalized_doc.get("issued") is None and normalized_doc.get("hasWorkflowState") == "published":
            normalized_doc["issued"] = "2023-09-04T07:17:00"
        for extra_field in wm_spm_extra_fields:
            normalized_doc[f'wm_spm_{extra_field}'] = doc["raw_value"].get(extra_field)

    if type(doc["raw_value"].get("legislative_reference")) is list:
        normalized_doc['legislative_reference'] = [val.get('title') for val in doc["raw_value"]["legislative_reference"]]
    if type(doc["raw_value"].get("theme")) is list:
        normalized_doc['wm_theme'] = doc["raw_value"].get("theme")

    print("OBJECT PROVIDES")
    print(normalized_doc["objectProvides"])

    normalized_doc["cluster_name"] = "wise-marine"
    normalized_doc = check_readingTime(normalized_doc, config)

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("wise_marine")
def preprocess_marine(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
