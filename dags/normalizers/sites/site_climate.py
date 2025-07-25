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


@register_facets_normalizer("climate")
def normalize_climate(doc, config):
    logger.info("NORMALIZE CLIMATE")
    logger.info(f"RS: {doc['raw_value'].get('review_state')}")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))
    logger.info(doc)

    portal_type = doc["raw_value"].get("@type", "")
    include_in_observatory = doc["raw_value"].get(
        "include_in_observatory", False)
    include_in_mission = doc["raw_value"].get("include_in_mission", False)
    publication_date = doc["raw_value"].get("publication_date", None)
    cca_uid = doc["raw_value"].get("UID", None)
    cca_created = doc["raw_value"].get("created", None)
    cca_published = doc["raw_value"].get("cca_published", None)
    cca_keywords = doc["raw_value"].get("keywords", [])
    cca_sectors = doc["raw_value"].get("sectors", [])
    cca_impacts = doc["raw_value"].get("climate_impacts", [])
    cca_elements = doc["raw_value"].get("elements", [])
    cca_health_impacts = doc["raw_value"].get("health_impacts", [])
    cca_origin_websites = doc["raw_value"].get("origin_website", [])
    cca_funding_programme = doc["raw_value"].get("funding_programme", None)
    cca_geographic = doc["raw_value"].get("geographic", None)
    cca_key_type_measure = doc["raw_value"].get("key_type_measures", [])
    cca_partner_contributors = doc["raw_value"].get("contributor_list", [])
    cca_key_system = doc["raw_value"].get("key_system", [])
    cca_countries = doc["raw_value"].get("country", [])
    cca_climate_threats = doc["raw_value"].get("climate_threats", [])
    cca_preview_image = doc["raw_value"].get('preview_image')

    cca_readiness_for_use = doc["raw_value"].get("readiness_for_use", [])
    cca_rast_steps = doc["raw_value"].get("rast_steps", [])
    cca_eligible_entities = doc["raw_value"].get("eligible_entities", [])
    cca_geographical_scale = doc["raw_value"].get("geographical_scale", [])
    cca_tool_language = doc["raw_value"].get("tool_language", [])
    cca_most_useful_for = doc["raw_value"].get("most_useful_for", [])
    cca_user_requirements = doc["raw_value"].get("user_requirements", [])

    ct_normalize_config = config["site"].get("normalize", {})

    logger.info("DATES:")
    logger.info(cca_published)
    logger.info(publication_date)

    _id = doc["raw_value"].get("@id", "")

    # if portal_type in ['News Item', 'Event'] and \
    #         any(path in _id
    #             for path in ["/mission/news/", "/mission/events/"]):
    if '/mission/' in _id:
        include_in_mission = True

    if not check_blacklist_whitelist(
        doc,
        ct_normalize_config.get("blacklist", []),
        ct_normalize_config.get("whitelist", []),
    ):
        logger.info("blacklisted")
        return None

    logger.info("whitelisted")

    doc["raw_value"]["themes"] = ["climate-change-adaptation"]
    doc_out = common_normalizer(doc, config)
    if not doc_out:
        return None

    doc_out["cca_uid"] = cca_uid
    doc_out["created"] = cca_created
    if doc_out.get("issued", None) is None:
        if cca_published is not None:
            doc_out["issued"] = cca_published
        else:
            if publication_date is not None:
                doc_out["issued"] = publication_date

    doc_out["publication_date"] = publication_date
    doc_out["cca_keywords"] = cca_keywords
    doc_out["cca_adaptation_sectors"] = vocab_to_list(cca_sectors)
    doc_out["cca_climate_impacts"] = vocab_to_list(cca_impacts)
    doc_out["cca_adaptation_elements"] = vocab_to_list(cca_elements)
    doc_out['cca_health_impacts'] = vocab_to_list(cca_health_impacts, "token")
    doc_out['cca_key_type_measure'] = vocab_to_list(
        cca_key_type_measure, "token")
    doc_out['cca_partner_contributors'] = vocab_to_list(
        cca_partner_contributors, 'title')

    doc_out['cca_readiness_for_use'] = vocab_to_list(
        cca_readiness_for_use, 'title')
    doc_out['cca_rast_steps'] = vocab_to_list(cca_rast_steps, 'title')
    doc_out['cca_eligible_entities'] = vocab_to_list(
        cca_eligible_entities, 'title')
    doc_out['cca_geographical_scale'] = vocab_to_list(
        cca_geographical_scale, 'title')
    doc_out['cca_tool_language'] = vocab_to_list(cca_tool_language, 'title')
    doc_out['cca_most_useful_for'] = vocab_to_list(
        cca_most_useful_for, 'title')
    doc_out['cca_user_requirements'] = vocab_to_list(
        cca_user_requirements, 'title')

    doc_out['key_system'] = vocab_to_list(cca_key_system, 'title')
    doc_countries = doc_out.get('spatial', [])
    if type(doc_countries) is not list:
        doc_countries = [doc_countries]
    if doc_countries[0] == 'Other':
        doc_countries = []
    doc_out['spatial'] = doc_countries + vocab_to_list(cca_countries, "title")
    doc_out['climate_threats'] = vocab_to_list(cca_climate_threats, 'title')

    if isinstance(cca_funding_programme, str):
        doc_out["cca_funding_programme"] = cca_funding_programme
    else:
        doc_out["cca_funding_programme"] = vocab_to_term(cca_funding_programme)

    doc_out["cca_origin_websites"] = vocab_to_list(cca_origin_websites)

    if cca_geographic:
        if 'countries' in cca_geographic:
            doc_out["cca_geographic_countries"] = [
                country for country in cca_geographic['countries']]
        if 'transnational_region' in cca_geographic:
            doc_out["cca_geographic_transnational_region"] = [
                country for country in cca_geographic['transnational_region']]

    doc_out["cluster_name"] = "cca"
    doc_out["cca_include_in_search"] = "true" if is_portal_type_in_search(
        portal_type) else 'false'
    doc_out["cca_include_in_search_observatory"] = "true" \
        if include_in_observatory else 'false'
    doc_out["cca_include_in_mission"] = "true" \
        if include_in_mission else 'false'
    print("preview")
    print(cca_preview_image)
    if portal_type == "mission_funding_cca":
        is_eu_funded = doc['raw_value'].get('is_eu_funded', False)
        if is_eu_funded:
            doc_out["cca_is_eu_funded"] = 'Yes'
        else:
            doc_out["cca_is_eu_funded"] = 'No'
    if cca_preview_image is not None:
        doc_out["cca_preview_image"] = cca_preview_image.get(
            'scales', {}).get('preview', {}).get('download')
    # if doc["raw_value"].get("review_state") == "archived":
    #     # raise Exception("review_state")
    #     expires = date.today() - timedelta(days=2)
    #     doc_out["expires"] = expires.isoformat()
    #     logger.info("RS EXPIRES")
    doc_out = check_readingTime(doc_out, config)

    doc_out = apply_norm_obj(doc_out, config.get(
        "normalizers", {}).get("normObj", {}))
    doc_out = add_counts(doc_out)
    return doc_out


@register_nlp_preprocessor("climate")
def preprocess_climate(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc


def is_portal_type_in_search(portal_type):
    allowed_portal_types = [
        "eea.climateadapt.aceproject",
        "eea.climateadapt.adaptationoption",
        "eea.climateadapt.casestudy",
        "eea.climateadapt.guidancedocument",
        "eea.climateadapt.indicator",
        "eea.climateadapt.informationportal",
        "eea.climateadapt.organisation",
        "eea.climateadapt.publicationreport",
        "eea.climateadapt.tool",
        "eea.climateadapt.video",
        "eea.climateadapt.mapgraphdataset",
        "eea.climateadapt.researchproject",
        "eea.climateadapt.c3sindicator",
    ]
    if portal_type in allowed_portal_types:
        return True
    return False
