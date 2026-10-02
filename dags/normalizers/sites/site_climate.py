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
    cca_adaptation_options = doc["raw_value"].get("cca_adaptation_options", [])
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

    cca_funding_type = doc["raw_value"].get("funding_type", [])
    cca_budget_range = doc["raw_value"].get("budget_range", [])

    cca_governance_level = doc["raw_value"].get("governance_level", [])
    cca_gallery_urls = doc["raw_value"].get("cca_gallery_urls", [])

    cca_ipcc_category = doc["raw_value"].get("ipcc_category", [])

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
    doc_out["cca_adaptation_options"] = cca_adaptation_options
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
    doc_out['cca_governance_level_list'] = vocab_to_list(
        cca_governance_level, 'title')
    doc_out["cca_gallery_urls"] = cca_gallery_urls
    doc_out["cca_ipcc_category"] = vocab_to_list(cca_ipcc_category, 'title')

    doc_out["cca_updated_params"] = 1
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

        if 'biogeographical_regions' in cca_geographic:
            doc_out["cca_biogeographical_regions"] = [
                biogeographical_region for biogeographical_region in cca_geographic['biogeographical_regions']]
        if 'geographic_characterisation' in cca_geographic:
            doc_out["cca_geographic_characterisation"] = [
                geographic_characterisation for geographic_characterisation in cca_geographic['geographic_characterisation']]
        if 'sub_nationals' in cca_geographic:
            doc_out["cca_sub_nationals"] = [
                sub_national for sub_national in cca_geographic['sub_nationals']]
        if 'city' in cca_geographic:
            doc_out["cca_city"] = cca_geographic["city"]
    doc_out["cluster_name"] = "cca"
    doc_out["cca_include_in_search"] = "true" if is_portal_type_in_search(
        portal_type) else 'false'
    doc_out["cca_include_in_search_observatory"] = "true" \
        if include_in_observatory else 'false'
    doc_out["cca_include_in_mission"] = "true" \
        if include_in_mission else 'false'
    print("preview")
    if portal_type == 'eea.climateadapt.extendedtool':
        doc_out["cluster_name"] = "cca_navigator"
        doc_out['exclude_from_globalsearch'] = ['True']
        doc_out["coder_1"] = doc["raw_value"].get("coder_1", None)
        doc_out["coder_2"] = doc["raw_value"].get("coder_2", None)
        doc_out["adaptation_support_cycle_step"] = doc["raw_value"].get(
            "adaptation_support_cycle_step", [])
        doc_out["place_of_implementation"] = doc["raw_value"].get(
            "place_of_implementation", [])
        doc_out["data_sources"] = doc["raw_value"].get("data_sources", [])
        doc_out["user_support_provisions"] = doc["raw_value"].get(
            "user_support_provisions", [])
        doc_out["tool_validation_use"] = doc["raw_value"].get(
            "tool_validation_use", [])
        doc_out["number_of_users_tool"] = doc["raw_value"].get(
            "number_of_users_tool", [])
        doc_out["tool_provider_mode"] = doc["raw_value"].get(
            "tool_provider_mode", [])
        doc_out["temporality_of_data"] = doc["raw_value"].get(
            "temporality_of_data", [])
        doc_out["only_interactive_support_tool"] = doc["raw_value"].get(
            "only_interactive_support_tool", None)
        doc_out["adaptation_cycle_step"] = doc["raw_value"].get(
            "adaptation_cycle_step", None)
        doc_out["updating_cycle_of_the_tool"] = doc["raw_value"].get(
            "updating_cycle_of_the_tool", None)
        doc_out["language_accessibility"] = doc["raw_value"].get(
            "language_accessibility", None)
        doc_out["free_access"] = doc["raw_value"].get("free_access", None)
        doc_out["hyperlink"] = doc["raw_value"].get("hyperlink", None)
        doc_out["functionality"] = doc["raw_value"].get(
            "functionality", None)

        doc_out["nature_based_solution"] = doc["raw_value"].get(
            "nature_based_solution", None)
        if doc_out["nature_based_solution"]:
            doc_out["cca_nature_based_solution"] = 'Yes'
        else:
            doc_out["cca_nature_based_solution"] = 'No'

        doc_out["tool_provider"] = doc["raw_value"].get("tool_provider", None)

        accessibility_and_usability = doc["raw_value"].get(
            "accessibility_and_usability", None)
        print(accessibility_and_usability)

        if accessibility_and_usability:
            accessibility_and_usability = accessibility_and_usability.get(
                'title', None)
        doc_out["accessibility_and_usability"] = accessibility_and_usability

        adaptation_support_cycle_step = doc["raw_value"].get(
            "adaptation_support_cycle_step", None)
        doc_out["cca_adaptation_support_cycle_step"] = vocab_to_list(
            adaptation_support_cycle_step, 'title')
        intended_user_groups = doc["raw_value"].get(
            "intended_user_groups", None)
        doc_out["cca_intended_user_groups"] = vocab_to_list(
            intended_user_groups, 'title')
        type_of_outputs = doc["raw_value"].get("type_of_outputs", None)
        doc_out["cca_type_of_outputs"] = vocab_to_list(type_of_outputs, 'title')
        type_of_data = doc["raw_value"].get("type_of_data", None)
        doc_out["cca_type_of_data"] = vocab_to_list(type_of_data, 'title')
        license_status = doc["raw_value"].get("license_status", None)
        doc_out["cca_license_status"] = vocab_to_list(license_status, 'title')


        focus_areas = doc["raw_value"].get("focus_areas", [])
        place_of_implementation = doc["raw_value"].get("place_of_implementation", [])
        elements = doc["raw_value"].get("elements", [])
        doc_out["cca_focus_areas"] = vocab_to_list(focus_areas, 'title')
        doc_out["cca_place_of_implementation"] = vocab_to_list(place_of_implementation, 'title')
        doc_out["cca_elements"] = vocab_to_list(elements, 'title')

    print(cca_preview_image)
    if portal_type == "mission_funding_cca":
        is_eu_funded = doc['raw_value'].get('is_eu_funded', False)
        if is_eu_funded:
            doc_out["cca_is_eu_funded"] = 'Yes'
        else:
            doc_out["cca_is_eu_funded"] = 'No'

        doc_out["cca_objective_funding_programme"] = doc['raw_value'].get('objective_funding_programme', None)
        doc_out['cca_funding_type'] = vocab_to_list(
            cca_funding_type, 'title')
        doc_out["cca_funding_rate"] = doc['raw_value'].get('funding_rate', None)
        doc_out['cca_budget_range'] = vocab_to_list(
            cca_budget_range, 'title')

        is_blended = doc['raw_value'].get('is_blended', False)
        if is_blended:
            doc_out["cca_is_blended"] = 'Yes'
        else:
            doc_out["cca_is_blended"] = 'No'

        is_consortium_required = doc['raw_value'].get('is_consortium_required', False)
        if is_consortium_required:
            doc_out["cca_is_consortium_required"] = 'Yes'
        else:
            doc_out["cca_is_consortium_required"] = 'No'


        doc_out["cca_administering_authority"] = doc['raw_value'].get('administering_authority', None)
        doc_out["cca_further_information"] = doc['raw_value'].get('further_information', None)
        doc_out["cca_general_information"] = doc['raw_value'].get('general_information', None)
        doc_out["cca_publication_page"] = doc['raw_value'].get('publication_page', None)
        doc_out["cca_funding_region"] = doc['raw_value'].get('funding_region', None)


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
        "eea.climateadapt.extendedtool",
    ]
    if portal_type in allowed_portal_types:
        return True
    return False

