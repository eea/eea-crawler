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


def simplify_list(attr_list, field="title"):
    print("========================")
    print(attr_list)
    attr_list = attr_list or []
    if type(attr_list) != list:
        attr_list = [attr_list]
    return [val[field] for val in attr_list or []]


@register_facets_normalizer("insitu")
def normalize_copernicus_insitu(doc, config):
    logger.info("NORMALIZE INSITU")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))

    insitu_preview_image = doc["raw_value"].get('preview_image')

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "copernicus_insitu"

    normalized_doc['title'] = get_page_title(doc)

    normalized_doc["taxonomy_report_category"] = simplify_list(
        doc.get("raw_value", {}).get("taxonomy_report_category", []))
    normalized_doc["taxonomy_copernicus_components"] = simplify_list(
        doc.get("raw_value", {}).get("taxonomy_copernicus_components", []))
    normalized_doc["taxonomy_copernicus_themes"] = simplify_list(
        doc.get("raw_value", {}).get("taxonomy_copernicus_themes", []))
    normalized_doc["data_providers_list"] = simplify_list(
        doc.get("raw_value", {}).get("data_providers_list", []))

    normalized_doc['copernicus_services'] = simplify_list(
        doc.get('raw_value', {}).get("copernicus_services", []))

    normalized_doc = check_readingTime(normalized_doc, config)

    if insitu_preview_image is not None:
        normalized_doc["insitu_preview_image"] = insitu_preview_image.get(
            'scales', {}).get('preview', {}).get('download')

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("insitu")
def preprocess_copernicus_insitu(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
