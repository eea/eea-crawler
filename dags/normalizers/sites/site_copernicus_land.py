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


def vocab_to_list(vocab, attr="title"):
    return [term[attr] for term in vocab] if vocab else []


def get_library_categories_values(raw_doc):
    categories = vocab_to_list(raw_doc.get("taxonomy_technical_library_categorization"), "title")
    return [cat.split("#")[-1] for cat in categories]

def get_library_categories_facet(raw_doc):
    values = get_library_categories_values(raw_doc)
    return list(dict.fromkeys([cat.split("»")[0].strip() for cat in values]))

def get_file_size(raw_doc):
    return raw_doc.get("file",{}).get("size")

def get_version(raw_doc):
    return raw_doc.get("version", "")

@register_facets_normalizer("land")
def normalize_copernicus_land(doc, config):
    logger.info("NORMALIZE LAND")
    logger.info(doc["raw_value"].get("@id", ""))
    logger.info(doc["raw_value"].get("@type", ""))

    normalized_doc = common_normalizer(doc, config)
    if not normalized_doc:
        return None

    normalized_doc["cluster_name"] = "copernicus_land"

    #normalized_doc['title'] = get_page_title(doc)

    normalized_doc["library_categories_facet"] =  get_library_categories_facet(doc["raw_value"])
    normalized_doc["library_categories_values"] =  get_library_categories_values(doc["raw_value"])
    normalized_doc["file_size"] = get_file_size(doc["raw_value"])
    normalized_doc["version"] = get_version(doc["raw_value"])

    if doc["raw_value"].get("@type", "") == "TechnicalLibrary":
        normalized_doc["issued"] = doc["raw_value"].get("publication_date")
        normalized_doc["year"] = doc["raw_value"].get("publication_date")
        normalized_doc["description"] = doc["raw_value"].get("description", "")
    normalized_doc = check_readingTime(normalized_doc, config)

    normalized_doc = add_counts(normalized_doc)
    return normalized_doc


@register_nlp_preprocessor("land")
def preprocess_copernicus_land(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
