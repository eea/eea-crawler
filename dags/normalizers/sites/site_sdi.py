"""
Mapping of fields from the sdi elastic endpoint

data series main query
----------------------
changeDate -> check if there were any update


data series mapping in airflow
------------------------------
metadataIdentifier -> about, original_id, id
agg_associated -> children (datasets)
isPublishedToAll -> hasWorkflowState
resourceDate || publicationDateForResource || createDate -> issued
overview[..].url -> overview.url
th_rod-eionet-europa-eu -> rod, instrument
th_eea-topics -> topic
th_gemet_tree -> gemet
cl_spatialRepresentationType -> spatialRepresentationType
th_regions -> spatial, places
resourceTemporalExtentDetails -> time_coverage
resourceAbstractObject.default -> description
resourceTitleObject.default -> title, label
mainLanguage -> language


dataset mapping in airflow
--------------------------
format -> dataset_formats


dataset mapping (only on frontend)
-----------------------------
format
resourceTemporalExtentDetails
resourceTitleObject
resourceType
link
link.name
link.protocol
link.url
link.description
link.function
"""

from datetime import datetime, timedelta
from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    update_from_theme_taxonomy,
    common_normalizer,
    check_blacklist_whitelist,
    simplify_elements,
    add_counts,
)
from normalizers.lib.nlp import common_preprocess
import logging

logger = logging.getLogger(__file__)


def simplify_list(sdi_list, field="default"):
    return [val[field] for val in sdi_list or []]


def capitalise_list(sdi_list, field="default"):
    return [val[field].title() for val in sdi_list or []]


def simplify_list_from_tree(sdi_list):
    return [val.split("^")[-1].title() for val in sdi_list or []]


def get_year_from_date(date_str):
    # return datetime.strptime(date_str.split("T")[0], "%Y-%m-%d")
    return int(date_str[:4])


def get_merged_ranges(ranges):
    has_from = True
    has_to = True
    years = []
    for time_range in ranges:
        # TODO: check default min & max for time coverage
        r_from_str = time_range.get("start").get("date", None)
        r_to_str = time_range.get("end").get("date", None)
        if r_from_str is None:
            has_from = False
            r_from_str = "2010-01-01"
        if r_to_str is None:
            has_to = False
            r_to_str = f"{datetime.now().year}-01-01"
        y_from = get_year_from_date(r_from_str)
        y_to = get_year_from_date(r_to_str)
        for year in list(range(y_from, y_to + 1)):
            if year not in years:
                years.append(year)

    years.sort()
    merged_ranges = []
    current_range = {}
    if len(years) > 0:
        for year in range(min(years), max(years) + 2):
            if current_range.get("start", None) is None:
                if year in years:
                    current_range["start"] = year
            else:
                if year not in years:
                    current_range["end"] = year - 1
                    merged_ranges.append(current_range)
                    current_range = {}
        if not has_from:
            del merged_ranges[0]["start"]
        if not has_to:
            del merged_ranges[-1]["end"]
    return merged_ranges


def get_years_from_ranges(ranges):
    years = []
    print(ranges)
    for time_range in ranges:
        # TODO: check default min & max for time coverage
        r_from_str = time_range.get("start").get("date", "2010-01-01")
        r_to_str = time_range.get("end").get(
            "date", f"{datetime.now().year}-01-01"
        )
        y_from = get_year_from_date(r_from_str)
        y_to = get_year_from_date(r_to_str)
        for year in list(range(y_from, y_to + 1)):
            if year not in years:
                years.append(year)

    years.sort()
    return years


def get_formats(datasets):
    formats = []
    for dataset in datasets:
        dataset_formats = dataset.get("format", [])
        if type(dataset_formats) != list:
            dataset_formats = [dataset_formats]
        for dataset_format in dataset_formats:
            formats.append(dataset_format)
    return formats


def pre_normalize_sdi(doc, config):
    doc["raw_value"]["site_id"] = "sdi"
    doc["raw_value"] = simplify_elements(doc["raw_value"], "")
    doc["raw_value"]["@type"] = "Data set"
    doc["raw_value"]["about"] = doc["raw_value"]["metadataIdentifier"]
    isPublishedToAll = doc["raw_value"].get("isPublishedToAll", "false")
    print("ISPUBLISHED")
    print(isPublishedToAll)
    if isinstance(isPublishedToAll, list):
        isPublishedToAll = isPublishedToAll[0]
    if isinstance(isPublishedToAll, type(True)):
        isPublishedToAll = str(isPublishedToAll).lower()
    print(isPublishedToAll)

    isPublishedToAll = "true"  # TODO: temporary fix, should be removed

    if isPublishedToAll == "true":
        doc["raw_value"]["review_state"] = "published"

        resourceDates = doc["raw_value"].get("resourceDate", [])
        if len(resourceDates) > 0:
            publishDates = [
                rdate["date"]
                for rdate in resourceDates
                if rdate["type"] == "publication"
            ]
            if len(publishDates) > 0:
                doc["raw_value"]["issued"] = publishDates[-1]
        else:
            # fallback to creation date
            doc["raw_value"]["issued"] = doc["raw_value"].get(
                "publicationDateForResource",
                doc["raw_value"].get("createDate"),
            )

    doc["raw_value"]["overview.url"] = simplify_list(
        doc["raw_value"].get("overview", []), "url"
    )
    doc["raw_value"]["sdi_rod"] = simplify_list(
        doc["raw_value"].get("th_rod-eionet-europa-eu", [])
    )
    doc["raw_value"]["sdi_topics"] = simplify_list(
        doc["raw_value"].get("th_eea-topics", [])
    )
    doc["raw_value"]["sdi_topics"] = [
        topic if topic != "Climate mitigation" else "climate"
        for topic in doc["raw_value"]["sdi_topics"]
    ]
    doc["raw_value"]["sdi_topics"] = [
        topic if topic != "Climate adaptation" else "climate-change-adaptation"
        for topic in doc["raw_value"]["sdi_topics"]
    ]
    doc["raw_value"]["sdi_topics"] = update_from_theme_taxonomy(
        doc.get("raw_value", {}).get("sdi_topics", []),
        config.get("full_config", {}).get("theme_taxonomy", {}),
    )
    doc["raw_value"]["sdi_gemet"] = simplify_list_from_tree(
        doc["raw_value"].get("th_gemet_tree.default", [])
    )
    doc["raw_value"]["sdi_spatialRepresentationType"] = simplify_list(
        doc["raw_value"].get("cl_spatialRepresentationType", [])
    )
    doc["raw_value"]["sdi_spatial"] = simplify_list(
        doc["raw_value"].get("th_regions", [])
    )
    doc["raw_value"]["time_coverage"] = get_years_from_ranges(
        doc["raw_value"].get("resourceTemporalExtentDetails", [])
    )
    doc["raw_value"]["merged_time_coverage_range"] = get_merged_ranges(
        doc["raw_value"].get("resourceTemporalExtentDetails", [])
    )

    rods = simplify_list(
        doc["raw_value"].get("th_rod-eionet-europa-eu", []), field="link"
    )

    doc["raw_value"]["dataset_formats"] = get_formats(
        doc["raw_value"].get("children", [])
    )
    doc["raw_value"]["instrument"] = list(
        set(
            [
                config.get("full_config", {})
                .get("obligations", {})
                .get(rod)["label"]
                for rod in rods
            ]
        )
    )
    return doc


@register_facets_normalizer("sdi")
def normalize_sdi(doc, config):
    logger.info("NORMALIZE SDI")
    doc = pre_normalize_sdi(doc, config)
    normalized_doc = common_normalizer(doc, config)
    normalized_doc["cluster_name"] = "sdi"
    tc = get_years_from_ranges(
        doc["raw_value"].get("resourceTemporalExtentDetails", [])
    )
    normalized_doc["time_coverage"] = [str(y) for y in tc]
    normalized_doc = add_counts(normalized_doc)
    normalized_doc["raw_value"] = doc["raw_value"]
    return normalized_doc


@register_nlp_preprocessor("sdi")
def preprocess_sdi(doc, config):

    doc = pre_normalize_sdi(doc, config)

    dict_doc = common_preprocess(doc, config)

    return dict_doc
