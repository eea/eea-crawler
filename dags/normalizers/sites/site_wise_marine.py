from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import common_normalizer
from normalizers.lib.nlp import common_preprocess


RULES = [
    {"path": "/marine/policy-and-reporting/*", "ct": ["Web page"]},
    {
        "path": "/marine/state-of-europe-seas/",
        "ct": ["Topic page", "Web page"],
    },
    {
        "path": "/marine/state-of-europe-seas/*",
        "ct": ["Topic page", "Web page"],
    },
    {
        "path": "/marine/state-of-europe-seas/marine-sectors-catalogue-of-measures",
        "ct": ["Dashboard"],
    },
    {
        "path": "/marine/data-maps-and-tools/map-viewers-visualization-tools/dashboards-on-marine-features-under-other-policies/*",
        "ct": ["Dashboard"],
    },
    {
        "path": "/marine/data-maps-and-tools/msfd-reporting-information-products/ges-assessment-dashboards/*",
        "ct": ["Dashboard"],
    },
    {
        "path": "/marine/data-maps-and-tools/msfd-reporting-information-products/ges-assessment-dashboards/country-thematic-dashboards/",
        "ct": ["Country fact sheet", "Dashboard"],
    },
    {
        "path": "/marine/data-maps-and-tools/msfd-reporting-information-products/msfd-reporting-data-explorer",
        "ct": ["Data"],
    },
    {
        "path": "/marine/data-maps-and-tools/map-viewers-visualization-tools/european-reference-maps",
        "ct": ["Map (interactive)"],
    },
    {
        "path": "/marine/countries-and-regional-seas/country-profiles/*",
        "ct": ["Country fact sheet", "Dashboard"],
    },
]


def is_doc_on_path(loc, doc_loc):
    loc = loc.strip("*")
    if (
        doc_loc.strip("/").find(loc.strip("/")) == 0
        and len(doc_loc.strip("/").split("/"))
        == len(loc.strip("/").split("/")) + 1
    ):
        return True
    return False


def is_doc_eq_path(loc, doc_loc):
    return loc.strip("/") == doc_loc.strip("/")


def find_ct_by_rules(doc_loc):
    ct = []
    for rule in RULES:
        if rule["path"].endswith("*"):
            if is_doc_on_path(rule["path"], doc_loc):
                ct = rule["ct"]
        else:
            if is_doc_eq_path(rule["path"], doc_loc):
                ct = rule["ct"]
    return ct


@register_facets_normalizer("water.europa.eu/marine")
def normalize_energy(doc, config):
    normalized_doc = common_normalizer(doc, config)

    normalized_doc["cluster_name"] = "WISE Marine (water.europa.eu/marine)"

    doc_loc = urlparse(normalized_doc["id"]).path

    ct = find_ct_by_rules(doc_loc)

    if len(ct) == 0:
        ct.append("Web page")
    # TODO: remove initial value of ct
    ct.append(f"from_json_{normalized_doc['objectProvides']}")

    normalized_doc["objectProvides"] = ct
    return normalized_doc


@register_nlp_preprocessor("water.europa.eu/marine")
def preprocess_energy(doc, config):
    dict_doc = common_preprocess(doc, config)

    return dict_doc
