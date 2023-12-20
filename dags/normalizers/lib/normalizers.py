# https://www.eea.europa.eu/api/SITE/soer/2020/outreach/soer-2020-outreach-specific-privacy-statement
# pubished_eionet

import json
import re
from datetime import date, timedelta

from normalizers.lib.trafilatura_extract import get_text_from_html, get_title_from_html
import logging

logger = logging.getLogger(__file__)


def apply_black_map(doc, black_map):
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if black_map.get(key, None) is None:
            value = doc[key]
        else:
            if isinstance(doc[key], list):
                tmp_value = []
                for val in doc[key]:
                    if val not in black_map[key]:
                        tmp_value.append(val)
                value = tmp_value
            else:
                if doc[key] in black_map[key]:
                    value = None
        clean_data[key] = value
    return clean_data


def apply_white_map(doc, white_map):
    clean_data = {}
    for key in doc.keys():
        value = None
        if white_map.get(key, None) is None:
            value = doc[key]
        else:
            if isinstance(doc[key], list):
                tmp_value = []
                for val in doc[key]:
                    if val in white_map[key]:
                        tmp_value.append(val)
                value = tmp_value
            else:
                if doc[key] in white_map[key]:
                    value = doc[key]
        clean_data[key] = value
    return clean_data


def apply_norm_obj(doc, norm_obj):
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if isinstance(doc[key], list):
            value = []
            for val in doc[key]:
                if isinstance(val, dict) or isinstance(val, list):
                    value.append(val)
                else:
                    if norm_obj.get(val, None) is not None:
                        value.append(norm_obj[val])
                    else:
                        value.append(val)
        else:
            if norm_obj.get(value, None) is not None:
                value = norm_obj[value]
        clean_data[key] = value

    return clean_data


def apply_norm_prop(doc, norm_prop):
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if norm_prop.get(key, None) is None:
            clean_data[key] = value
        else:
            if not isinstance(norm_prop[key], list):
                norm_prop[key] = [norm_prop[key]]
            for new_key in norm_prop[key]:
                clean_data[new_key] = value
    return clean_data


def apply_norm_missing(doc, norm_missing):
    clean_data = doc
    for key in norm_missing.keys():
        if clean_data.get(key, None) is None:
            if isinstance(norm_missing[key], str) and norm_missing[
                key
            ].startswith("field:"):
                clean_data[key] = doc.get(
                    norm_missing[key].split("field:")[-1].strip()
                )
            else:
                clean_data[key] = norm_missing[key]
    return clean_data


def remove_duplicates(doc):
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if isinstance(value, list):
            try:
                value = list(dict.fromkeys(value))
            except Exception:
                value = value
        clean_data[key] = value
    return clean_data


def remove_extra_webpages(doc):
    logger.info("REMOVE EXTRA WEBPAGES")
    logger.info(doc.get("objectProvides", []))
    ops = doc.get("objectProvides", [])
    if isinstance(ops, list):
        if "Webpage" in ops and len(ops) > 1:
            ops.remove("Webpage")
            doc["objectProvides"] = ops
    logger.info(doc.get("objectProvides", []))
    return doc


def remove_empty(doc):
    clean_data = {}
    for key in doc.keys():
        ignore_attr = False
        if isinstance(doc[key], list):
            if len(doc[key]) == 0:
                ignore_attr = True
        else:
            if doc[key] is None:
                ignore_attr = True
            else:
                if isinstance(doc[key], str) and len(doc[key]) == 0:
                    ignore_attr = True
        if not ignore_attr:
            clean_data[key] = doc[key]
    return clean_data


def strip_fields(doc):
    for key in doc.keys():
        if isinstance(doc[key], str):
            doc[key] = doc[key].strip()

    return doc


def add_places(norm_doc):
    if norm_doc.get("spatial", None) is not None:
        norm_doc["places"] = norm_doc["spatial"]
    return norm_doc


def join_text_fields(
    text, json_doc, txt_props, txt_props_black, include_title=True
):
    # json_doc = json.loads(doc)
    # start text with the document title.
    title = json_doc.get("title", "no title") or "no title"

    if include_title:
        text += "\n\n" + title + ".\n\n"

    # get other predefined fields first in the order defined in txt_props param
    for prop in txt_props:
        prop_v = json_doc.get(prop, {})
        if type(prop_v) is dict:
            txt = cleanhtml(prop_v.get("data", ""))
        else:
            txt = cleanhtml(prop_v)
        if len(txt) and not txt.endswith("."):
            txt = txt + "."
        # avoid redundant text
        if len(txt) and txt not in text:
            text = text + txt + "\n\n"

    # find automatically all props that have text or html in it
    # and append to text if not already there.
    for k, v in json_doc.items():
        if type(v) is dict and k not in txt_props_black:
            txt = ""
            # print(f'%s is a dict' % k)
            mime_type = json_doc.get(k, {}).get("content-type", "")
            if mime_type == "text/plain":
                # print('%s is text/plain' % k)
                txt = json_doc.get(k, {}).get("data", "")
            elif mime_type == "text/html":
                # print('%s is text/html' % k)
                txt = cleanhtml(json_doc.get(k, {}).get("data", ""))
            # avoid redundant text
            if len(txt) and txt not in text:
                if not txt.endswith("."):
                    txt = txt + "."
                text = text + "\n\n" + txt + "\n\n"

    # TODO: for volto based content types with blocks, the above would not work,
    # a better approach would need to grab the rendered html page and strip the html. could be done for all content types.

    return text


def cleanhtml(raw_html):
    cleantext = ""
    if isinstance(raw_html, str):
        cleanr = re.compile("<.*?>")
        cleantext = re.sub(cleanr, "", raw_html)

    return cleantext.strip()


def simplify_elements(element, element_key):
    clean_element = {}
    if isinstance(element, dict):
        for key in element.keys():
            new_elements = simplify_elements(element[key], key)
            for new_key in new_elements.keys():

                new_element_key = new_key
                if len(element_key) > 0:
                    new_element_key = element_key + "." + new_key
                clean_element[new_element_key] = new_elements[new_key]
    else:
        clean_element[element_key] = element
    return clean_element


def create_doc(doc):
    return simplify_elements(doc, "")


def get_attrs_to_delete(doc, proplist):
    attrs = []
    for key in doc.keys():
        if key not in proplist:
            attrs.append(key)
    return attrs


def delete_attrs(doc, attrs):
    clean_data = {}
    for key in doc.keys():
        if key not in attrs:
            clean_data[key] = doc[key]
    return clean_data

def get_page_title(doc, trafilatura_config={}):
    html = doc.get("web_html", "")

    title = get_title_from_html(html, trafilatura_config)
    return title

def add_reading_time_and_fulltext(
    norm_doc, doc, txt_props=[], txt_props_black=[], trafilatura_config={}, reading_time_blacklist=[]
):
    html = doc.get("web_html", "")
    #    print("BEFORE")
    #    print(html)
    text = get_text_from_html(html, trafilatura_config)
    #    print("AFTER")
    #    print(text)

    if not text or len(text) == 0:
        text = join_text_fields(
            text,
            doc["raw_value"],
            txt_props,
            txt_props_black,
            include_title=False,
        )

    # CHECK pdf viewer & trafilatura

    pdf_text = doc.get("pdf_text", "")

    text += "\n\n" + pdf_text
    norm_doc["fulltext"] = text
    wc = res = len(re.findall(r"\w+", text))

    print("Add reading time")
    print(reading_time_blacklist)
    if doc.get("raw_value", "@type") not in reading_time_blacklist:
        norm_doc["readingTime"] = wc / 228
        print("Reading time added")
    else:
        norm_doc["readingTime"] = -1
        print("Reading time not added, @type is blacklisted")
    return norm_doc


def update_locations(norm_doc):
    try:
        json_location = json.loads(norm_doc.get("location", ""))
        norm_doc["location"] = [
            loc["properties"]["title"] for loc in json_location["features"]
        ]
    except:
        pass
    return norm_doc


def fetch_geo_coverage(norm_doc):
    print("FETCH_GEO")
    print(norm_doc.get("geo_coverage.geolocation", []))
    geo_locations = [
        loc["label"] for loc in norm_doc.get("geo_coverage.geolocation", []) if loc.get("label", None) is not None
    ]
    if len(geo_locations) > 0:
        norm_doc["spatial"] = geo_locations
    return norm_doc


def fetch_temporal_coverage(norm_doc):
    geo_locations = [
        loc["label"] for loc in norm_doc.get("temporal_coverage.temporal", [])
    ]
    if len(geo_locations) > 0:
        norm_doc["time_coverage"] = geo_locations
    return norm_doc


def merge_types(doc):
    allowed_op = [
        "Products.EEAContentTypes.content.interfaces.ICountryProfile"
    ]

    if isinstance(doc.get("@components.object_provides"), str):
        doc["@components.object_provides"] = [
            doc["@components.object_provides"]
        ]
    if isinstance(doc.get("@type"), str):
        doc["@type"] = [doc["@type"]]
        doc["@type"] = doc["@type"] + [
            d
            for d in doc.get("@components.object_provides", [])
            if d in allowed_op
        ]
    return doc


def update_language(doc):
    doc["language"] = doc.get("language", doc.get("language.token", "en"))
    return doc


def fix_state(doc):
    # list of all issues with examples

    ## ignore that has no state published
    # treat it by site, not generic

    # keep this
    if (
        doc.get("objectProvides") == "File"
        and doc.get("hasWorkflowState") == "visible"
    ):
        doc["hasWorkflowState"] = doc["parent.review_state"]
    # if no publish date => don't index
    # if doc["hasWorkflowState"] in ["published", "archived"]:
    #     if not doc.get("issued"):
    #         doc["issued"] = doc.get("created", doc.get("creation_date"))
    # keep this
    if doc["hasWorkflowState"] == "archived" and not doc.get("expires"):
        expires = date.today() - timedelta(
            days=2
        )  ## should be modification date
        doc["expires"] = expires.isoformat()

    # get rid,
    # if doc.get("issued"):
    #     doc["hasWorkflowState"] = "published"

    return doc


ALLOWED_CONTENT_TYPES = [
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-word.document.macroEnabled.12",
    "application/pdf",
]


def addFormat(doc, raw_doc):
    if raw_doc.get("pdf_text", None):
        doc_format = doc.get("format", None)
        if not isinstance(doc_format, list):
            doc_format = [doc_format]
        if len(set(ALLOWED_CONTENT_TYPES).intersection(set(doc_format))) == 0:
            # if "application/pdf" not in doc_format:
            doc_format.append("application/pdf")
            doc["format"] = doc_format
    return doc


def update_from_theme_taxonomy(themes, theme_taxonomy):
    if type(themes) != list:
        themes = [themes]
    print("update themes from taxonomy")
    print(themes)
    print(theme_taxonomy)
    updated_themes = [
        theme_taxonomy.get(theme, {}).get("label", theme) for theme in themes
    ]
    print(updated_themes)
    return updated_themes


def merge_themes(doc):
    original_themes = doc["raw_value"].get("themes", [])
    taxonomy_themes = [
        theme["token"] for theme in doc["raw_value"].get("taxonomy_themes", [])
    ]
    themes = original_themes + taxonomy_themes
    return themes

def find_all(key, node, name, results=[]):
    if key == name:
        results.append(node)
    else:
        if type(node) is dict:
            for elem in node.keys():
                results = find_all(elem, node[elem], name, results)
        else:
            if type(node) is list:
                for elem in node:
                    results = find_all('', elem, name, results)
    return results

def get_data_provenance(doc):
    print("DATA PROVENANCE")
    dps = find_all('', doc, 'data_provenance',[])
    dps_full = []
    for dp_part in dps:
        dp_data = dp_part.get('data',[])
        for dp in dp_data:
            should_add = True

            for dp_seen in dps_full:
                if dp["link"] == dp_seen["link"] and dp["organisation"] == dp_seen["organisation"] and dp["title"] == dp_seen["title"]:
                    should_add = False
            if should_add:
                dps_full.append({"link":dp["link"], "organisation": dp["organisation"], "title":dp["title"]})

    if len(dps_full) == 0:
        chartSources = find_all('', doc, 'chartSources', [])
        for cs_part in chartSources:
            for cs in cs_part:
                should_add = True
                for dp_seen in dps_full:
                    if cs["chart_source_link"] == dp_seen["link"] and cs["chart_source"] == dp_seen["organisation"]:
                        should_add = False
                if should_add:
                    dps_full.append({"link":cs["chart_source_link"], "organisation": cs["chart_source"], "title":cs["chart_source"]})

    dp_organisations = list(dict.fromkeys([dp["organisation"] for dp in dps_full]))

    return {
        "data_provenances" : dps_full,
        "data_provenances_organisations": dp_organisations
    }

def common_normalizer(doc, config):
    doc["raw_value"]["themes"] = merge_themes(doc)
    doc["raw_value"]["themes"] = update_from_theme_taxonomy(
        doc["raw_value"].get("themes", []),
        config.get("full_config", {}).get("theme_taxonomy", {}),
    )

    if doc["raw_value"]["@type"] == "Plone Site":
        return None
    if doc["raw_value"]["@type"] == "File":
        rw = doc.get("raw_value", {})
        rw_file = rw.get("file", {}) or {}
        rw_file_ct = rw_file.get("content-type",'') or ''
        if (
            rw_file_ct
            not in ALLOWED_CONTENT_TYPES
        ):
            print("file content-type:")
            print(rw_file_ct)
            logger.info("file, but not in allowed list")
            return None
        else:
            doc["raw_value"]["format"] = doc["raw_value"]["file"][
                "content-type"
            ]
    doc["raw_value"]["hasWorkflowState"] = (
        doc["raw_value"].get("review_state", "visible") or "missing"
    )
    normalizer = config["normalizers"]
    # if has issued & no hasWorkflowState => set hasWorkflowState
    # if hasWorkflowState & no issued => set issued
    normalized_doc = create_doc(doc["raw_value"])
    normalized_doc = update_language(normalized_doc)
    normalized_doc = merge_types(normalized_doc)
    normalized_doc = update_locations(normalized_doc)
    attrs_to_delete = get_attrs_to_delete(
        normalized_doc, normalizer.get("proplist", [])
    )
    normalized_doc = add_reading_time_and_fulltext(
        normalized_doc,
        doc,
        config.get("nlp", {}).get("text", {}).get("whitelist", []),
        config.get("nlp", {}).get("text", {}).get("blacklist", []),
        config["site"].get("trafilatura", {}),
        config.get("site", {}).get("reading_time_blacklist", config.get("full_config",{}).get("reading_time_blacklist",[]))
    )

    normalized_doc = apply_black_map(
        normalized_doc, normalizer.get("blackMap", {})
    )
    normalized_doc = apply_white_map(
        normalized_doc, normalizer.get("whiteMap", {})
    )
    normalized_doc = remove_empty(normalized_doc)
    normalized_doc = apply_norm_obj(
        normalized_doc, normalizer.get("normObj", {})
    )
    normalized_doc = apply_norm_prop(
        normalized_doc, normalizer.get("normProp", {})
    )
    normalized_doc = fetch_geo_coverage(normalized_doc)
    normalized_doc = fetch_temporal_coverage(normalized_doc)
    normalized_doc = add_places(normalized_doc)
    normalized_doc = apply_norm_missing(
        normalized_doc, normalizer.get("normMissing", {})
    )

    # TODO e.g. File -> Corporate document if it contains xyz
    # normalized_doc = apply_types_detection(normalized_doc)

    normalized_doc = remove_duplicates(normalized_doc)

    normalized_doc = remove_extra_webpages(normalized_doc)

    normalized_doc = fix_state(normalized_doc)
    normalized_doc = addFormat(normalized_doc, doc)

    normalized_doc = delete_attrs(normalized_doc, attrs_to_delete)
    normalized_doc["original_id"] = normalized_doc["about"]
    normalized_doc = strip_fields(normalized_doc)
    dp = get_data_provenance(doc)
    normalized_doc['data_provenances'] = dp.get('data_provenances', [])
    normalized_doc['data_provenances_organisations'] = dp.get('data_provenances_organisations', [])

    # normalize objects again, after we add values in various ways (for spatial)
    normalized_doc = apply_norm_obj(
        normalized_doc, normalizer.get("normObj", {})
    )

    if not normalized_doc.get("description"):
        normalized_doc["description"] = " ".join(
            normalized_doc.get("fulltext", "").strip().split(" ")[:100]
        )

    return normalized_doc


def check_blacklist_whitelist(doc, blacklist, whitelist):
    if len(whitelist) > 0:
        if doc["raw_value"].get("@type", "") in whitelist:
            return True
    if len(blacklist) > 0:
        if doc["raw_value"].get("@type", "") not in blacklist:
            return True
    if len(whitelist) == 0 and len(blacklist) == 0:
        return True
    return False


def is_doc_on_path(loc, doc_loc):
    loc = loc.strip("*")
    if doc_loc.strip("/").find(loc.strip("/")) == 0:
        # if (
        #     doc_loc.strip("/").find(loc.strip("/")) == 0
        #     and len(doc_loc.strip("/").split("/"))
        #     == len(loc.strip("/").split("/")) + 1
        # ):
        return True
    return False


def is_doc_eq_path(loc, doc_loc):
    return loc.strip("/") == doc_loc.strip("/")


def find_ct_by_rules(doc_loc, rules, fallback):
    ct = []
    for rule in rules:
        if rule["path"].endswith("*"):
            if is_doc_on_path(rule["path"], doc_loc):
                ct = rule["ct"]
        else:
            if is_doc_eq_path(rule["path"], doc_loc):
                ct = rule["ct"]
    if len(ct) == 0:
        ct.append(fallback)
    return ct


def add_counts(doc):
    doc_with_counts = {}
    for key in doc.keys():
        doc_with_counts[key] = doc[key]
        if isinstance(doc[key], list):
            doc_with_counts[f"items_count_{key}"] = len(doc[key])
        else:
            doc_with_counts[f"items_count_{key}"] = 1
    return doc_with_counts
