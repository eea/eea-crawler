import json
import re
from normalizers.lib.trafilatura_extract import get_text_from_html


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
                clean_data[key] = doc[
                    norm_missing[key].split("field:")[-1].strip()
                ]
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


def join_text_fields(json_doc, txt_props, txt_props_black):
    # json_doc = json.loads(doc)
    # start text with the document title.
    title = json_doc.get("title", "no title") or "no title"
    text = title + ".\n\n"

    # get other predefined fields first in the order defined in txt_props param
    for prop in txt_props:
        prop_v = json_doc.get(prop, {})
        if type(prop_v) is dict:
            txt = cleanhtml(prop_v.get("data", ""))
        else:
            txt = cleanhtml(prop_v)
        if not txt.endswith("."):
            txt = txt + "."
        # avoid redundant text
        if txt not in text:
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
            if txt and txt not in text:
                if not txt.endswith("."):
                    txt = txt + "."
                text = text + "\n\n" + k.upper() + ": " + txt + "\n\n"

    # TODO: for volto based content types with blocks, the above would not work,
    # a better approach would need to grab the rendered html page and strip the html. could be done for all content types.

    return text


def cleanhtml(raw_html):
    cleantext = ""
    if isinstance(raw_html, str):
        cleanr = re.compile("<.*?>")
        cleantext = re.sub(cleanr, "", raw_html)

    return cleantext


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


def add_reading_time(
    norm_doc, doc, txt_props=[], txt_props_black=[], trafilatura_config={}
):
    html = doc.get("web_html", "")
    text = get_text_from_html(html, trafilatura_config)
    if not text or len(text) == 0:
        text = join_text_fields(doc["raw_value"], txt_props, txt_props_black)
    pdf_text = doc.get("pdf_text", "")
    text += pdf_text
    wc = res = len(re.findall(r"\w+", text))
    norm_doc["readingTime"] = wc / 228
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
    geo_locations = [
        loc["label"] for loc in norm_doc.get("geo_coverage.geolocation", [])
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
    print("update_types")
    print(doc)
    if isinstance(doc.get("@components.object_provides"), str):
        doc["@components.object_provides"] = [
            doc["@components.object_provides"]
        ]
    if isinstance(doc.get("@type"), str):
        doc["@type"] = [doc["@type"]]
        doc["@type"] = doc["@type"] + doc["@components.object_provides"]
    print(doc)
    return doc


def common_normalizer(doc, config):
    normalizer = config["normalizers"]

    normalized_doc = create_doc(doc["raw_value"])
    normalized_doc = merge_types(normalized_doc)
    normalized_doc = update_locations(normalized_doc)
    attrs_to_delete = get_attrs_to_delete(
        normalized_doc, normalizer.get("proplist", [])
    )
    normalized_doc = add_reading_time(
        normalized_doc,
        doc,
        config["nlp"]["text"].get("blacklist", []),
        config["nlp"]["text"].get("whitelist", []),
        config["site"].get("trafilatura", {}),
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

    normalized_doc = delete_attrs(normalized_doc, attrs_to_delete)
    normalized_doc["original_id"] = normalized_doc["about"]
    normalized_doc = strip_fields(normalized_doc)
    return normalized_doc


def check_blacklist_whitelist(doc, blacklist, whitelist):
    if len(whitelist) > 0:
        if doc["raw_value"].get("@type", "") in whitelist:
            return True
    if len(blacklist) > 0:
        if doc["raw_value"].get("@type", "") not in blacklist:
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
