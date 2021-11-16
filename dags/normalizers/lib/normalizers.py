import json
import re


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


def add_places(norm_doc):
    if norm_doc.get("spatial", None) is not None:
        norm_doc["places"] = norm_doc["spatial"]
    return norm_doc


def join_text_fields(json_doc, txt_props, txt_props_black):
    # json_doc = json.loads(doc)
    # start text with the document title.
    title = json_doc.get("title", "no title")
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


def add_reading_time(norm_doc, doc, txt_props=[], txt_props_black=[]):
    text = doc.get("web_text", "")
    if not text or len(text) == 0:
        text = join_text_fields(doc, txt_props, txt_props_black)
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
