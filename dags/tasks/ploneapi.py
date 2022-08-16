SKIP_EXTENSIONS = ["png", "svg", "jpg", "gif", "eps"]
from ..lib.variables import get_variable
import json


def extract_docs_from_json(page, task_params):
    params = task_params
    site_config_variable = get_variable("Sites").get(params["site"], None)
    site_config = get_variable(site_config_variable)
    types_blacklist = site_config.get("types_blacklist", [])
    print("TYPES BLACKLIST:")
    print(types_blacklist)
    json_doc = json.loads(page)
    print("EXTRACT_DOCS_FROM_JSON")
    print(json_doc)
    docs_from_json = json_doc["items"]

    docs = []
    for doc in docs_from_json:
        skip = False
        portal_types = site_config.get("portal_types", [])
        if len(portal_types) > 0:
            if doc["@type"] not in portal_types:
                skip = True
        if doc["@type"] == "File":
            if doc["@id"].split(".")[-1].lower() in SKIP_EXTENSIONS:
                skip = True

        if not skip:
            docs.append(doc)
    urls = [doc["@id"] for doc in docs if doc["@type"] not in types_blacklist]
    print("Number of documents:")
    print(len(urls))

    for item in urls:
        print(item)

    return urls


def extract_next_from_json(page, task_params):
    params = task_params
    site_config_variable = get_variable("Sites").get(params["site"], None)
    site_config = get_variable(site_config_variable)

    if params.get("trigger_next_bulk", False):
        json_doc = json.loads(page)
        if json_doc.get("batching", {}).get("next", False):
            next_url = json_doc.get("batching", {}).get("next")
            if site_config.get("fix_items_url", None):
                if site_config["fix_items_url"]["without_api"] in next_url:
                    next_url = next_url.replace(
                        site_config["fix_items_url"]["without_api"],
                        site_config["fix_items_url"]["with_api"],
                    )
            print("NEXT URL:")
            print(next_url)
            return [next_url]

    return []


def get_docs(site):
    docs = []
    xc_resp = http_request(xc_item)
    xc_urls = extract_docs_from_json(xc_resp, xc_params)
    xc_next = extract_next_from_json(xc_resp, xc_params)

    return docs


def get_all_docs():
    docs = []
    sites = get_variable("Sites")
    for site in sites:
        docs = docs + get_docs(site)
    return docs
