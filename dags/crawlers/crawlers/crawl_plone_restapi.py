from crawlers.registry import register_site_crawler, register_doc_crawler

from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import json

logger = logging.getLogger(__file__)
import requests

import urllib.parse
from lib import plone_rest_api, robots_txt
from lib import elastic

SKIP_EXTENSIONS = ["png", "svg", "jpg", "gif", "eps", "jpeg"]


@register_site_crawler("plone_rest_api")
def parse_all_documents(
    v, site, site_config, handler=None, doc_handler=None, quick=False
):
    print(site)
    print(quick)

    urls_whitelist = site_config.get("urls", {}).get("whitelist", [])
    urls_blacklist = site_config.get("urls", {}).get("blacklist", [])
    rp = robots_txt.init(site_config)

    queries = plone_rest_api.build_queries_list(
        site_config, {"query_size": 500, "quick": quick}
    )

    print(queries)

    threshold = site_config.get("threshold", 25)
    ignore_delete_threshold = v.get("ignore_delete_threshold", False)

    es_docs = elastic.get_all_ids_from_raw_for_site(v, site)
    prev_es_docs_len = len(es_docs)

    portal_types = site_config.get("portal_types", [])
    types_blacklist = site_config.get("types_blacklist", [])
    print("TYPES BLACKLIST")
    print(types_blacklist)
    skip_docs = v.get("skip_docs", [])
    print("skip docs")
    print(skip_docs)
    cnt = 0
    for query in queries:
        docs = plone_rest_api.get_docs(query)
        for doc in docs:
            print(cnt)
            cnt += 1
            skip = False
            doc_id = plone_rest_api.get_no_api_url(site_config, doc["@id"])
            print(doc_id)
            print(doc["@type"])
            print(portal_types)
            doc_modified = doc.get(
                "modification_date", doc.get("modified", None)
            )

            if len(urls_whitelist) > 0:
                if doc_id not in urls_whitelist:
                    print("Document not in whitelist, skip indexing")
                    skip = True

            if len(urls_blacklist) > 0:
                if doc_id in urls_blacklist:
                    print("Document in blacklist, skip indexing")
                    skip = True

            if not robots_txt.test_url(rp, doc_id):
                print("skip because of robots.txt")
                skip = True
            if len(portal_types) > 0:
                if doc["@type"] not in portal_types and urllib.parse.quote(doc["@type"]) not in portal_types:
                    print("skip because not in portal_types")
                    skip = True
            if doc["@type"] == "File":
                if doc_id.split(".")[-1].lower() in SKIP_EXTENSIONS:
                    print("skip because wrong file type")
                    skip = True
            if doc["@type"] in types_blacklist:
                print("skip because type is blacklisted")
                skip = True
            if doc_id in skip_docs:
                print("Document had errors, skip")
                skip = True
                del es_docs[doc_id]
            if not skip:
                #                import pdb; pdb.set_trace()
                es_doc = es_docs.get(doc_id, {})
                es_doc_modified = es_doc.get("modified", None)
                es_doc_errors = es_doc.get("errors", None)

                #                if es_doc_modified == doc_modified:
                if es_doc_modified == doc_modified and len(es_doc_errors) == 0:
                    print("Document did not change, skip indexing")
                else:
                    print("Should be indexed")
                    print(es_doc_modified)
                    print(doc_modified)
                    print(es_doc_errors)
                    handler(v, site, site_config, doc_id, doc_handler)
                if es_doc_modified is not None or es_doc_errors is not None:
                    del es_docs[doc_id]

    es = elastic.elastic_connection(v)
    elastic_conf = v.get("elastic")

    if quick:
        print(
            "Quick sync enabled, ignore removing documents from elasticsearch"
        )
        return

    to_delete_es_docs_len = len(es_docs)

    should_delete_old_docs = True

    if prev_es_docs_len == 0:
        should_delete_old_docs = True
    else:
        diff = to_delete_es_docs_len * 100 / prev_es_docs_len
        if diff > threshold:
            should_delete_old_docs = False

    if should_delete_old_docs or ignore_delete_threshold:
        print("REMOVE FROM ES, DOCS THAT ARE NOT PRESENT IN PLONE:")
        print(es_docs.keys())
        for doc_id in es_docs.keys():
            print(doc_id)
            elastic.delete_doc(es, elastic_conf.get("raw_index"), doc_id)
            if v.get("enable_prepare_docs", False):
                try:
                    elastic.delete_doc(
                        es, elastic_conf.get("searchui_target_index"), doc_id
                    )
                    print("document deleted from search index")
                except:
                    print("document not found in search index")
    else:
        raise Exception("WARNING: Too many documents to be deleted")


def prepare_doc_for_rabbitmq(
    doc, scraped, pdf_text, doc_errors, site, site_config
):

    raw_doc = {}
    raw_doc["id"] = doc.get("id", "")
    raw_doc["@type"] = doc.get("@type", "")
    raw_doc["raw_value"] = doc

    if scraped.get("downloaded", None) is not None:
        raw_doc["web_html"] = scraped.get("downloaded", "")
    if scraped.get("status_code", None) is not None:
        raw_doc["status_code"] = scraped.get("status_code", 0)

    if pdf_text:
        raw_doc["pdf_text"] = pdf_text

    raw_doc["original_id"] = doc["id"]
    raw_doc["site_id"] = site
    raw_doc["errors"] = doc_errors
    raw_doc["modified"] = doc.get(
        "modified", doc.get("modification_date", None)
    )
    raw_doc["site"] = site_config["url"]
    raw_doc["indexed_at"] = datetime.now().isoformat()

    return raw_doc


@register_doc_crawler("plone_rest_api")
def crawl_doc(v, site, site_config, doc_id, handler=None):
    doc_errors = []
    errors = []

    try:
        r = plone_rest_api.get_doc_from_plone(site_config, doc_id)
        assert json.loads(r)["@id"]
    except Exception:
        logger.exception("retrieving json from api")
        errors.append("retrieving json from api")
        doc_errors.append("json")
        r = json.dumps({"@id": doc_id})

    doc = json.loads(r)
    doc["id"] = doc_id
    scraped = {}
    if doc.get("@type", None) != "File":
        scrape_errors = False
        try:
            scraped = plone_rest_api.scrape(v, site_config, doc_id)
            if int(scraped.get("status_code", 0)) >= 400:
                print(f"status_code:", scraped.get("status_code", 0))
                scrape_errors = True
            final_url = scraped.get("final_url", doc_id)
            print("CHECK REDIRECT")
            print(f"url {doc_id}")
            print(f"final_url {final_url}")
            if (
                doc_id.split("?")[0].split("#")[0]
                != final_url.split("?")[0].split("#")[0]
            ):
                logger.exception(f"Redirected {doc_id} -> {final_url}")
                errors.append("document redirected")
                doc_errors.append("redirect")

        except Exception:
            scrape_errors = True
        if scrape_errors:
            logger.exception("Error scraping the page")
            errors.append("scraping the page")
            doc_errors.append("web")

    pdf_text = ""
    try:
        pdf_text = plone_rest_api.extract_pdf(v, site_config, doc)
    except Exception:
        logger.exception("Error converting pdf file")
        errors.append("converting pdf file")
        doc_errors.append("pdf")

    raw_doc = prepare_doc_for_rabbitmq(
        doc, scraped, pdf_text, doc_errors, site, site_config
    )
    if handler:
        handler(v, raw_doc)

    return {"raw_doc": raw_doc, "errors": doc_errors}
