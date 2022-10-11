from crawlers.registry import register_site_crawler, register_doc_crawler

from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import json

logger = logging.getLogger(__file__)
import requests

from lib import plone_rest_api, robots_txt
from lib import elastic

SKIP_EXTENSIONS = ["png", "svg", "jpg", "gif", "eps"]


@register_site_crawler("plone_rest_api")
def parse_all_documents(v, site, site_config, handler=None, doc_handler=None):
    rp = robots_txt.init(site_config)

    queries = plone_rest_api.build_queries_list(
        site_config, {"query_size": 500}
    )

    es_docs = elastic.get_all_ids_from_raw_for_site(v, site)

    portal_types = site_config.get("portal_types", [])

    cnt = 0
    for query in queries:
        docs = plone_rest_api.get_docs(query)
        for doc in docs:
            print(cnt)
            cnt += 1
            skip = False
            doc_id = plone_rest_api.get_no_api_url(site_config, doc["@id"])
            print(doc_id)
            doc_modified = doc.get(
                "modification_date", doc.get("modified", None)
            )

            if not robots_txt.test_url(rp, doc_id):
                skip = True
            if len(portal_types) > 0:
                if doc["@type"] not in portal_types:
                    skip = True
            if doc["@type"] == "File":
                if doc_id.split(".")[-1].lower() in SKIP_EXTENSIONS:
                    skip = True

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

    print("REMOVE FROM ES, DOCS THAT ARE NOT PRESENT IN PLONE:")
    for doc_id in es_docs.keys():
        print(doc_id)
        elastic.delete_doc(es, elastic_conf.get("raw_index"), doc_id)
        if v.get("enable_prepare_docs", False):
            elastic.delete_doc(
                es, elastic_conf.get("searchui_target_index"), doc_id
            )


def prepare_doc_for_rabbitmq(
    doc, scraped, pdf_text, doc_errors, site, site_config
):

    raw_doc = {}
    raw_doc["id"] = doc.get("id", "")
    raw_doc["@type"] = doc.get("@type", "")
    raw_doc["raw_value"] = doc

    if scraped:
        raw_doc["web_html"] = scraped

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

    scraped = ""
    if doc.get("@type", None) != "File":
        try:
            scraped = plone_rest_api.scrape(v, site_config, doc_id)
        except Exception:
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
