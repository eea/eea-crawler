from crawlers.registry import register_site_crawler, register_doc_crawler

from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import json

logger = logging.getLogger(__file__)
import requests

import urllib.parse
from lib import plone_rest_api, robots_txt, sitemap
from lib import elastic

SKIP_EXTENSIONS = ["png", "svg", "jpg", "gif", "eps", "jpeg"]


@register_site_crawler("singlepage")
def parse_all_documents(
    v, site, site_config, handler=None, doc_handler=None, quick=False
):
    doc = site_config.get('url')

    handler(v, site, site_config, doc, doc_handler, extra_opts={"url":site_config.get('url_to_parse', site_config.get('url'))})

    es = elastic.elastic_connection(v)
    elastic_conf = v.get("elastic")

def prepare_doc_for_rabbitmq(
    doc, scraped, pdf_text, doc_errors, site, site_config
):

    print("PREPARE:")
    print(doc)

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


@register_doc_crawler("singlepage")
def crawl_doc(v, site, site_config, doc_id, handler=None, extra_opts=None):
    doc_errors = []
    errors = []

    doc = {}
    doc["id"] = doc_id
    scraped = {}
    scrape_errors = False
    url_to_parse = extra_opts["url"]
    try:
        scraped = plone_rest_api.scrape(v, site_config, url_to_parse)
        if int(scraped.get("status_code", 0)) >= 400:
            print(f"status_code:", scraped.get("status_code", 0))
            scrape_errors = True
        final_url = scraped.get("final_url", doc_id)
        print("CHECK REDIRECT")
        print(f"url {url_to_parse}")
        print(f"final_url {final_url}")
        if (
            url_to_parse
            != final_url
        ):
            logger.exception(f"Redirected {url_to_parse} -> {final_url}")
            errors.append("document redirected")
            doc_errors.append("redirect")

    except Exception:
        scrape_errors = True
    if scrape_errors:
        logger.exception("Error scraping the page")
        errors.append("scraping the page")
        doc_errors.append("web")


    raw_doc = prepare_doc_for_rabbitmq(
        doc, scraped, "", doc_errors, site, site_config
    )
    if handler:
        handler(v, raw_doc)

    return {"raw_doc": raw_doc, "errors": doc_errors}
