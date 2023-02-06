from datetime import datetime, timedelta
import json
import logging
import requests
import magic
from tenacity import retry, stop_after_attempt, wait_exponential
from urllib.parse import urlunsplit

logger = logging.getLogger(__file__)


def get_api_url(site_config, url):
    if url.find("www.eea.europa.eu") > -1:
        if url.find("/api/") > -1:
            return url

    if site_config.get("fix_items_url", None):
        if f'{site_config["fix_items_url"]["without_api"]}/' in url:
            url = url.replace(
                site_config["fix_items_url"]["without_api"],
                site_config["fix_items_url"]["with_api"],
            )
        return url

    if site_config["url_api_part"].strip("/") == "":
        return url

    if site_config["url_api_part"] in url:
        logger.info(
            "Found url_api_part (%s) in url: %s",
            site_config["url_api_part"],
            url,
        )
        return url

    url_parts = url.split("/")
    if "://" in url:
        url_parts.insert(3, site_config["url_api_part"])
    else:
        url_parts.insert(1, site_config["url_api_part"])

    return "/".join(url_parts)


def get_no_api_url(site_config, url):
    if site_config.get("fix_items_url", None):
        if f'{site_config["fix_items_url"]["without_api"]}/' in url:
            return url
        if f'{site_config["fix_items_url"]["with_api"]}/' in url:
            return url.replace(
                site_config["fix_items_url"]["with_api"],
                site_config["fix_items_url"]["without_api"],
            )
        with_api2 = site_config["fix_items_url"].get("with_api2", None)
        if with_api2 is not None and f"{with_api2}/" in url:
            return url.replace(
                site_config["fix_items_url"]["with_api2"],
                site_config["fix_items_url"]["without_api"],
            )

    url_parts = url.split("://")
    protocol = url_parts[0]
    url = url_parts[1]

    ret_url = "/".join(url.split("/" + site_config["url_api_part"] + "/"))

    # Handle languages
    if url.find("www.eea.europa.eu") > -1:
        if url.find("/api/") > -1:
            ret_url = "/".join(ret_url.split("/api/"))

    return f"{protocol}://{ret_url}"


def build_queries_list(site_config, query_config):
    query_limit = ""
    if query_config.get("quick", False):
        today = datetime.now()
        yesterday = today - timedelta(1)
        query_limit = f"&modified.query:date={yesterday.strftime('%m-%d-%Y')}&modified.range=min"
    url = site_config["url"].strip("/")
    if site_config.get("fix_items_url", None):
        if site_config["fix_items_url"]["without_api"] in url:
            url = url.replace(
                site_config["fix_items_url"]["without_api"],
                site_config["fix_items_url"]["with_api"],
            )
    else:
        url_api_part = site_config["url_api_part"].strip("/")
        if url_api_part != "":
            url = f"{url}/{url_api_part}"

    if site_config.get("portal_types", None):
        # queries = []
        queries = [
            f"{url}/@search?b_size={query_config['query_size']}&metadata_fields=modification_date&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date&portal_type={portal_type}{query_limit}"
            for portal_type in site_config["portal_types"]
        ]
        if site_config.get("languages", None):
            for language in site_config.get("languages"):
                queries.append(
                    f"{url}/{language}/@search?b_size={query_config['query_size']}&metadata_fields=modification_date&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date{query_limit}"
                )
    else:
        queries = [
            f"{url}/@search?b_size={query_config['query_size']}&metadata_fields=modification_date&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date{query_limit}"
        ]
    return queries


@retry(wait=wait_exponential(), stop=stop_after_attempt(3))
def request_with_retry(url, method="get", data=None):
    print("Query:")
    print(url)
    print("-------------")
    logger.info("Fetching %s", url)
    handler = getattr(requests, method)
    resp = ""
    try:
        resp = handler(
            url,
            headers={"Accept": "application/json"},
            data=json.dumps(data),
            timeout=120,
        )
        logger.info("Response: %s", resp.text)
    except:
        logger.info("Timeout")

    assert json.loads(resp.text)  # test if response is json
    logger.info("Response is valid json")

    return resp.text


def execute_query(query):
    while True:
        resp = request_with_retry(query)
        docs = json.loads(resp)
        for doc in docs["items"]:
            yield (doc)
        next_query = docs.get("batching", {}).get("next", False)
        if next_query:
            query = next_query
        else:
            break


def get_docs(query):
    docs = execute_query(query)
    for doc in docs:
        yield (doc)


def get_doc_from_plone(site_config, doc_id):
    url_with_api = get_api_url(site_config, doc_id)
    r_url = f"{url_with_api}?expand=object_provides"
    if site_config.get("avoid_cache_api", False):
        dt = datetime.now()
        # dts = datetime.strptime(
        #         dt.split("T")[0], "%Y-%m-%d"
        #     )
        r_url = f"{r_url}&crawler={dt}"

    r = request_with_retry(r_url)

    return r


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def scrape_with_retry(v, url, js=False):
    logger.info("Scraping url: %s", url)
    hc = v.get("headless_chrome").get("endpoint")
    if js:
        resp = requests.post(
            hc,
            headers={"Content-Type": "application/json"},
            data=f'{{"url":"{url}", "js":true,"raw":true}}',
        )
        downloaded = resp.text
        status = resp.status_code
        final_url = resp.headers.get("final_url", "")
    else:
        resp = requests.get(url)
        downloaded = resp.text
        status = resp.status_code
        final_url = resp.headers.get("final_url", "")

    if magic.from_buffer(downloaded) == "data":
        return {
            "downloaded": None,
            "status_code": status,
            "final_url": final_url,
        }

    logger.info("Downloaded: %s", downloaded)

    return {
        "downloaded": downloaded,
        "status_code": status,
        "final_url": final_url,
    }


def scrape(v, site_config, doc_id):
    url_without_api = get_no_api_url(site_config, doc_id)
    scrape = False
    s_url = ""
    scrape_with_js = False
    if site_config.get("scrape_pages", False):
        s_url = url_without_api
        scrape_with_js = site_config.get("scrape_with_js", False)
        scrape = True
    response = {}
    if scrape:
        if site_config.get("avoid_cache_web", False):
            dt = datetime.now()
            # dts = datetime.strptime(
            #         dt.split("T")[0], "%Y-%m-%d"
            #     )
            s_url = f"{url_without_api}?scrape={dt}"
        response = scrape_with_retry(v, s_url, scrape_with_js)
    return response


FIELD_MARKERS = {"file": {"content-type", "download", "filename"}}


def is_field_of_type(field, _type):
    if not isinstance(field, dict):
        return False

    if _type not in FIELD_MARKERS:
        return False

    return set(field.keys()).issuperset(FIELD_MARKERS[_type])


def fix_download_url(download_url, source_url):
    if "www.eea.europa.eu" in source_url:
        return download_url.replace("@@download", "at_download")
    return download_url


def extract_attachments(json_doc, nlp_service_params):

    url = json_doc["id"]
    params = nlp_service_params["converter"]

    logger.info("Extract attachments %s %s", json_doc, url)

    converter_dsn = urlunsplit(
        ("http", params["host"] + ":" + params["port"], params["path"], "", "")
    )

    text_fragments = []

    for name, value in json_doc.items():
        if (
            is_field_of_type(value, "file")
            and value["content-type"] == "application/pdf"
        ):
            download_url = fix_download_url(value["download"], url)
            logger.info("Download url found: %s", download_url)
            try:
                resp = request_with_retry(
                    converter_dsn, "post", {"url": download_url}
                )
            except Exception:
                logger.exception("failed pdf extraction, retry")
                download_url = value["download"]
                logger.info("Retry with download url: %s", download_url)
                resp = request_with_retry(
                    converter_dsn, "post", {"url": download_url}
                )
            if isinstance(resp, str):
                resp = json.loads(resp)
            for doc in resp["documents"]:
                text_fragments.append(doc["text"].strip())

    text = "\n".join(text_fragments)

    logger.info("Retrieved file content: %r", text)

    return text


def extract_pdf(v, site_config, doc):
    pdf_text = ""
    nlp_service_params = v.get("nlp_services")
    should_extract_pdf = True
    if site_config.get("pdf_days_limit", 0) > 0:
        current_date = datetime.now()
        logger.info("CHECK DATE")
        mod_date_str = doc.get("modification_date", None)
        if mod_date_str:
            mod_date = datetime.strptime(
                mod_date_str.split("T")[0], "%Y-%m-%d"
            )
            logger.info(current_date)
            logger.info(mod_date)
            diff = current_date - mod_date
            delta = diff.days
            logger.info(delta)
            logger.info(site_config.get("pdf_days_limit"))
            if delta > site_config.get("pdf_days_limit"):
                should_extract_pdf = False
    if should_extract_pdf:
        logger.info("EXTRACT PDF")
        pdf_text = extract_attachments(doc, nlp_service_params)
    return pdf_text
