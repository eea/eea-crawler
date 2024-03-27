from elasticsearch import Elasticsearch
from lib import elastic

from crawlers.registry import register_site_crawler, register_doc_crawler

OBSOLETE_KEYS = ["obsolete", "superseded"]


def isObsolete(doc):
    print("check_obsolete")
    obsolete = False
    status = doc.get("cl_status", None)
    if status is not None:
        if isinstance(status, list):
            if len(
                [
                    stat
                    for stat in status
                    if stat.get("key", "") in OBSOLETE_KEYS
                ]
            ):
                obsolete = True
        else:
            if status.get("key", None) in OBSOLETE_KEYS:
                obsolete = True
    print("obsolete")
    print(obsolete)
    return obsolete


def sdi_es(sdi_conf):
    econf = {
        "host": sdi_conf.get("endpoint"),
        "port": sdi_conf.get("port"),
        "schema": "https",
        "use_ssl": True,
    }

    if sdi_conf.get("authorization"):
        econf["headers"] = {"authorization": sdi_conf.get("authorization"), "accept": "application/json"}
    else:
        econf["headers"] = {"accept": "application/json"}
    es = Elasticsearch([econf])

    return es


@register_site_crawler("sdi")
def parse_all_documents(
    v, site, sdi_conf, handler=None, doc_handler=None, quick=False
):
    query = sdi_conf.get("query")
    path = sdi_conf.get("path")
    es_sdi = sdi_es(sdi_conf)
    threshold = sdi_conf.get("threshold", 25)
    ignore_delete_threshold = v.get("ignore_delete_threshold", False)
    print("SDI CONF")
    print(sdi_conf)
    print("SDI QUERY")
    print(query)
    docs = elastic.get_docs(es=es_sdi, query=query, path=path)
    print("SDI DOCS")
    print(docs)
    es_docs = elastic.get_all_ids_from_raw_for_site(v, site)
    prev_es_docs_len = len(es_docs)
    print("ES DOCS")
    print(es_docs)
    for doc in docs:
        doc_id = doc["_source"]["metadataIdentifier"]
        doc_modified = doc["_source"]["changeDate"]
        es_doc_modified = es_docs.get(doc_id, {}).get("modified", None)
        print("DOC:")
        print(doc_id)
        print(es_doc_modified)
        print(doc_modified)
        if es_doc_modified == doc_modified and not sdi_conf.get(
            "fetch_all_docs", False
        ):
            print("Document did not change, skip indexing")
        else:
            print("Indexing")
            handler(v, site, sdi_conf, doc_id, doc_handler)

        if es_doc_modified is not None:
            del es_docs[doc_id]

    to_delete_es_docs_len = len(es_docs)

    should_delete_old_docs = True

    if prev_es_docs_len == 0:
        should_delete_old_docs = True
    else:
        diff = to_delete_es_docs_len * 100 / prev_es_docs_len
        if diff > threshold:
            should_delete_old_docs = False

    if should_delete_old_docs or ignore_delete_threshold:
        es = elastic.elastic_connection(v)
        elastic_conf = v.get("elastic")

        print("REMOVE FROM ES, DOCS THAT ARE NOT PRESENT IN SDI:")
        for doc_id in es_docs.keys():
            print(doc_id)
            elastic.delete_doc(es, elastic_conf.get("raw_index"), doc_id)
            if v.get("enable_prepare_docs", False):
                elastic.delete_doc(
                    es, elastic_conf.get("searchui_target_index"), doc_id
                )
    else:
        raise Exception("WARNING: Too many documents to be deleted")


def crawl_for_metadata_identifier(v, sdi_conf, metadataIdentifier):
    es = sdi_es(sdi_conf)
    path = sdi_conf.get("path")
    doc = elastic.get_doc_by_id(
        es=es,
        path=path,
        item=metadataIdentifier,
        field="metadataIdentifier",
        excludes=["*.data"],
    )
    return doc


def prepare_doc_for_rabbitmq(doc, site):
    raw_doc = {}
    raw_doc["id"] = doc.get("metadataIdentifier", "")
    raw_doc["@type"] = "series"
    raw_doc["raw_value"] = doc
    raw_doc["site_id"] = site
    raw_doc["modified"] = doc.get("changeDate", None)
    return raw_doc


@register_doc_crawler("sdi")
def crawl_doc(v, site, sdi_conf, metadataIdentifier, handler=None, extra_opts=None):
    doc = crawl_for_metadata_identifier(v, sdi_conf, metadataIdentifier)
    doc["children"] = []

    children = doc.get("agg_associated", [])
    if type(children) != list:
        children = [children]
    for child_id in list(dict.fromkeys(children)):
        child_doc = crawl_for_metadata_identifier(v, sdi_conf, child_id)
#        if child_doc is not None and not isObsolete(child_doc):
        if child_doc is not None:
            if type(child_doc.get("linkProtocol", [])) != list:
                child_doc["linkProtocol"] = [child_doc["linkProtocol"]]
            doc["children"].append(child_doc)

    raw_doc = prepare_doc_for_rabbitmq(doc, site)

    if handler:
        handler(v, raw_doc)
    return {"raw_doc": raw_doc, "errors": []}
