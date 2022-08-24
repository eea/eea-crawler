from elasticsearch import Elasticsearch
from lib import elastic

from crawlers.registry import register_site_crawler, register_doc_crawler


def sdi_es(sdi_conf):
    econf = {
        "host": sdi_conf.get("endpoint"),
        "port": sdi_conf.get("port"),
        "schema": "https",
        "use_ssl": True,
    }

    if sdi_conf.get("authorization"):
        econf["headers"] = {"authorization": sdi_conf.get("authorization")}
    es = Elasticsearch([econf])

    return es


@register_site_crawler("sdi")
def parse_all_documents(v, site, sdi_conf, handler=None, doc_handler=None):
    query = sdi_conf.get("query")
    path = sdi_conf.get("path")
    es_sdi = sdi_es(sdi_conf)
    docs = elastic.get_docs(es=es_sdi, query=query, path=path)
    print("SDI DOCS")
    print(docs)
    es_docs = elastic.get_all_ids_from_raw_for_site(v, site)
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
        if es_doc_modified == doc_modified:
            print("Document did not change, skip indexing")
        else:
            print("Indexing")
            handler(v, site, sdi_conf, doc_id, doc_handler)

        if es_doc_modified is not None:
            del es_docs[doc_id]

    es = elastic.elastic_connection(v)
    elastic_conf = v.get("elastic")

    print("REMOVE FROM ES, DOCS THAT ARE NOT PRESENT IN SDI:")
    for doc_id in es_docs.keys():
        print(doc_id)
        elastic.delete_doc(es, elastic_conf.get("raw_index"), doc_id)


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
def crawl_doc(v, site, sdi_conf, metadataIdentifier, handler=None):
    doc = crawl_for_metadata_identifier(v, sdi_conf, metadataIdentifier)
    doc["children"] = []

    children = doc.get("agg_associated", [])
    if type(children) != list:
        children = [children]
    for child_id in children:
        child_doc = crawl_for_metadata_identifier(v, sdi_conf, child_id)
        if child_doc is not None:
            doc["children"].append(child_doc)

    raw_doc = prepare_doc_for_rabbitmq(doc, site)

    if handler:
        handler(v, raw_doc)
