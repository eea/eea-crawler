import json
from urllib.parse import urlparse

from airflow.decorators import task
from airflow.models import Variable
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError

from lib.debug import pretty_id
from lib.dagrun import trigger_dag
from tasks.helpers import find_site_by_url


def get_elastic_config():
    conf = {}
    conf["host"] = Variable.get("elastic_host")
    conf["port"] = Variable.get("elastic_port")
    conf["index"] = Variable.get("elastic_index")
    return conf


def connect(conf):
    es = Elasticsearch(host=conf["host"], port=conf["port"])
    return es


@task
def index_doc(doc):
    conf = get_elastic_config()
    es = connect(conf)
    es.index(index=conf["index"], id=doc["id"], body=doc)


@task
def create_index(config, add_embedding=False):
    return simple_create_index(config, add_embedding)


def simple_create_index(config, add_embedding=False):
    # TODO: check if index already exists
    timeout = 100
    es = Elasticsearch(
        [
            {
                "host": config["host"],
                "port": config["port"],
            }
        ],
        timeout=timeout,
    )
    if add_embedding:
        config["mapping"]["embedding"] = {
            "type": "dense_vector",
            "dims": 768,
        }
    # body = {"settings":config['elastic']['settings']}
    body = {
        "mappings": {"properties": config["mapping"]},
        "settings": config["settings"],
    }

    try:
        es.indices.create(index=config["target_index"], body=body)
    except RequestError as e:
        if e.error == "resource_already_exists_exception":
            print("Index already exists")
        else:
            raise (e)


@task
def handle_all_ids(config, dag_params, pool_name, dag_id, handler=None):
    timeout = 1000
    size = 500
    body = {"query": {"bool": {"must": [], "must_not": [], "should": []}}}

    if dag_params["params"].get("portal_type", "") != "":
        body["query"]["bool"]["must"].append(
            {"match": {"@type": dag_params["params"]["portal_type"]}}
        )

    if dag_params["params"].get("site", "") != "":
        site = dag_params["params"].get("site", "")

        sites = Variable.get("Sites", deserialize_json=True)
        site_config = Variable.get(sites[site], deserialize_json=True)
        site_loc = site_config["url"]
        body["query"]["bool"]["must"].append({"match": {"site": site_loc}})
    print("ES QUERY:")
    print(body)
    # Init Elasticsearch instance
    es = Elasticsearch(
        [
            {
                "host": config["host"],
                "port": config["port"],
            }
        ],
        timeout=timeout,
    )

    #    ids = []
    # Process hits here

    def process_hits(hits):
        for item in hits:
            # ids.append(item["_id"])
            print(item)
            params = {"item": item}
            params["item"] = item["_id"]
            #            dag_params["params"]["raw_d = item["_source"]["raw_value"]
            if not dag_params["params"].get("fast", False):
                trigger_dag(dag_id, params, pool_name)
            else:
                handler(params)

    # Check index exists
    if not es.indices.exists(index=config["raw_index"]):
        print("Index " + config["raw_index"] + " not exists")
        exit()

    # Init scroll by search
    data = es.search(
        index=config["raw_index"],
        scroll="60m",
        size=size,
        body=body,
        _source=["@id"],
    )

    # Get the scroll ID
    sid = data["_scroll_id"]
    scroll_size = len(data["hits"]["hits"])

    while scroll_size > 0:
        "Scrolling..."

        # Before scroll, process current batch of hits
        process_hits(data["hits"]["hits"])
        data = es.scroll(scroll_id=sid, scroll="60m")

        # Update the scroll ID
        sid = data["_scroll_id"]

        # Get the number of results that returned in the last scroll
        scroll_size = len(data["hits"]["hits"])


#   return ids


def get_doc_from_raw_idx(item, config):
    timeout = 1000
    es = Elasticsearch(
        [
            {
                "host": config["host"],
                "port": config["port"],
            }
        ],
        timeout=timeout,
    )
    res = es.get(index=config["raw_index"], id=item)
    doc = {
        "raw_value": json.loads(res["_source"]["raw_value"]),
        "web_text": res["_source"].get("web_text", ""),
        "pdf_text": res["_source"].get("pdf_text", ""),
    }

    return doc
