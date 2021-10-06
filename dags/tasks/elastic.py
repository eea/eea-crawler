import json

from airflow.decorators import task
from airflow.models import Variable
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError

from lib.debug import pretty_id


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
    timeout = 1000
    es = Elasticsearch(
        [
            {
                "host": config["elastic"]["host"],
                "port": config["elastic"]["port"],
            }
        ],
        timeout=timeout,
    )
    if add_embedding:
        config["elastic"]["mapping"]["embedding"] = {
            "type": "dense_vector",
            "dims": 768,
        }
    # body = {"settings":config['elastic']['settings']}
    body = {
        "mappings": {"properties": config["elastic"]["mapping"]},
        "settings": config["elastic"]["settings"],
    }

    try:
        es.indices.create(index=config["elastic"]["target_index"], body=body)
    except RequestError as e:
        if e.error == "resource_already_exists_exception":
            print("Index already exists")
        else:
            raise (e)

@task
def get_all_ids(config):
    timeout = 1000
    size = 1000
    body = {}
    if config.get("portal_type", "") != "":
        body = {
            "query": {
                "bool": {
                    "must": [{"match": {"@type": config["portal_type"]}}],
                    "must_not": [],
                    "should": [],
                }
            }
        }

    # Init Elasticsearch instance
    es = Elasticsearch(
        [
            {
                "host": config["elastic"]["host"],
                "port": config["elastic"]["port"],
            }
        ],
        timeout=timeout,
    )

    ids = []
    # Process hits here

    def process_hits(hits):
        for item in hits:
            # print(json.dumps(item, indent=2))
            ids.append(item["_id"])

    # Check index exists
    if not es.indices.exists(index=config["elastic"]["index"]):
        print("Index " + config["elastic"]["index"] + " not exists")
        exit()

    # Init scroll by search
    data = es.search(
        index=config["elastic"]["index"],
        scroll="2m",
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
        data = es.scroll(scroll_id=sid, scroll="2m")

        # Update the scroll ID
        sid = data["_scroll_id"]

        # Get the number of results that returned in the last scroll
        scroll_size = len(data["hits"]["hits"])

    return ids

def get_doc_from_raw_idx(item, config):
    timeout = 1000
    es = Elasticsearch(
        [
            {
                "host": config["elastic"]["host"],
                "port": config["elastic"]["port"],
            }
        ],
        timeout=timeout,
    )
    res = es.get(index=config["elastic"]["index"], id=pretty_id(item))
    return json.loads(res["_source"]["raw_value"])
