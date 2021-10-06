""" Given a source ES Doc id, cleanup, preprocess and write the doc in an ES index

(via Logstash)
"""

from elasticsearch import Elasticsearch
import json
import re
import requests
from urllib.parse import urlparse
from tenacity import retry, wait_exponential, stop_after_attempt


from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from lib.debug import pretty_id

from normalizers.defaults import normalizers
from normalizers.normalizers import simple_normalize_doc, join_text_fields
from tasks.helpers import simple_dag_param_to_dict, merge
from tasks.rabbitmq import simple_send_to_rabbitmq
from tasks.elastic import get_doc_from_raw_idx

default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "https://www.eea.europa.eu/api/SITE/highlights/better-raw-material-sourcing-can",
    "params": {
        "elastic": {"host": "elastic", "port": 9200, "index": "data_raw"},
        "rabbitmq": {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_nlp",
        },
        "nlp": {
            "services": {
                "embedding": {
                    "host": "nlp-embedding",
                    "port": "8000",
                    "path": "api/embedding",
                }
            },
            "text": {
                "props": ["description", "key_message", "summary", "text"],
                "blacklist": ["contact", "rights"],
                "split_length": 500,
            },
        },
        "normalizers": normalizers,
        "url_api_part": "api/SITE",
    },
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def prepare_doc_for_nlp(item=default_dag_params):
    transform_doc(item)


def get_haystack_data(json_doc, config):
    text = join_text_fields(json_doc, config)
    title = json_doc["title"]
    # metadata
    url = json_doc["@id"]
    uid = json_doc["UID"]
    content_type = json_doc["@type"]
    source_domain = urlparse(url).netloc

    # Archetype DC dates
    if "creation_date" in json_doc:
        creation_date = json_doc["creation_date"]
        publishing_date = json_doc.get("effectiveDate", "")
        expiration_date = json_doc.get("expirationDate", "")
    # Dexterity DC dates
    elif "created" in json_doc:
        creation_date = json_doc["created"]
        publishing_date = json_doc.get("effective", "")
        expiration_date = json_doc.get("expires", "")

    review_state = json_doc.get("review_state", "")

    # build haystack dict
    dict_doc = {
        "text": text,
        "meta": {
            "name": title,
            "url": url,
            "uid": uid,
            "content_type": content_type,
            "creation_date": creation_date,
            "publishing_date": publishing_date,
            "expiration_date": expiration_date,
            "review_state": review_state,
            "source_domain": source_domain,
        },
    }

    return dict_doc


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def preprocess_split_doc(doc, config):
    from haystack.preprocessor.preprocessor import PreProcessor

    preprocessor = PreProcessor(
        clean_empty_lines=True,
        clean_whitespace=True,
        clean_header_footer=False,
        split_by="word",
        split_length=config["split_length"],
        split_respect_sentence_boundary=True,
    )

    docs = preprocessor.process(doc)
    print(doc["meta"]["name"])
    print(f"n_docs_output: {len(docs)}")

    for tmp_doc in docs:
        tmp_doc["id"] = f"{tmp_doc['id']}#{tmp_doc['meta']['_split_id']}"

    return docs


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def add_embeddings_doc(docs, nlp_service):
    # data = {'snippets':[doc['text']], "is_passage": True}

    data = {"is_passage": True, "snippets": []}

    for doc in docs:
        data["snippets"].append(doc["text"])

    data = json.dumps(data)
    r = requests.post(
        f"http://{nlp_service['host']}:{nlp_service['port']}/{nlp_service['path']}",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        data=data,
    )

    embeddings = json.loads(r.text)["embeddings"]
    for doc in docs:
        for embedding in embeddings:
            if doc["text"] == embedding["text"]:
                doc["embedding"] = embedding["embedding"]
    return docs


@task
def transform_doc(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    # get a single document from elasticsearch or from the params
    if dag_params["params"].get("raw_doc", None):
        doc = dag_params["params"].get("raw_doc")
    else:
        doc = get_doc_from_raw_idx(dag_params["item"], dag_params["params"])

    # do the same normalization as we do for searchlib, so we can use the same filters
    normalized_doc = simple_normalize_doc(doc, dag_params["params"])

    # build the haystack document with text field and meta information
    haystack_data = get_haystack_data(doc, dag_params["params"]["nlp"]["text"])

    # merge the normalized document and the haystack document
    haystack_data = merge(normalized_doc, haystack_data)

    # split the document
    splitted_docs = preprocess_split_doc(
        haystack_data, dag_params["params"]["nlp"]["text"]
    )

    # add the embeddings
#    docs_with_embedding = add_embeddings_doc(
#        splitted_docs, dag_params["params"]["nlp"]["services"]["embedding"]
#    )

    # print(docs_with_embedding)

#    for doc in docs_with_embedding:
    for doc in splitted_docs:
        simple_send_to_rabbitmq(doc, dag_params["params"])


prepare_doc_for_nlp_dag = prepare_doc_for_nlp()


# @task
# def get_doc_from_raw_idx(item, config):
#     return simple_get_doc_from_raw_idx(item, config)
