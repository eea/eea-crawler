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

from normalizers.registry import get_facets_normalizer, get_nlp_preprocessor

from tasks.helpers import simple_dag_param_to_dict, merge, find_site_by_url
from tasks.rabbitmq import simple_send_to_rabbitmq
from tasks.elastic import get_doc_from_raw_idx
from lib.variables import get_variable

default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "https://www.eea.europa.eu/api/SITE/highlights/water-stress-is-a-major",
    "params": {},
    "site": "",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Create embeddings for document",
)
def nlp_2_prepare_doc_for_nlp(item=default_dag_params):
    task_transform_doc(item)


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
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


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
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
def task_transform_doc(full_config):
    transform_doc(full_config)


def transform_doc(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    dag_variables = dag_params["params"].get("variables", {})
    site_map = dag_params.get("site_map", None)
    site = find_site_by_url(dag_params["item"], site_map)

    es = get_variable("elastic", dag_variables)
    rabbitmq = get_variable("rabbitmq", dag_variables)
    rabbitmq["queue"] = rabbitmq["nlp_queue"]

    # get a single document from elasticsearch or from the params
    if dag_params["params"].get("raw_doc", None):
        doc = {
            "raw_value": dag_params["params"].get("raw_doc"),
            "web_html": dag_params["params"].get("web_html", None),
            "pdf_text": dag_params["params"].get("pdf_text", ""),
        }
    else:
        doc = get_doc_from_raw_idx(dag_params["item"], es)

    # do the same normalization as we do for searchlib, so we can use the same filters
    sites = get_variable("Sites", dag_variables)

    site_config = get_variable(sites[site], dag_variables)
    normalizers_config = get_variable(
        site_config["normalizers_variable"], dag_variables
    )
    normalize = get_facets_normalizer(
        dag_params["item"], site_map, dag_variables
    )
    config = {
        "normalizers": normalizers_config,
        "nlp": site_config.get("nlp_preprocessing", None),
        "site": site_config,
    }
    normalized_doc = normalize(doc, config)
    if not normalized_doc:
        print("Should not be preprocessed & indexed for nlp")
        return
    normalized_doc["fulltext"] = None
    preprocess = get_nlp_preprocessor(
        dag_params["item"], site_map, dag_variables
    )
    haystack_data = preprocess(doc, config)

    # build the haystack document with text field and meta information
    #    haystack_data = get_haystack_data(doc, dag_params["params"]["nlp"]["text"])

    # merge the normalized document and the haystack document
    haystack_data = merge(normalized_doc, haystack_data)

    # split the document
    print("TRANSFORM_NLP")
    print(haystack_data)
    splitted_docs = preprocess_split_doc(haystack_data, config["nlp"]["text"])

    nlp_services = get_variable("nlp_services", dag_variables)
    # add the embeddings
    docs_with_embedding = add_embeddings_doc(
        splitted_docs, nlp_services["embedding"]
    )

    # print(docs_with_embedding)

    for doc_with_embedding in docs_with_embedding:
        doc_with_embedding["site_id"] = doc["raw_value"].get("site_id")
        simple_send_to_rabbitmq(doc_with_embedding, rabbitmq)


prepare_doc_for_nlp_dag = nlp_2_prepare_doc_for_nlp()


# @task
# def get_doc_from_raw_idx(item, config):
#     return simple_get_doc_from_raw_idx(item, config)
