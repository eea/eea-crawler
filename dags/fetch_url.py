import json
import re

from urllib.parse import urlparse
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from eea.rabbitmq.client import RabbitMQConnector
from unidecode import unidecode
from lib.debug import pretty_id
from plone_restapi_harvester.harvester.harvester import harvest_document
#from haystack.preprocessor.preprocessor import PreProcessor
#from haystack.document_store.elasticsearch import ElasticsearchDocumentStore


from tasks.debug import debug_value

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

# Simply remove tags from text strings
# TODO: we need to preserve the links used behind a-tags some times, 
# we ask question about what is the link for XYZ
def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext
  
@task()
def get_api_url(url):
    no_protocol_url = url.split("://")[-1]
    if '/api/' in no_protocol_url:
        print(no_protocol_url)
        return no_protocol_url
    url_parts = no_protocol_url.split("/")
    url_parts.insert(1, "api/SITE")
    url_with_api = "/".join(url_parts)
    print(url_with_api)
    return url_with_api

@task
def normalize_doc(doc):
    json_doc = json.loads(doc)
    normalized = harvest_document(json_doc)
    print('Original:', json_doc)
    print('Normalized:', normalized)

@task
def get_relevant_data(doc, item):
    json_doc = json.loads(doc)
    print('Type:', type(json_doc))
    print('Doc:', json_doc)

    data = {}
    data["title"] = json_doc.get("title", "no title")
    data["review_state"] = json_doc.get("review_state", "no state")
    data["modified"] = json_doc.get("modified", "not modified")
    data["UID"] = json_doc.get("UID")
    data["id"] = pretty_id(item)
    return data

@task
def get_haystack_data(doc, item):
    json_doc = json.loads(doc)
    print('Type:', type(json_doc))
    print('Doc:', json_doc)
    
    txt_props = ['description', 'key_message', 'summary', 'text']
    txt_props_black = ['contact', 'rights']
    
    # start text with the document title.
    title = json_doc.get("title", "no title")
    text = title + '.\n\n'
    
    # get other predefined fields first in the order defined in txt_props param
    for prop in txt_props:
        txt = cleanhtml(json_doc.get(prop, {}).get('data', ''))
        if not txt.endswith('.'):
            txt = txt + '.'
        #avoid redundant text
        if txt not in text:
           text = text + txt + '\n\n'
    
    # find automatically all props that have text or html in it
    # and append to text if not already there.
    for k, v in json_doc.items():
        if (type(v) is dict and k not in txt_props_black):
            txt = ''
            #print(f'%s is a dict' % k)
            mime_type = json_doc.get(k, {}).get('content-type','')
            if  mime_type == 'text/plain':
                #print('%s is text/plain' % k)
                txt = json_doc.get(k, {}).get('data', '')
            elif mime_type == 'text/html':
                #print('%s is text/html' % k)
                txt = cleanhtml(json_doc.get(k, {}).get('data', ''))
            #avoid redundant text
            if txt and txt not in text:
                if not txt.endswith('.'):
                    txt = txt + '.'
                text = text + '\n\n' + k.upper() + ': ' + txt + '\n\n'
            
    # TODO: further cleanup of text
    # If you're working with English text and don't have to worry about losing diacritics 
    # then maybe you can preprocess your text with unidecode.
    # https://githubmemory.com/repo/explosion/spacy-stanza/issues/68
    # https://towardsdatascience.com/cleaning-preprocessing-text-data-by-building-nlp-pipeline-853148add68a
    text = unidecode(text)
    
    # metadata
    url = json_doc["@id"]
    uid = json_doc['UID']
    content_type = json_doc["@type"]
    creation_date = json_doc["creation_date"]
    publishing_date = json_doc.get('effectiveDate','')
    expiration_date = json_doc.get('expirationDate','')
    review_state = json_doc.get('review_state','')
    
    # build haystack dict
    dict_doc = {"text": text, 
                "meta": {
                    "name": title, 
                    "url": url,
                    "uid": uid,
                    "content_type": content_type,
                    "creation_date": creation_date,
                    "publishing_date": publishing_date,
                    "expiration_date": expiration_date,
                    "review_state": review_state
                }
               }
    
    return dict_doc
    

@task()
def preprocess_split_doc(doc):
    from haystack.preprocessor.preprocessor import PreProcessor
    preprocessor = PreProcessor(
        clean_empty_lines=True,
        clean_whitespace=True,
        clean_header_footer=False,
        split_by="word",
        split_length=500,
        split_respect_sentence_boundary=True
    )

    docs = preprocessor.process(doc)
    print(doc["meta"]["name"])
    print(f"n_docs_output: {len(docs)}")

    return docs

@task()
def haystack_write_to_store(docs, es_hostname, es_index_name):
    #write to es index
    from haystack.document_store.elasticsearch import ElasticsearchDocumentStore
    document_store = ElasticsearchDocumentStore(host=es_hostname, username="", password="", index=es_index_name)
    document_store.write_documents(docs)
    
@task()
def update_embeddings(es_hostname, es_index_name, update_existing_embeddings = False):
    from haystack.document_store.elasticsearch import ElasticsearchDocumentStore
    document_store = ElasticsearchDocumentStore(host=es_hostname, username="", password="", index=es_index_name)
    from haystack.retriever.dense import DensePassageRetriever
    retriever = DensePassageRetriever(document_store=document_store,
                                  query_embedding_model="facebook/dpr-question_encoder-single-nq-base",
                                  passage_embedding_model="facebook/dpr-ctx_encoder-single-nq-base",
                                  max_seq_len_query=64,
                                  max_seq_len_passage=256,
                                  batch_size=16,
                                  use_gpu=True,
                                  embed_title=True,
                                  use_fast_tokenizers=True)
    document_store.update_embeddings(retriever, es_index_name, update_existing_embeddings = update_existing_embeddings)

@task()
def send_to_rabbitmq(doc):
    rabbit_config = {
        "rabbit_host": "rabbitmq",
        "rabbit_port": "5672",
        "rabbit_username": "guest",
        "rabbit_password": "guest",
    }
    queue_name = "es_indexing"

    rabbit = RabbitMQConnector(**rabbit_config)
    rabbit.open_connection()
    rabbit.declare_queue(queue_name)
    rabbit.send_message(queue_name, json.dumps(doc))
    rabbit.close_connection()


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def fetch_url(item: str = "https://myrul", es_hostname: str = "elastic", haystack_index_name: str = "haystack-docs"):
    """
    ### get info about an url
    """

    print("fetch_url", item)

    url_with_api = get_api_url(item)

    doc = SimpleHttpOperator(
        task_id="get_doc",
        method="GET",
        endpoint=url_with_api,
        headers={"Accept": "application/json"},
    )
    debug_value(doc.output)
    prepared_data = get_relevant_data(doc.output, item)
    haystack_data = get_haystack_data(doc.output, item)
    debug_value(haystack_data)
    docs = preprocess_split_doc(haystack_data)
    debug_value(docs)
    haystore = haystack_write_to_store(docs, es_hostname, haystack_index_name)
    update_embeddings(es_hostname, haystack_index_name) << haystore

#    for doc in docs:
#        send_to_rabbitmq(doc)
    #normalize_doc(doc.output)
    #send_to_rabbitmq(prepared_data)


fetch_url_dag = fetch_url()

#    helpers.debug_value(parent)
#    helpers.debug_value(doc.output)
#    helpers.debug_value(prepared_data)
#    helpers.show_dag_run_conf({"item": item})
# es_conf = elastic_helpers.get_elastic_config()
# es_conn = elastic_helpers.connect(es_conf)
#    elastic_helpers.index_doc(prepared_data)
# import requests
# from airflow.models import Variable
# from airflow.operators.python_operator import PythonOperator
# import elastic_helpers
# from xml.dom import minidom
# from ../scripts import crawler
