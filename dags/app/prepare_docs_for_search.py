#!/usr/bin/env python

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from lib import variables
    from lib import rabbitmq
else:
    from ..lib import variables
    from ..lib import rabbitmq

from normalizers import normalizer
import weaviate

def send_to_rabbitmq(v, doc):
    index_name = v.get("elastic", {}).get("searchui_target_index", None)
    if index_name is not None:
        doc["index_name"] = index_name
        rabbitmq_config = v.get("rabbitmq")
        rabbitmq.send_to_rabbitmq(doc, rabbitmq_config)

weaviate_client = None

def init_weaviate():
    client = weaviate.Client("http://localhost:8080")

    class_obj = {
      "class": "EEA_Docs",
      "description": "EEA Documents",
      "properties": [
         {
             "dataType": [
                 "string"
             ],
             "description": "Document ID",
             "name": "doc_id",
         },
         {
             "dataType": [
                 "string"
             ],
             "description": "The splitted doc ID",
             "name": "doc_split_id"
         },
         {
             "dataType": [
                 "string"
             ],
             "description": "The splitted document",
             "name": "text"
         },
         {
             "dataType": [
                 "vector"
             ],
             "description": "The vector representation of 'text'",
             "name": "embedding"
         }
      ]
    }
    client.schema.create_class(class_obj)
    return client

def store_in_weaviate(v, doc):
    import pdb; pdb.set_trace()

def prepare_doc(v, doc_id, site_id, doc_handler):
    raw_doc = normalizer.get_raw_doc_by_id(v, doc_id)
    normalizer.preprocess_doc(v, doc_id, site_id, raw_doc, doc_handler)


def prepare_docs(app):
    import pdb; pdb.set_trace()
    weaviate_client = init_weaviate()
    v = variables.load_variables_from_disk("../variables.json", app)

#    normalizer.parse_all_documents(v, prepare_doc, send_to_rabbitmq)
    normalizer.parse_all_documents(v, prepare_doc, store_in_weaviate)


if __name__ == "__main__":
    prepare_docs("global_search")
