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


def send_to_rabbitmq(v, doc):
    rabbitmq_config = v.get("rabbitmq")
    rabbitmq_config["queue"] = rabbitmq_config["searchui_queue"]
    rabbitmq.send_to_rabbitmq(doc, rabbitmq_config)


def prepare_doc(v, doc_id, site_id, doc_handler):
    normalizer.preprocess_doc(v, doc_id, site_id, doc_handler)


def prepare_docs():
    v = variables.load_variables_from_disk("../variables.json")

    normalizer.parse_all_documents(v, prepare_doc, send_to_rabbitmq)


if __name__ == "__main__":
    prepare_docs()
