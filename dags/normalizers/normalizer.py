from lib import elastic
from normalizers.registry import get_facets_normalizer, get_nlp_preprocessor
from normalizers.lib.nlp import preprocess_split_doc, add_embeddings_to_doc

def parse_all_documents_for_site(site_id, v, handler, doc_handler):
    print("HERE")
    print(site_id)
    raw_docs = elastic.get_all_ids_from_raw_for_site(v, site_id)
    for raw_doc in raw_docs.keys():
        should_index = True
        if len(raw_docs[raw_doc].get("errors")) > 0:
            should_index = False
        if should_index:
            #print(raw_docs[raw_doc])
#            if raw_doc == 'https://water.europa.eu/freshwater/nwrm-imported/measures/buffer-strips-and-hedges':
            handler(v, raw_doc, site_id, doc_handler)

def parse_all_documents(v, handler, doc_handler):
    raw_docs = elastic.get_all_ids_from_raw(v)
    search_docs = elastic.get_all_ids_from_searchui(v)
    for raw_doc in raw_docs.keys():
        should_index = True
        if (
            len(raw_docs[raw_doc].get("errors")) > 0
            and search_docs.get(raw_doc, None) is not None
        ):
            should_index = False
        if should_index:
            handler(v, raw_doc, raw_docs[raw_doc]["site_id"], doc_handler)


def get_raw_doc_by_id(v, doc_id):
    raw_doc = elastic.get_doc_from_raw_idx(v, doc_id)
    return raw_doc


def preprocess_doc(v, doc_id, site_id, raw_doc, doc_handler):
    print(f"{site_id} - {doc_id}")
    facets_normalizer = get_facets_normalizer(site_id)
    nlp_preprocessor = get_nlp_preprocessor(site_id)
    print(facets_normalizer)
    print(nlp_preprocessor)
    nlp_services = v.get("nlp_services")

    sites = v.get("Sites")
    site_config_variable = sites.get(site_id, None)
    site_config = v.get(site_config_variable)


    print(sites)
    print(site_config_variable)
    print(site_config)
    normalizers_config = v.get(
        site_config.get("normalizers_variable", "default_normalizers")
    )

    config = {
        "normalizers": normalizers_config,
        "nlp": site_config.get("nlp_preprocessing", {}),
        "site": site_config,
        "full_config": v,
    }
    # print(raw_doc)
    # import pdb; pdb.set_trace()
    raw_doc["raw_value"]["about"] = doc_id
    normalized_doc = facets_normalizer(raw_doc, config)
    if normalized_doc:
        if not v.get("metadata_only"):
            haystack_data = nlp_preprocessor(raw_doc, config)
            normalized_doc["fulltext"] = haystack_data.get("text", "")

            normalized_doc["site_id"] = raw_doc["raw_value"].get("site_id")

            # normalized_doc = preprocess_split_doc(
            #     normalized_doc,
            #     config["nlp"]["text"],
            #     field="fulltext",
            #     field_name="nlp_500",
            #     split_length=500,
            # )
            # normalized_doc = add_embeddings_to_doc(
            #     normalized_doc, nlp_services["embedding"], field_name="nlp_500"
            # )

            normalized_doc = preprocess_split_doc(
                normalized_doc, nlp_services["split"]
            )
            normalized_doc = add_embeddings_to_doc(
                normalized_doc, nlp_services["embedding"]
            )

            # normalized_doc = preprocess_split_doc(
            #     normalized_doc,
            #     config["nlp"]["text"],
            #     field="fulltext",
            #     field_name="nlp_100",
            #     split_length=100,
            # )
            # normalized_doc = add_embeddings_to_doc(
            #     normalized_doc, nlp_services["embedding"], field_name="nlp_100"
            # )
        doc_handler(v, normalized_doc)
