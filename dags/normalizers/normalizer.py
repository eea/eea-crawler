from lib import elastic

def parse_all_documents(v, handler, doc_handler):
    raw_docs = elastic.get_all_ids_from_raw(v) 
    search_docs = elastic.get_all_ids_from_searchui(v)
    for raw_doc in raw_docs.keys():
        should_index = True
        if len(raw_docs[raw_doc].get("errors")) > 0 and search_docs.get(raw_doc, None) is not None:
            should_index = False
        if should_index:
            handler(v, raw_doc, raw_docs[raw_doc]["site_id"], doc_handler)
