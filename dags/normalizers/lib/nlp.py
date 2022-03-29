import json
import requests

from tenacity import retry, wait_exponential, stop_after_attempt
from haystack.preprocessor.preprocessor import PreProcessor

from normalizers.lib.normalizers import join_text_fields
from urllib.parse import urlparse
from normalizers.lib.trafilatura_extract import get_text_from_html


def common_preprocess(doc, config):
    raw_doc = doc["raw_value"]
    html = doc.get("web_html", "")
    text = get_text_from_html(html, config["site"].get("trafilatura", {}))
    if not text or len(text) == 0:
        text = join_text_fields(
            text,
            raw_doc,
            config["nlp"]["text"].get("blacklist", []),
            config["nlp"]["text"].get("whitelist", []),
        )
    pdf_text = doc.get("pdf_text", "")

    text += "\n\n" + pdf_text
    title = raw_doc["title"]
    # metadata
    url = raw_doc["@id"]
    uid = raw_doc["UID"]
    content_type = raw_doc["@type"]
    source_domain = urlparse(url).netloc

    # Archetype DC dates
    if "creation_date" in raw_doc:
        creation_date = raw_doc["creation_date"]
        publishing_date = raw_doc.get("effectiveDate", "")
        expiration_date = raw_doc.get("expirationDate", "")
    # Dexterity DC dates
    elif "created" in raw_doc:
        creation_date = raw_doc["created"]
        publishing_date = raw_doc.get("effective", "")
        expiration_date = raw_doc.get("expires", "")

    review_state = raw_doc.get("review_state", "")

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

@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def preprocess_split_doc(doc, config, field="text", field_name="nlp", split_length=500):
    preprocessor = PreProcessor(
        clean_empty_lines=True,
        clean_whitespace=True,
        clean_header_footer=False,
        split_by="word",
        #split_length=config["split_length"],
        split_length=split_length,
        split_respect_sentence_boundary=True,
    )
    tmp_doc = {'content': doc.get(field,'')}
#    doc["content"] = doc.get('text', '')

    docs = preprocessor.process(tmp_doc)

    # for tmp_doc in docs:
    #     tmp_doc["id"] = f"{tmp_doc['id']}#{tmp_doc['meta']['_split_id']}"
    doc[field_name] = []
    for tmp_doc in docs:
        doc[field_name].append({'text':tmp_doc['content']})

    return doc

@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def add_embeddings_to_doc(doc, nlp_service, field_name="nlp"):
    # data = {'snippets':[doc['text']], "is_passage": True}

    data = {"is_passage": True, "snippets": []}
    for content in doc[field_name]:
        data["snippets"].append(content["text"])

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
    for content in doc[field_name]:
        for embedding in embeddings:
            if content["text"] == embedding["text"]:
                content["embedding"] = embedding["embedding"]
    return doc
