from normalizers.lib.normalizers import join_text_fields
from urllib.parse import urlparse


def common_preprocess(doc, config):
    raw_doc = doc["raw_value"]
    text = doc.get("web_text", "")
    if len(text) == 0:
        text = join_text_fields(
            raw_doc,
            config["nlp"]["text"].get("blacklist", []),
            config["nlp"]["text"].get("whitelist", []),
        )
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
