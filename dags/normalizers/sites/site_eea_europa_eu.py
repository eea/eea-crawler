from urllib.parse import urlparse

from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)
from normalizers.lib.normalizers import (
    create_doc,
    update_locations,
    get_attrs_to_delete,
    add_reading_time,
    apply_black_map,
    apply_white_map,
    remove_empty,
    apply_norm_prop,
    apply_norm_obj,
    apply_norm_missing,
    add_places,
    remove_duplicates,
    delete_attrs,
    join_text_fields,
)


@register_facets_normalizer("www.eea.europa.eu")
def normalize_eea_europa_eu(doc, config):
    normalizer = config["normalizers"]

    normalized_doc = create_doc(doc["raw_value"])
    normalized_doc = update_locations(normalized_doc)
    attrs_to_delete = get_attrs_to_delete(
        normalized_doc, normalizer.get("proplist", [])
    )
    normalized_doc = add_reading_time(
        normalized_doc,
        doc,
        config["nlp"]["text"].get("blacklist", []),
        config["nlp"]["text"].get("whitelist", []),
    )
    normalized_doc = apply_black_map(
        normalized_doc, normalizer.get("blackMap", {})
    )
    normalized_doc = apply_white_map(
        normalized_doc, normalizer.get("whiteMap", {})
    )
    normalized_doc = remove_empty(normalized_doc)
    normalized_doc = apply_norm_obj(
        normalized_doc, normalizer.get("normObj", {})
    )
    normalized_doc = apply_norm_prop(
        normalized_doc, normalizer.get("normProp", {})
    )
    normalized_doc = add_places(normalized_doc)
    normalized_doc = apply_norm_missing(
        normalized_doc, normalizer.get("normMissing", {})
    )
    
    #TODO e.g. File -> Corporate document if it contains xyz
    #normalized_doc = apply_types_detection(normalized_doc)
        
    normalized_doc = remove_duplicates(normalized_doc)
    
    normalized_doc = delete_attrs(normalized_doc, attrs_to_delete)
    
    normalized_doc["cluster_name"] = "EEA Website (www.eea.europa.eu)"

    return normalized_doc


@register_nlp_preprocessor("www.eea.europa.eu")
def preprocess_eea_europa_eu(doc, config):
    raw_doc = doc["raw_value"]
    print("DOC:")
    print(doc)
    print("web_text")
    print(doc["web_text"])
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
