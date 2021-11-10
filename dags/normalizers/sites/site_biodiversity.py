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


@register_facets_normalizer("biodiversity.europa.eu")
def normalize_biodiversity_europa_eu(doc, config):
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

    # TODO e.g. File -> Corporate document if it contains xyz
    # normalized_doc = apply_types_detection(normalized_doc)

    normalized_doc = remove_duplicates(normalized_doc)

    normalized_doc = delete_attrs(normalized_doc, attrs_to_delete)

    normalized_doc["cluster_name"] = "BISE (biodiversity.europa.eu)"

    return normalized_doc


@register_nlp_preprocessor("biodiversity.europa.eu")
def preprocess_biodiversity_europa_eu(doc, config):
    pass
