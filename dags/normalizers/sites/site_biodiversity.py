from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)


@register_facets_normalizer("biodiversity.europa.eu")
def normalize_biodiversity_europa_eu(doc, config):
    pass


@register_nlp_preprocessor("biodiversity.europa.eu")
def preprocess_biodiversity_europa_eu(doc, config):
    pass
