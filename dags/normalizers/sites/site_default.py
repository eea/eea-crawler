from normalizers.registry import (
    register_facets_normalizer,
    register_nlp_preprocessor,
)


@register_facets_normalizer("default")
def normalize_default(doc, config):
    pass


@register_nlp_preprocessor("default")
def preprocess_default(doc, config):
    pass
