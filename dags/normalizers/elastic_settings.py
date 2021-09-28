settings = {
    "index": {
        "mapping": {"ignore_malformed": "true"},
        "similarity": {"default": {"type": "BM25"}},
        "max_shingle_diff": "12",
        "analysis": {
            "filter": {
                "autocompleteFilter": {
                    "max_shingle_size": "12",
                    "min_shingle_size": "2",
                    "type": "shingle",
                    "filler_token": "",
                },
                "english_stemmer": {"type": "stemmer", "language": "english"},
            },
            "char_filter": {
                "url_filter": {
                    "type": "mapping",
                    "mappings": ["/ => \u0020", ". => \u0020"],
                }
            },
            "analyzer": {
                "default": {
                    "filter": [
                        "lowercase",
                        "stop",
                        "asciifolding",
                        "english_stemmer",
                    ],
                    "type": "custom",
                    "tokenizer": "standard",
                },
                "date2year": {"pattern": "[-](.*)", "type": "pattern"},
                "didYouMean": {
                    "filter": ["lowercase"],
                    "char_filter": ["html_strip"],
                    "type": "custom",
                    "tokenizer": "standard",
                },
                "freetext": {
                    "filter": ["lowercase"],
                    "char_filter": ["html_strip"],
                    "type": "custom",
                    "tokenizer": "standard",
                },
                "autocomplete": {
                    "filter": [
                        "lowercase",
                        "stop",
                        "autocompleteFilter",
                        "trim",
                    ],
                    "char_filter": ["html_strip"],
                    "type": "custom",
                    "tokenizer": "standard",
                },
                "pipe": {
                    "lowercase": "false",
                    "pattern": "\|",
                    "type": "pattern",
                },
                "none": {"filter": ["lowercase"], "type": "keyword"},
                "coma": {
                    "lowercase": "false",
                    "pattern": ", ",
                    "type": "pattern",
                },
                "semicolon": {
                    "lowercase": "false",
                    "pattern": "; ",
                    "type": "pattern",
                },
            },
            "tokenizer": {
                "url_tokenizer": {
                    "pattern": "[a-zA-Z0-9\.\-]*",
                    "type": "simple_pattern",
                }
            },
        },
    }
}
