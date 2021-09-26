mapping = {
    "properties": {
        "SearchableText": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "references": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "year": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "date2year",
            "type": "text"
        },
        "all_fields_for_freetext": {
            "copy_to": ["text"],
            "fielddata": True,
            "analyzer": "freetext",
            "type": "text"
        },
        "rdf_lastRefreshed": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "subject": {
            "type": "text",
            "fields": {
                "keyword": {
                    "ignore_above": 256,
                    "type": "keyword"
                }
            }
        },
        "searchable_organisation": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "about": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text",
            "fields": {
                    "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                    }
            }
        },
        "items_count_places": {
            "type": "long"
        },
        "organisation": {
            "copy_to": ["did_you_mean", "searchable_organisation", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "searchable_time_coverage": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "items_count_organisation": {
            "type": "long"
        },
        "language": {
            "null_value": "en",
            "type": "keyword"
        },
        "fleschReadingEaseScore": {
            "type": "double"
        },
        "type": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "items_count_fleschReadingEaseScore": {
            "type": "long"
        },
        "items_count_expires": {
            "type": "long"
        },
        "creator_cms": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "items_count_creator_cms": {
            "type": "long"
        },
        "items_count_language": {
            "type": "long"
        },
        "hasWorkflowState": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "items_count_SearchableText": {
            "type": "long"
        },
        "items_count_time_coverage": {
            "type": "long"
        },
        "modified": {
            "type": "date"
        },
        "time_coverage": {
            "copy_to": ["searchable_time_coverage", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "date2year",
            "type": "text"
        },
        "items_count_dataProvider": {
            "type": "long"
        },
        "text": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "issued": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text",
            "fields": {
                    "date": {
                        "type": "date"
                    },
                "toindex": {
                        "fielddata": True,
                        "type": "text"
                    },
                "index": {
                        "type": "keyword"
                    }
            }
        },
        "spatial": {
            "copy_to": ["did_you_mean", "searchable_spatial", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "items_count_creator": {
            "type": "long"
        },
        "items_count_objectProvides": {
            "type": "long"
        },
        "format": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "items_count_modified": {
            "type": "long"
        },
        "items_count_label": {
            "type": "long"
        },
        "SearchableText_embeddings": {
            "dims": 768,
            "type": "dense_vector"
        },
        "dataProcessor": {
            "copy_to": ["organisation", "searchable_organisation", "all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "searchable_objectProvides": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "did_you_mean": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "didYouMean",
            "type": "text"
        },
        "topic": {
            "copy_to": ["did_you_mean", "searchable_topics"],
            "null_value": "Various other issues",
            "type": "keyword"
        },
        "items_count_text": {
            "type": "long"
        },
        "items_count_subject": {
            "type": "long"
        },
        "searchable_spatial": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "expires": {
            "type": "date"
        },
        "items_count_dataProcessor": {
            "type": "long"
        },
        "searchable_topics": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "items_count_title": {
            "type": "long"
        },
        "description": {
            "type": "text",
            "fields": {
                "keyword": {
                    "ignore_above": 256,
                    "type": "keyword"
                }
            }
        },
        "items_count_year": {
            "type": "long"
        },
        "items_count_cluster_id": {
            "type": "long"
        },
        "items_count_type": {
            "type": "long"
        },
        "title": {
            "copy_to": ["did_you_mean", "autocomplete", "all_fields_for_freetext"],
            "fielddata": True,
            "type": "text",
            "fields": {
                    "toindex": {
                        "fielddata": True,
                        "type": "text"
                    },
                "eea_title": {
                        "type": "keyword"
                    },
                "index": {
                        "type": "keyword"
                    }
            }
        },
        "cluster_id": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "objectProvides": {
            "copy_to": ["did_you_mean", "searchable_objectProvides"],
            "null_value": "Other",
            "type": "keyword"
        },
        "readingTime": {
            "type": "double"
        },
        "items_count_readingTime": {
            "type": "long"
        },
        "items_count_cluster_name": {
            "type": "long"
        },
        "cluster_name": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "creator": {
            "type": "text",
            "fields": {
                "keyword": {
                    "ignore_above": 256,
                    "type": "keyword"
                }
            }
        },
        "items_count_topic": {
            "type": "long"
        },
        "autocomplete": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "autocomplete",
            "type": "text"
        },
        "items_count_references": {
            "type": "long"
        },
        "searchable_places": {
            "copy_to": ["all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "label": {
            "copy_to": ["did_you_mean", "all_fields_for_freetext"],
            "fielddata": True,
            "type": "text",
            "fields": {
                    "toindex": {
                        "fielddata": True,
                        "type": "text"
                    },
                "index": {
                        "type": "keyword"
                    },
                "label": {
                        "type": "keyword"
                    }
            }
        },
        "items_count_description": {
            "type": "long"
        },
        "items_count_hasWorkflowState": {
            "type": "long"
        },
        "places": {
            "copy_to": ["did_you_mean", "searchable_places", "all_fields_for_freetext"],
            "fielddata": True,
            "analyzer": "none",
            "type": "text"
        },
        "items_count_format": {
            "type": "long"
        },
        "items_count_rdf_lastRefreshed": {
            "type": "long"
        },
        "items_count_issued": {
            "type": "long"
        },
        "items_count_spatial": {
            "type": "long"
        },
        "dataProvider": {
            "copy_to": ["organisation", "searchable_organisation", "all_fields_for_freetext"],
            "fielddata": True,
            "type": "text"
        },
        "all_fields_for_freetext_embeddings": {
            "dims": 768,
            "type": "dense_vector"
        }
    }
}
