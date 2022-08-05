from datetime import datetime
from elasticsearch.exceptions import RequestError
from elasticsearch.client.utils import query_params
import json

BLOCK_WRITE_TRUE = {"settings": {"index.blocks.write": True}}
BLOCK_WRITE_FALSE = {"settings": {"index.blocks.write": False}}
SKIP_IN_PATH = ['', None, "None"]

@query_params(
    "_source",
    "_source_excludes",
    "_source_includes",
    "allow_no_indices",
    "allow_partial_search_results",
    "analyze_wildcard",
    "analyzer",
    "batched_reduce_size",
    "ccs_minimize_roundtrips",
    "default_operator",
    "df",
    "docvalue_fields",
    "expand_wildcards",
    "explain",
    "from_",
    "ignore_throttled",
    "ignore_unavailable",
    "lenient",
    "max_concurrent_shard_requests",
    "pre_filter_shard_size",
    "preference",
    "q",
    "request_cache",
    "rest_total_hits_as_int",
    "routing",
    "scroll",
    "search_type",
    "seq_no_primary_term",
    "size",
    "sort",
    "stats",
    "stored_fields",
    "suggest_field",
    "suggest_mode",
    "suggest_size",
    "suggest_text",
    "terminate_after",
    "timeout",
    "track_scores",
    "track_total_hits",
    "typed_keys",
    "version",
)
def search(es, body=None, index='', doc_type=None, params=[], headers=None, path=None):
        """customized search for elastic endpoints that are published in a path"""
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")
        path = f'{path}/{index}'
        path = "/".join([part for part in path.split("/") if part not in SKIP_IN_PATH])
        full_path = f'/{path}/_search'

        return es.transport.perform_request(
            "POST",
            full_path,
            params=params,
            headers=headers,
            body=body,
        )

def backup_index(es, index, sufix='backup'):
    bu_index = f"{index}_{sufix}"
    bu_alias = f"{index}_backups"
    es.indices.put_settings(json.dumps(BLOCK_WRITE_TRUE), index)
    es.indices.clone(index, bu_index)
    es.indices.put_settings(json.dumps(BLOCK_WRITE_FALSE), index)
    es.indices.put_alias(bu_index, bu_alias)

def backup_indices(es, indices, cnt = 3):
    now = datetime.now()
    ts = now.strftime("%Y_%m_%d_%H_%M_%S")
    for index in indices:
        backup_index(es, index, ts)
    delete_old_indeces_for_index(es, index, cnt)

def create_index(es, index, mapping, settings, add_embedding=False):
    if add_embedding:
        mapping["embedding"] = {
            "type": "dense_vector",
            "dims": 768,
        }
    body = {
        "mappings": {"properties": mapping},
        "settings": settings,
    }

    try:
        es.indices.create(index=index, body=body)
    except RequestError as e:
        if e.error == "resource_already_exists_exception":
            print("Index already exists")
        else:
            raise (e)
    return True

def get_docs(es, index=None, query=None, _source=None, path=None, scroll_size=10000):
    data = search(
        es=es,
        path=path,
        index=index,
        scroll="60m",
        size=scroll_size,
        body=query,
        _source=_source,
    )
    sid = data.get("_scroll_id", None)
    scroll_size = len(data["hits"]["hits"])
    while scroll_size > 0:
        "Scrolling..."
        print("scroll")
        for item in data["hits"]["hits"]:
            yield (item)
        if sid is None:
            scroll_size = 0
        else:
            data = es.scroll(scroll_id=sid, scroll="60m")

            sid = data["_scroll_id"]

            # Get the number of results that returned in the last scroll
            scroll_size = len(data["hits"]["hits"])

def get_doc_by_id(es, item, path=None, index=None, headers=None, params=None, field=None):
    try:
        if field is None:
            res = es.get(index=index, id=item)
        else:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {}
                            }
                        ]
                    }
                }
            }
            query['query']['bool']['must'][0]['match'][field]=item
            res = search(es=es, path=path, body=query)
            res = res['hits']['hits'][0]
    except Exception:
        return None

    doc = res["_source"]
    return doc

def delete_index(es, index):
    es.indices.delete(index=index, ignore=[400, 404])

def get_backups_for_index(es, index):
    bu_alias = f"{index}_backups"
    indices = []
    if es.indices.exists_alias(bu_alias):
        bu_indices = es.indices.get(bu_alias) 
        for idx in bu_indices.keys():
            indices.append({'index': idx, 'creation_date': bu_indices[idx]['settings']['index']['creation_date']})
    indices = sorted(indices, key=lambda d:d['creation_date'], reverse=True)
    return indices

def delete_old_indeces_for_index(es, index, cnt=3):
    bu_indices = get_backups_for_index(es, index)
    for idx in bu_indices[cnt:]:
        delete_index(es, idx['index'])

def delete_doc(es, index, doc_id):
    es.delete(index, id='9a248e53-0747-4bba-b92b-1dea02030dc0')
