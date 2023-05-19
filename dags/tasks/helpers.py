import collections
from copy import deepcopy
from airflow.decorators import task
from urllib.parse import urlparse
from lib.airflow_variables import get_variable, get_all_variables

from airflow import settings
from airflow.models import DagRun

def merge(dict1, dict2):
    """Return a new dictionary by merging two dictionaries recursively."""

    result = deepcopy(dict1)

    for key, value in dict2.items():
        if isinstance(value, collections.Mapping):
            result[key] = merge(result.get(key, {}), value)
        else:
            result[key] = deepcopy(dict2[key])

    return result


def rebuild(tree):
    clean_tree = None
    if isinstance(tree, dict):
        clean_tree = {}
        if tree.get("__type", "non_dict") == "dict":
            for key in tree.get("__var").keys():
                clean_tree[key] = rebuild(tree.get("__var")[key])
        else:
            for key in tree.keys():
                clean_tree[key] = rebuild(tree[key])
    elif isinstance(tree, list):
        clean_tree = []
        for item in tree:
            clean_tree.append(rebuild(item))
    else:
        clean_tree = tree
    return clean_tree


@task
def dag_param_to_dict(task_params, defaults={}):
    return simple_dag_param_to_dict(task_params, defaults)


def simple_dag_param_to_dict(params, defaults={}):
    """
    dag params with a dict format have the format:
    {
        "item": {
            "__var": {
                "site": "http://eea.europa.eu",
                "rabbitmq": {
                    "__var": {
                        "host": "rabbitmq",
                        "port": "5672",
                        "username": "guest",
                        "password": "guest",
                        "queue": "default"
                    },
                    "__type": "dict"
                }
            },
            "__type": "dict"
        }
    }
    we want to clean it into the form:
    {
        "item": {
            "site": "http://eea.europa.eu",
            "rabbitmq": {
                "host": "rabbitmq",
                "port": "5672",
                "username": "guest",
                "password": "guest",
                "queue": "default"
            }
        }
    }
    """
    clean_params = rebuild(params)
    final_params = merge(defaults, clean_params)
    return final_params


@task
def build_items_list(items, task_params):
    return [{"item": item, "params": task_params} for item in items]


@task
def get_params(task_params):
    return task_params["params"]


@task
def get_item(task_params):
    return task_params["item"]


@task
def get_attr(params, attr):
    return params.get(attr, None)


@task
def set_attr(task_params, attr, val):
    task_params[attr] = val
    return task_params


@task
def get_es_variable(variable):
    return get_variable(variable)


def get_site_map(variables={}):
    sites = get_variable("Sites", variables)
    site_map = {}
    for site in sites:
        site_config = get_variable(sites[site], variables)
        site_map[site] = site_config["url"]

    return site_map


def find_site_by_url(url, sites=None, variables={}):
    if not sites:
        sites = get_site_map(variables=variables)
    parts = url.split("://")[-1].strip("/").split("/")

    names = ["/".join(parts[: (i * -1)]) for i in range(1, len(parts))]

    site_name = ""
    for name in names:
        for site in sites.keys():
            if site_name == "":
                site_url = sites[site].split("://")[-1].strip("/")
                if name == site_url:
                    site_name = site
    return site_name


@task
def load_variables(task_params):
    print(task_params)
    app_name = task_params["app"]
    conf_name = f"app_{app_name}"
    variables = get_all_variables(conf_name)
    task_params["variables"] = variables
    return task_params

STATES = ["queued", 'running']
DAGS = ["d1_sync", "d2_crawl_site", "d3_crawl_fetch_for_id", "d5_prepare_doc_for_searchui"]

def get_app_identifier(appname):
    session = settings.Session()

    app_id =  f"|_{appname}_|"
    total_cnt = 0
    print(dir(session))
    for dag in DAGS:
        for state in STATES:
            query_as_string = f"""
                SELECT count(dag_run.id)
                FROM dag_run
                WHERE dag_run.dag_id = '{dag}' AND dag_run.state = '{state}' AND dag_run.conf LIKE '%{app_id}%'
                """
            #print(query_as_string)
            res = session.execute(query_as_string)
            cnt = 0
            for row in res:
                cnt = row[0]
            print(f"Dag: {dag}, State: {state}, Cnt: {cnt}")
            total_cnt += cnt
    session.close()
    if total_cnt > 0:
        msg = f"WARNING: sync for {appname} already in progress"
        raise Exception(msg)
    return app_id
