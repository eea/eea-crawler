import collections
from copy import deepcopy
from airflow.decorators import task
from airflow.models import Variable
from urllib.parse import urlparse


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
def dag_param_to_dict(params, defaults={}):
    return simple_dag_param_to_dict(params, defaults)


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
def build_items_list(items, params):
    return [{"item": item, "params": params} for item in items]


@task
def get_params(params):
    return params["params"]


@task
def get_item(params):
    return params["item"]


@task
def get_attr(params, attr):
    return params.get(attr, None)


@task
def set_attr(params, attr, val):
    params[attr] = val
    return params


@task
def get_variable(variable):
    return Variable.get(variable, deserialize_json=True)


def find_site_by_url(url):
    parts = url.split("://")[-1].strip("/").split("/")

    names = ["/".join(parts[: (i * -1)]) for i in range(1, len(parts))]

    sites = Variable.get("Sites", deserialize_json=True)

    site_name = ""
    for name in names:
        for site in sites.keys():
            if site_name == "":
                site_config = Variable.get(sites[site], deserialize_json=True)
                site_url = site_config["url"].split("://")[-1].strip("/")
                if name == site_url:
                    site_name = site

    return site_name
