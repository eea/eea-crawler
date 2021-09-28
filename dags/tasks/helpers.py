from airflow.decorators import task
import collections
from copy import deepcopy


def merge(dict1, dict2):
    """ Return a new dictionary by merging two dictionaries recursively. """

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
