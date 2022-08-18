import json
from airflow.models import Variable


def get_variable(variable, variables={}):
    val = variables.get(variable, None)
    if val:
        return val
    else:
        return Variable.get(variable, deserialize_json=True)


def get_all_variables():
    variables = {}
    variables["elastic"] = Variable.get("elastic", deserialize_json=True)
    variables["elastic_settings"] = Variable.get("elastic_settings", deserialize_json=True)
    variables["elastic_mapping"] = Variable.get("elastic_mapping", deserialize_json=True)
    variables["elastic_raw_mapping"] = Variable.get("elastic_raw_mapping", deserialize_json=True)
    variables["rabbitmq"] = Variable.get("rabbitmq", deserialize_json=True)
    variables["nlp_services"] = Variable.get(
        "nlp_services", deserialize_json=True
    )
    variables["Sites"] = Variable.get("Sites", deserialize_json=True)
    for site in variables["Sites"].keys():
        variables[variables["Sites"][site]] = Variable.get(
            variables["Sites"][site], deserialize_json=True
        )
        normalizer = variables[variables["Sites"][site]].get(
            "normalizers_variable", "default_normalizers"
        )
        variables[normalizer] = Variable.get(normalizer, deserialize_json=True)

    return variables

