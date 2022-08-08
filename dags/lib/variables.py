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

def load_variables_from_disk(fileName):
    f = open(fileName)
    variables = json.load(f)
    f.close()
    return variables
