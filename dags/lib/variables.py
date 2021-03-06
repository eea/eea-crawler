from airflow.models import Variable


def get_variable(variable, variables={}):
    # print("GET VARIABLE")
    # print(variable)
    val = variables.get(variable, None)
    if val:
        # print("exists in cache")
        return val
    else:
        # print("not found in cache")
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
            "normalizers_variable", ""
        )
        variables[normalizer] = Variable.get(normalizer, deserialize_json=True)

    return variables
