import json
from airflow.models import Variable
import os


def get_variable(variable, variables={}):
    val = variables.get(variable, None)
    if val:
        return val
    else:
        return Variable.get(variable, deserialize_json=True)


def get_all_variables(conf_name):
    variables = {}

    app_conf = Variable.get(conf_name, deserialize_json=True)
    variables["Sites"] = app_conf.get("Sites")
    variables["allowed_errors_for_doc"] = app_conf.get(
        "allowed_errors_for_doc", 3
    )
    variables["skip_doc_cnt"] = app_conf.get("skip_doc_cnt", 10)
    elastic_config = app_conf.get("elastic_config")
    variables["elastic"] = Variable.get(
        elastic_config.get("elastic"), deserialize_json=True
    )
    variables["elastic_mapping"] = Variable.get(
        elastic_config.get("elastic_mapping"), deserialize_json=True
    )
    variables["elastic_raw_mapping"] = Variable.get(
        elastic_config.get("elastic_raw_mapping"), deserialize_json=True
    )
    variables["elastic_settings"] = Variable.get(
        elastic_config.get("elastic_settings"), deserialize_json=True
    )

    variables["rabbitmq"] = Variable.get(
        app_conf.get("rabbitmq_config"), deserialize_json=True
    )
    variables["nlp_services"] = Variable.get(
        app_conf.get("nlp_config"), deserialize_json=True
    )

    variables["headless_chrome"] = Variable.get(
        "headless_chrome", deserialize_json=True
    )
    try:
        variables["obligations"] = Variable.get(
            "obligations", deserialize_json=True
        )
    except:
        variables["obligations"] = {}

    try:
        variables["theme_taxonomy"] = Variable.get(
            "theme_taxonomy", deserialize_json=True
        )
    except:
        variables["theme_taxonomy"] = {}

    for site in variables["Sites"].keys():
        variables[variables["Sites"][site]] = Variable.get(
            variables["Sites"][site], deserialize_json=True
        )
        normalizer = variables[variables["Sites"][site]].get(
            "normalizers_variable", "default_normalizers"
        )
        variables[normalizer] = Variable.get(normalizer, deserialize_json=True)

    for v_site_conf in variables["Sites"].keys():
        site_conf = variables["Sites"][v_site_conf]
        if variables[site_conf].get("authorization", None):
            variables[site_conf]["authorization"] = os.getenv(
                variables[site_conf]["authorization"]
            )

    return variables
