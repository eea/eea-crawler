import json
import os


def load_variables_from_disk(fileName, app_name):
    conf_name = f"app_{app_name}"

    f = open(fileName)
    variables = json.load(f)
    f.close()
    app_conf = variables.get(conf_name)
    variables["Sites"] = app_conf.get("Sites")

    for v_site_conf in variables["Sites"].keys():
        site_conf = variables["Sites"][v_site_conf]
        if variables[site_conf].get("authorization", None):
            variables[site_conf]["authorization"] = os.getenv(
                variables[site_conf]["authorization"]
            )

    elastic_config = app_conf.get("elastic_config")

    variables["elastic"] = variables.get(elastic_config.get("elastic"))
    variables["elastic_mapping"] = variables.get(
        elastic_config.get("elastic_mapping")
    )
    variables["elastic_raw_mapping"] = variables.get(
        elastic_config.get("elastic_raw_mapping")
    )
    variables["elastic_settings"] = variables.get(
        elastic_config.get("elastic_settings")
    )

    variables["rabbitmq"] = variables.get(app_conf.get("rabbitmq_config"))
    variables["nlp_services"] = variables.get(app_conf.get("nlp_config"))
    return variables
