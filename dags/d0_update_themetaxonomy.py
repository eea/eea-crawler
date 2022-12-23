from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from lib.plone_rest_api import request_with_retry


import json

default_args = {"owner": "airflow"}


@task
def updateThemeTaxonomy():
    data = request_with_retry(
        "https://www.eea.europa.eu/api/@vocabularies/collective.taxonomy.themes?b_size=1000"
    )

    data = json.loads(data)
    themes = {}
    for theme in data["items"]:
        themes[theme["token"]] = {"label": theme["title"]}

    try:
        Variable.update("theme_taxonomy", json.dumps(themes, indent=4))
    except:
        Variable.set(
            "theme_taxonomy",
            json.dumps(themes, indent=4),
            description="generated variable, don't modify, it will be overwritten",
        )


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    description="theme taxonomy",
    tags=["theme taxonomy"],
)
def d0_update_themetaxonomy():
    updateThemeTaxonomy()


update_themetaxonomy_dag = d0_update_themetaxonomy()
