from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from lib.plone_rest_api import request_with_retry
import requests
from lxml import etree

import json

default_args = {"owner": "airflow"}


def get_default_themes():
    themes = []
    doc = requests.get(
        "https://raw.githubusercontent.com/eea/eea.coremetadata/master/eea/coremetadata/profiles/default/taxonomies/topics.xml"
    )

    tree = etree.fromstring(bytes(doc.text, encoding="utf-8"))

    terms = tree.findall("term", tree.nsmap)

    for term in terms:
        token = term.findall("termIdentifier", tree.nsmap)[0].text
        title = (
            term.findall("caption", tree.nsmap)[0]
            .findall("langstring[@language='en']", tree.nsmap)[0]
            .text
        )
        themes.append({"token": token, "title": title})

    return themes


@task
def updateThemeTaxonomy():
    data = request_with_retry(
        "https://www.eea.europa.eu/api/@vocabularies/collective.taxonomy.themes?b_size=1000"
    )

    data = json.loads(data)
    themes = {}
    for theme in data["items"]:
        themes[theme["token"]] = {"label": theme["title"]}

    default_themes = get_default_themes()
    for theme in default_themes:
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
