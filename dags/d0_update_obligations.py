from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag

from airflow.models import Variable
from rdflib import Graph

import json

default_args = {"owner": "airflow"}


@task
def updateNormObj():
    g = Graph()
    g.parse("https://rod.eionet.europa.eu/obligations/rdf")
    g.parse("https://rod.eionet.europa.eu/instruments/rdf")

    results = g.query(
        """
        SELECT ?obl ?instr ?instr_label ?instr_identifier
        WHERE {
            ?obl a <http://rod.eionet.europa.eu/schema.rdf#Obligation> .
            ?instr a <http://rod.eionet.europa.eu/schema.rdf#Instrument> .
            ?obl <http://rod.eionet.europa.eu/schema.rdf#instrument> ?obl_instr .
            OPTIONAL {?instr <http://www.w3.org/2000/01/rdf-schema#label> ?instr_label} .
            OPTIONAL {?instr <http://purl.org/dc/terms/identifier> ?instr_identifier} .
            FILTER (?instr = ?obl_instr)
        }
        """
    )
    obligations = {}
    for (obl, instr, label, identifier) in results:
        obligations[obl.toPython()] = {
            "label": label.toPython(),
            "instrument": instr.toPython(),
        }
    try:
        Variable.update("obligations", json.dumps(obligations, indent=4))
    except:
        Variable.set(
            "obligations",
            json.dumps(obligations, indent=4),
            description="generated variable, don't modify, it will be overwritten",
        )


@task
def test_variable():
    obl = Variable.get("obligations", deserialize_json=True)
    print(obl)
    print(obl["http://rod.eionet.europa.eu/obligations/104"])
    print(obl.get("http://rod.eionet.europa.eu/obligations/8"))


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    description="obligations",
    tags=["obligations"],
)
def d0_update_obligations():
    t1 = updateNormObj()
    t2 = test_variable()
    t1 >> t2


update_obligations_dag = d0_update_obligations()
