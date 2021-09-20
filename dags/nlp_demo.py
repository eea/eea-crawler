import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from tasks.debug import debug_value

default_args = {
    "owner": "airflow",
}

@task
def dummy_get_docs():
    return [
        "Maritime transport plays and will continue to play an essential role in global and European trade and economy.",
        "The European Environment Agency provides sound, independent information on the environment for those involved in developing, adopting, implementing and evaluating environmental policy, and also the general public.",
        "Climate-friendly practices for sourcing raw materials hold significant potential to cut greenhouse gas emissions in Europe and globally."
    ]

@task
def build_data(docs):
    return json.dumps({
        "snippets":docs
    })
    
@task
def build_qr_data():
    return json.dumps(
        {
            "num_questions": 10,
            "text": "With 77 % of European external trade and 35 % of all trade by value between EU Member States moved by sea, maritime transport is a key part of the international supply chain. Despite a drop in shipping activity in 2020 due to the effects of the COVID-19 pandemic, the sector is expected to grow strongly over the coming decades, fueled by rising demand for primary resources and container shipping.\n\nAgainst this background, the European Maritime Transport Environmental Report, launched today by the European Environment Agency and the European Maritime Safety Agency, marks the first comprehensive health-check of the sector. The report shows that ships produce 13.5 % of all greenhouse gas emissions from transport in the EU, behind emissions from road transport (71 %) and aviation (14.4 %). Sulphur dioxide (SO2) emissions from ships calling in European ports amounted to approximately 1.63 million tonnes in 2019, a figure which is expected to fall further over the coming decades due to stricter environmental rules and measures.\n\nMaritime transport is estimated to have contributed to the fact that underwater noise levels in EU waters have more than doubled between 2014 and 2019 and has been responsible for half of all non-indigenous species introduced into European seas since 1949. However, even though the volume of oil transported by sea has been steadily increasing, only eight accidental medium to large oil tanker spills out of a worldwide total of 62 occurred in EU waters over the past decade.\n\nThe joint report assesses the current state of emerging maritime transport sustainability solutions, including alternative fuels, batteries and onshore power supply, and provides a comprehensive picture of their uptake in the EU. It also outlines future challenges posed by climate change for the industry, including the potential impact of rising sea levels on ports.\n\n“Our Sustainable and Smart Mobility Strategy makes clear that all transport modes need to become more sustainable, smarter and more resilient —  including shipping. Although maritime transport has improved its environmental footprint in past years, it still faces big challenges when it comes to decarbonising and reducing pollution. Based on all the latest evidence, our policies aim to help the sector confront these challenges, by making the most of innovative solutions and digital technologies. This way, maritime transport can keep growing and delivering on our citizens’ daily needs, in harmony with the environment, all the while maintaining its competitiveness and continuing to create quality jobs,” said Adina Vălean, EU Commissioner for Transport.\n\n“This joint report gives us an excellent overview of the present and future challenges related to maritime transport. The message is clear: maritime transport is expected to increase in the coming years and unless we act now, the sector will produce more and more greenhouse gas emissions, air pollutants and underwater noise. A smooth but rapid transition of the sector is crucial to meet the objectives of the European Green Deal and move towards carbon neutrality. This will also create new economic opportunities for the European transport industry as part of the necessary transition to a sustainable blue economy. The challenge is immense, but we have the technologies, the resources and the will to tackle it, said Virginijus Sinkevičius, European Commissioner for Environment, Oceans and Fisheries."
        }
    )

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def nlp_demo():
    """
    ### get info about an url
    """
    xc_docs = dummy_get_docs()
    xc_data = build_data(xc_docs)
    debug_value(xc_data)
    page = SimpleHttpOperator(
        http_conn_id='pure_http',
        task_id="nlp_request_embedding",
        endpoint="nlp-embedding:8000/api/embedding/",
        data=xc_data,
        method="POST",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
    debug_value(page.output)


    xc_qr_data = build_qr_data()
    
    qr_page = SimpleHttpOperator(
        http_conn_id='pure_http',
        task_id="nlp_request_qr",
        endpoint="nlp-question-generation:8000/api/questiongeneration/",
        data=xc_qr_data,
        method="POST",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
    debug_value(qr_page.output)

nlp_demo_dag = nlp_demo()