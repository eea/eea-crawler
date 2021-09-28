from xml.dom import minidom

from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

from tasks.dagrun import BulkTriggerDagRunOperator

from usp.tree import sitemap_tree_for_homepage
from tasks.pool import CreatePoolOperator
from lib.pool import url_to_pool
from tasks.debug import debug_value

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

""" @task()
def get_sitemap_url(website_url: str):
    sitemap_url = website_url.split("://")[-1] + "/sitemap.xml.gz"
    print("sitemap_url", sitemap_url)
    return sitemap_url


@task()
def get_urls_from_sitemap(sitemap: str):
    response = []
    dom = minidom.parseString(sitemap)
    urls = dom.getElementsByTagName("url")
    for url in urls:
        get = url.getElementsByTagName
        item = {
            "url": get("loc")[0].firstChild.nodeValue,
            "date": get("lastmod")[0].firstChild.nodeValue,
        }
        response.append(item)
    print(response)
    return response """


@task
def get_urls_to_update(urls: list = []) -> dict:
    my_clean_urls = []
    for url in urls:
        my_clean_urls.append(url["url"])
    print(my_clean_urls)
    return my_clean_urls


@task
def get_sitemap(url):
    tree = sitemap_tree_for_homepage(url)
    urls = []
    for page in tree.all_pages():
        urls.append(
            {
                "url": page.url
                # , 'last_modified':page.last_modified.strftime("%m/%d/%Y, %H:%M:%S")
            }
        )
    print("Retrieved %s urls" % len(urls))
    return urls


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def crawl_plonerestapi_website(
    item: str = "",
    maintainer_email: str = "",
):
    """
    ### Crawls a plone.restapi powered website.

    Main task to crawl a website
    """
    pool_name = url_to_pool(item)
    xc_urls = get_sitemap(item)
    xc_clean_urls = get_urls_to_update(xc_urls)

    cpo = CreatePoolOperator(
        task_id="create_pool",
        name=pool_name,
        slots=2,
    )

    bt = BulkTriggerDagRunOperator(
        task_id="fetch_urls",
        items=xc_clean_urls,
        trigger_dag_id="clms_fetch_url",
        custom_pool=pool_name,
    )


#    bulk_run(pool_name, xc_clean_urls)
#    debug_value(pool_name)
#    debug_value(xc_clean_urls)

"""     bt = BulkTriggerDagRunOperator(
        task_id="fetch_urls",
        items=xc_clean_urls,
        trigger_dag_id="fetch_url",
        custom_pool=pool_name,
    )
 """  #    [xc_clean_urls, cpo] >> bt


#    xc_clean_urls >> cpo


#   helpers.show_dag_run_conf(
#        {"website_url": website_url, "maintainer_email": maintainer_email}
#    )


#    xc_sitemap_url = get_sitemap_url(website_url)

#    xc_sitemap = SimpleHttpOperator(
#        task_id="get_sitemap",
#        method="GET",
#        endpoint=xc_sitemap_url,
# pool="{{ti.xcom_pull('allocated_api_pool')}}"
#    )

#    helpers.debug_value(xc_sitemap.output)

#    xc_urls = get_urls_from_sitemap(xc_sitemap.output)

#    xc_clean_urls = get_urls_to_update(xc_urls)

#    BulkTriggerDagRunOperator(
#        task_id="fetch_urls",
#        items=xc_clean_urls,
#        trigger_dag_id="fetch_url",
#        parent=website_url,
#    )


crawl_website_dag = crawl_plonerestapi_website()
