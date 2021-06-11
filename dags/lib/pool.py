from urllib.parse import urlparse
from airflow.decorators import task

from lib.debug import pretty_id


@task
def url_to_pool(url: str):
    return "p-{}".format(pretty_id(urlparse(url).hostname))[:49]
