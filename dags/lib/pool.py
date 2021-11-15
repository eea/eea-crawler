from urllib.parse import urlparse
from airflow.decorators import task

from lib.debug import pretty_id
from airflow.api.common.experimental.pool import (
    create_pool as create_pool_original,
    get_pool,
)
from airflow.exceptions import PoolNotFound
from airflow.models import BaseOperator


@task
def url_to_pool(url: str, prefix: str = "default"):
    return simple_url_to_pool(url, prefix)


def simple_url_to_pool(url: str, prefix: str = "default"):
    return "p-{}-{}".format(prefix, pretty_id(urlparse(url).hostname))[:49]


@task
def val_to_pool(val: str, prefix: str = "default"):
    return simple_val_to_pool(val, prefix)


def simple_val_to_pool(val: str, prefix: str = "default"):
    return "p-{}-{}".format(prefix, val)[:49]


def create_pool(name, slots, description=""):
    try:
        pool = get_pool(name=name)
        if pool:
            print(f"Pool exists: {pool}")
    except PoolNotFound:
        # create the pool
        pool = create_pool_original(
            name=name, slots=slots, description=description
        )
        print(f"Created pool: {pool}")
