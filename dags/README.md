# Dataflow

## Example workflows

I want to index a bunch of EEA Documents (let's say highlights).

1. Trigger `crawl_with_query`
2. This triggers `fetch_url_row` for all discovered URLs
3. This will create an index in ES, as configured in the logstash pipeline
4. Now trigger the `get_docs_from_es`, which reads all docs created by logstash
   and triggers `prepare_doc_for_search_ui`

## Crawling DAGs

### `crawl_plonerestapi_website`

Given a website URL, it reads sitemap and triggers fetch url on each link in the
sitemap


### `crawl_with_query`

Given a plone.restapi search endpoint, it triggers harvesting for the
discovered URLs


### `crawl_with_sitemap`

Given a website URL, it reads sitemap and triggers fetch url on each link.
Different to `crawl_plonerestapi_website` because it calls `fetch_url_raw`
instead of `fetch_url`


### `index_all_websites`

A DAG that can trigger `crawl_plonerestapi_website` for a list of websites
configured as environment variables. Can be used to trigger manually, as well
as by Airflow cron.


## Harvesting DAGs


### `get_docs_from_es`

Read all docs ids from an ES index, triggers a preprocessing dag for each one


### `get_docs_from_plone`

Trigger `fetch_url` for all Plone URLs from a plone.restapi search


### `prepare_doc_for_search_ui`

Reads a doc from ES and preprocess it, sends it to a Logstash queue to be
indexed


### `fetch_url_raw`

Fetch a URL, store it in the RAW ES index

### `fetch_url`

(Testing) Full pipeline to index a URL to haystack ES index


## Various other DAGs

### `esbootstrap_indexer`

Uses `eea.searchserver` scripts to index content in an ES index
