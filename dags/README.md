

# Dataflow

## Example workflows

I want to index a bunch of EEA Documents (let's say highlights).

1. Trigger `crawl_with_query`
2. This triggers `fetch_url_raw` for all discovered URLs
3. This will create an index in ES, as configured in the logstash pipeline,
   with queue `queue_raw`
4. Now trigger the `prepare_docs_for_search_ui_from_es`, which reads all docs created by logstash
   and triggers `prepare_doc_for_search_ui` which uses the logstash queue
   `queue_searchui`
5. Next, trigger the `prepare_docs_for_nlp_from_es`, which reads all docs created by logstash
   and triggers `prepare_doc_for_nlp` which uses the logstash queue
   `queue_nlp`

Check the `logstash/pipeline` folder to understand where the queue indexing
ends up

If the defaults are used, we will have 3 indices in elasticsearch:
- **data_raw** - with the raw documents from plone, without any processing
- **data_searchui** - the documents were normalized (property names, default values, etc.)
- **data_nlp** - contains all the data from `data_searchui` but with extra preprocessing: a merged `text` field was created, the documents are split in multiple smaller documents, `embeddings` were added to each document

## Crawling DAGs

### `crawl_plonerestapi_website` (currently not used)

Given a website URL, it reads sitemap and triggers fetch url on each link in the
sitemap


### `crawl_with_query`

Given a plone.restapi search endpoint, it triggers harvesting for the
discovered URLs. For each URL, a new `fetch_url_raw` dag will be created.
#### `Parameters: `
    {
    	'item': "http://www.eea.europa.eu/api/@search?portal_type=Highlight&sort_order=reverse&sort_on=Date&created.query=2019/6/1&created.range=min&b_size=500",
    	'params': {
    		'rabbitmq': {
    			"host": "rabbitmq",
    			"port": "5672",
    			"username": "guest",
    			"password": "guest",
    			"queue": "queue_raw_data"
    		},
    		'url_api_part': 'api/SITE'
    	}
    }
- **item** - the query for plone.restapi
- **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
- **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents


### `crawl_with_sitemap`

Given a website URL, it reads sitemap and triggers fetch url on each link.
Different to `crawl_plonerestapi_website` because it calls `fetch_url_raw`
instead of `fetch_url`
#### `Parameters:`
    {
		'item': 'http://eea.europa.eu',
		'params': {
			'rabbitmq': {
				"host": "rabbitmq",
				"port": "5672",
				"username": "guest",
				"password": "guest",
				"queue": "default"
			},
			'url_api_part': 'api/SITE'
		}
	}

- **item** - the url of the website
- **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
- **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents

### `index_all_websites` (currently not used)

A DAG that can trigger `crawl_plonerestapi_website` for a list of websites
configured as environment variables. Can be used to trigger manually, as well
as by Airflow cron.


## Harvesting DAGs


### `prepare_docs_for_search_ui_from_es`

Read all docs ids from an ES index, triggers a preprocessing dag for each one: `prepare_doc_for_search_ui`
#### `Parameters:`
	{
		'item': "http://eea.europa.eu",
		'params': {
			'elastic': {
				'host': 'elastic',
				'port': 9200,
				'index': 'data_raw',
				'mapping': {...},
				'settings': {...},
				'target_index': 'data_searchui'
			},
			'rabbitmq': {
				"host": "rabbitmq",
				"port": "5672",
				"username": "guest",
				"password": "guest",
				"queue": "queue_searchui"
			},
			'url_api_part': 'api/SITE'
		}
	}
- **item** - the url of the website
- **params.elastic** - the elasticsearch configuration
- **params.elastic.index** - the index from where the raw data is read
- **params.elastic.target_index** - the index from where the preprocessed data will be written. Even if the documents are sent to rabbitmq, we have to create and prepare (set the filters, analyzers, and field mapping) the elastic index before we start storing data in it.
- **params.elastic.settings** - contains all filters and analyzers, by default we use [dags/normalizers/elastic_settings.py](https://github.com/eea/eea-crawler/blob/master/dags/normalizers/elastic_settings.py)
- **params.elastic.mapping** - contains the fields mapping, by default we use [dags/normalizers/elastic_mapping.py](https://github.com/eea/eea-crawler/blob/master/dags/normalizers/elastic_mapping.py)
- **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
- **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents

### `get_docs_from_plone` (currently not used)

Trigger `fetch_url` for all Plone URLs from a plone.restapi search


### `prepare_doc_for_search_ui`

Reads a doc from ES and preprocess it, sends it to a Logstash queue to be
indexed
#### `Parameters:`
	{
		'item': "https://www.eea.europa.eu/api/SITE/highlights/better-raw-material-sourcing-can",
		'params': {
			'elastic': {
				'host': 'elastic',
				'port': 9200,
				'index': 'data_raw'
			},
			'rabbitmq': {
				"host": "rabbitmq",
				"port": "5672",
				"username": "guest",
				"password": "guest",
				"queue": "queue_searchui"
			},
			'normalizers': {..},
			'url_api_part': 'api/SITE'
		}
	}
- **item** - the url of the document
- **params.elastic** - the elasticsearch configuration
- **params.elastic.index** - the index from where the raw data is read
- **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
- **params.normalizers** - the configuration of normalizers: list of properties, properties normalization, white and blackmap for values, object normalization, set missing values. By default we use [dags/normalizers/edfaults.py](https://github.com/eea/eea-crawler/blob/master/dags/normalizers/defaults.py)
- **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents

#### Note:
This dag can be used individually, only if the index was already created and it's mapping was properly set.

### `fetch_url_raw`

Fetch a single URL, store it in the RAW ES index. This dag is triggered either by `crawl_with_query` or `crawl_with_sitemap`.
#### `Parameters:`
	{
		'item': "https://www.eea.europa.eu/highlights/better-raw-material-sourcing-can",
		'params': {
			'rabbitmq': {
				"host": "rabbitmq",
				"port": "5672",
				"username": "guest",
				"password": "guest",
				"queue": "default"
			},
			'url_api_part': 'api/SITE'
		}
	}

 - **item** - the url of the document
 - **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
 - **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents

## NLP DAGs


### `prepare_docs_for_nlp_from_es`

Read all docs ids from an ES index, triggers a preprocessing dag for each one: `prepare_doc_for_nlp`
#### `Parameters:`
	{
		'item': "http://eea.europa.eu",
		'params': {
			'elastic': {
				'host': 'elastic',
				'port': 9200,
				'index': 'data_raw',
				'mapping': {...},
				'settings': {...},
				'target_index': 'data_nlp'
			},
			'rabbitmq': {
				"host": "rabbitmq",
				"port": "5672",
				"username": "guest",
				"password": "guest",
				"queue": "queue_nlp"
			},
			'nlp': {
				'services': {
					'embedding': {
						'host': 'nlp-embedding',
						'port': '8000',
						'path': 'api/embedding'
					}

				},
				'text': {
					'props': ['description', 'key_message', 'summary', 'text'],
					'blacklist': ['contact', 'rights'],
					'split_length': 500
				}
			},
			'url_api_part': 'api/SITE'
		}
	}
- **item** - the url of the website
- **params.elastic** - the elasticsearch configuration
- **params.elastic.index** - the index from where the raw data is read
- **params.elastic.target_index** - the index from where the preprocessed data will be written. Even if the documents are sent to rabbitmq, we have to create and prepare (set the filters, analyzers, and field mapping) the elastic index before we start storing data in it.
- **params.elastic.settings** - contains all filters and analyzers, by default we use [dags/normalizers/elastic_settings.py](https://github.com/eea/eea-crawler/blob/master/dags/normalizers/elastic_settings.py)
- **params.elastic.mapping** - contains the fields mapping, by default we use [dags/normalizers/elastic_mapping.py](https://github.com/eea/eea-crawler/blob/master/dags/normalizers/elastic_mapping.py)
- **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
- **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents
- **params.nlp.services** - the configuration for nlp service, currently only `embedding` is used
- **params.nlp.text** - configuration for text transformations
- **params.nlp.text.props** - list of properties that will be forced to be in front of the merged text
- **params.nlp.text.blacklist** - list of properties to be ignored when merging the text
- **params.nlp.text.split_length** - parameter for haystack.preprocessor, the length how the merged text to be split

### `prepare_doc_for_nlp`

Reads a doc from ES and applies the same preporcessing as `prepare_doc_for_search_ui`, extracts the text fields and merges them, splits the documents to smaller ones, generates the embedding, and finally pushes all documents to the Logstash queue.
 sends it to a Logstash queue to be
indexed
#### `Parameters:`
	{
		'item': "https://www.eea.europa.eu/api/SITE/highlights/better-raw-material-sourcing-can",
		'params': {
			'elastic': {
				'host': 'elastic',
				'port': 9200,
				'index': 'data_raw'
			},
			'rabbitmq': {
				"host": "rabbitmq",
				"port": "5672",
				"username": "guest",
				"password": "guest",
				"queue": "queue_nlp"
			},
			'normalizers': {..},
			'nlp': {
				'services': {
					'embedding': {
						'host': 'nlp-embedding',
						'port': '8000',
						'path': 'api/embedding'
					}

				},
				'text': {
					'props': ['description', 'key_message', 'summary', 'text'],
					'blacklist': ['contact', 'rights'],
					'split_length': 500
				}
			},
			'url_api_part': 'api/SITE'
		}
	}
- **item** - the url of the document
- **params.elastic** - the elasticsearch configuration
- **params.elastic.index** - the index from where the raw data is read
- **params.rabbitmq** - the rabbitmq configuration, including the name of the **queue**, where the documents will be sent
- **params.normalizers** - the configuration of normalizers: list of properties, properties normalization, white and blackmap for values, object normalization, set missing values. By default we use [dags/normalizers/edfaults.py](https://github.com/eea/eea-crawler/blob/master/dags/normalizers/defaults.py)
- **params.url_api_part** - the url part, that says where the api is located; this will be injected in all urls of the documents
- **params.nlp.services** - the configuration for nlp service, currently only `embedding` is used
- **params.nlp.text** - configuration for text transformations
- **params.nlp.text.props** - list of properties that will be forced to be in front of the merged text
- **params.nlp.text.blacklist** - list of properties to be ignored when merging the text
- **params.nlp.text.split_length** - parameter for haystack.preprocessor, the length how the merged text to be split

#### Note:
This dag can be used individually, only if the index was already created and it's mapping was properly set.

### `fetch_url` (currently not used)

(Testing) Full pipeline to index a URL to haystack ES index


## Various other DAGs

### `esbootstrap_indexer` (for indexing with legacy nodejs esbootstrap indexer)

Uses `eea.searchserver` scripts to index content in an ES index
