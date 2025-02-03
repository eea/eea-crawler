# Configuration

## Airflow configuration variables
It is not mandatory to create all the configurations below, some may have default values
- app_NAME
- config_NAME
- elastic_NAME
- nlp_services_NAME

### App

```go
{
	"Sites": {
		"MYNAME": "config_MYNAME"
	},
	"allowed_errors_for_doc": 2,
	"elastic_config": {
		"elastic": "elastic_MYNAME",
		"elastic_mapping": "elastic_mapping_default",
		"elastic_raw_mapping": "elastic_raw_mapping_default",
		"elastic_settings": "elastic_settings_default"
	},
	"nlp_config": "nlp_services_local",
	"rabbitmq_config": "rabbitmq_global_search",
	"skip_doc_cnt": 100
}
```

#### Sites

List of websites will be indexed ex:
```go
{
	"KEY1": "config_KEY1",
	"KEY2": "config_KEY2",
	"KEY3": "config_KEY3",
	……
	"KEYn": "config_KEYn",
}
```

#### Allowed_errors_for_doc
How many times will be tried to read a document in case it gives an error

#### Skip_doc_cnt
Once reach “allowed_errors_for_doc” will skip indexing this document for the nr of times specified in this variable

#### elastic_config

- `elastic` name of elastic configuration
- `elastic_raw_mapping` raw_value : a json variable with all the information about the document, headless chrome ...
- `elastic_mapping` normalization data from elastic_raw_mapping
- `elastic_settings` different settings for elastic search, for example stop words

#### rabitmq_config
Settings for configuring rabbitmq: host, port, username, password, port, queue name

#### nlp_config

### Config
```go
{
    "avoid_cache_api": true,
    "avoid_cache_web": true,
    "ignore_robots_txt": true,
    "concurrency": 4,
    "exclude": [],    
    "normalize": {
        "blacklist": [],
        "whitelist": [
            "DOCUMENTTYPE1",
            "DOCUMENTTYPE2"
        ]
    },
    "normalizers_variable": "default_normalizers",
    "portal_types": [
        "DOCUMENTTYPE1",
        "DOCUMENTTYPE2"
    ],
    "scrape_pages": false,
    "scrape_with_js": true,
    "type": "plone_rest_api",
    "url": "https://my.domain.name",
    "url_api_part": "api",
    "fix_items_url": {
        "without_api":'',
        "with_api":''
    }
}
```
#### Normalize
In blacklist and whitelist : types of documents
##### normalizers_variable
Normalization of variables
Example :
- `normObj`
    - "Term": "Glossary 
    - "Turkey": "T\u00fcrkiye"
    - "Products.EEAContentTypes.content.interfaces.IArticle": "Article"
- `blackMap` values that will be excluded for the fields
- `whiteMap` values that will be excluded for the field
- `normMissing` set the value if the field it is not defined
    - "creator": "European Environment Agency (EEA)"
       if creator field does not exist will be set with European Environment Agency (EEA)
    - "creation_date": "field:created",
        if creation_date does not ecist value will be set the value from field 'created' in this case

#### Portal_types
If this variable has values, only the specified types will be imported from all pages on the site
##### type
- `plone_rest_api` All documents will be read except:
    - the URL whitelist is defined and not included
    - the URL blacklist is defined and included
    - URL exists in robots.txt
    - portal_types is defined and the document type is not included
    - if it is a file, check if the extension appears in SKIP_EXTENSTIONS
    - if it appears in the list of types of documents to be omitted
    - if it is in the list of documents that must be omitted (for example, there was an error in the past)
- `sdi` All documents whose modification date is different from the last one will be read, except for the case when "fetch_all_docs" is set
- `singlepage`
- `sitemap` All documents will be read except:
    - the URL is in exlude_list
    - the URL whitelist is defined and not included 
    - the URL blacklist is defined and included
    - URL exists in robots.txt
    - if it is in the list of documents that must be omitted (for example, there was an error in the past)
    - portal_types has no effect

##### fix_items_url
- `without_api` ?!?!
- `with_api` ?!?!

#### url_api_part
Appends a value to the defined URL

##### scrape_pages
If it is true, it will also read the page

##### scrape_with_js
Wait for JS to finish

### Elastic

```go
{
    "host": "elastic",
    "port": 9200,
    "raw_index": "[NAME]_raw",
    "searchui_target_index": "[NAME]_searchui"
}
```

### NLP
```go
{
  "converter": {
    "host": "nlp-searchlib",
    "path": "api/converter",
    "port": "8000"
  },
  "embedding": {
    "dest_field_name": "nlp_250",
    "host": "nlp-searchlib",
    "path": "api/embedding",
    "port": "8000"
  },
  "split": {
    "clean_empty_lines": true,
    "clean_header_footer": false,
    "clean_whitespace": true,
    "dest_field_name": "nlp_250",
    "fulltext_field": "fulltext",
    "host": "nlp-searchlib",
    "path": "api/split",
    "port": "8000",
    "split_by": "word",
    "split_length": 250,
    "split_overlap": 50,
    "split_respect_sentence_boundary": false
  }
}
```

`split`
- `clean_empty_lines` name of elastic configuration
- `dest_field_name` name of elastic configuration
- `fulltext_field`

# Dags

## D0

```python
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from tasks.helpers import get_app_identifier

START_DATE = days_ago(1)
SCHEDULE_INTERVAL = "@daily"
default_args = {"owner": "airflow"}

TASK_PARAMS = {"params": {"app": "app_NAME", "enable_prepare_docs": True}}


@task
def trigger_sync(ignore_delete_threshold):
    app_id = get_app_identifier("app_climatevideo_id")
    TASK_PARAMS["params"]["ignore_delete_threshold"] = ignore_delete_threshold
    TASK_PARAMS["params"]["app_identifier"] = app_id
    TASK_PARAMS["params"]["skip_status"] = True
    trigger_dag("d1_sync", TASK_PARAMS, "default_pool")


@dag(
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    description="scheduled global search sync",
    tags=["crawl"],
)

def d0_sync_DAG_NAME(ignore_delete_threshold=False):
    trigger_sync(ignore_delete_threshold)

sync_cca_DAG_NAME = d0_sync_DAG_NAME()
```

The DAG can be started manually, or using another DAG but to which we specify the data with which it will run (Trigger DAG w/ config).
After starting, the following will start in order:
- `d1_sync`: for each specified website it will create the configurations and run the next DAG with the website data
- `d2_crawl_site`: depending on the configuration, it will bring all the documents from the website
- `d3_crawl_fetch_for_id`: will fetch all the documents according to the type of crawler dags/crawlers/crawlers/TYPE.py -> parse_all_documents. For each document, dags/crawlers/crawlers/TYPE.py -> crawl_doc will be called.