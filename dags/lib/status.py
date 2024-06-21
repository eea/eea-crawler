from datetime import datetime, timezone
import json

from lib import elastic, rabbitmq

STATUS_QUERY_ALL = """{
  "size": 0,
  "track_total_hits": true
}"""

def strtime_to_ts(strtime):
    dt = datetime.strptime(strtime, '%Y_%m_%d_%H_%M_%S')
    ts = dt.replace(tzinfo=timezone.utc).timestamp() * 1000
    return ts

def add_site_status(v, task_name='', msg='', status=''):
    if v.get("skip_status", None) == True:
        return
    now = datetime.utcnow()
    str_time = now.strftime("%Y_%m_%d_%H_%M_%S")
    rabbitmq_config = v.get('rabbitmq')
    elastic_conf = v.get("elastic")

    raw_status = {'index_name' : f'status_{elastic_conf["searchui_target_index"]}'}
    sites = list(v.get('Sites',{}).keys())

    raw_status["id"] = f'main_task_{str_time}'
    raw_status['cluster'] = 'main_task'
    raw_status['task_name'] = task_name
    raw_status['msg'] = msg
    raw_status['status'] = status
    raw_status['sites'] = sites
    raw_status['start_time'] = str_time
    raw_status['start_time_ts'] = strtime_to_ts(str_time)
    raw_status['next_execution_date'] = v.get('next_execution_date')
    raw_status['next_execution_date_ts'] = strtime_to_ts(v.get('next_execution_date')) + 30000 # add 5 minutes margin for next execution date

    es = elastic.elastic_connection(v)
    resp = elastic.search(es, body=STATUS_QUERY_ALL, index=elastic_conf["searchui_target_index"])
    raw_status['docs_cnt'] = resp.get('hits',{}).get('total',{}).get('value', -1)

    rabbitmq.send_to_rabbitmq(raw_status, rabbitmq_config)

def add_cluster_status(v, cluster, status='', str_time=None, msg=''):
    if v.get("skip_status", None) == True:
        return
    if str_time is None:
        now = datetime.utcnow()
        str_time = now.strftime("%Y_%m_%d_%H_%M_%S")

    rabbitmq_config = v.get('rabbitmq')
    elastic_conf = v.get("elastic")

    raw_status = {'index_name' : f'status_{elastic_conf["searchui_target_index"]}'}

    raw_status["id"] = f'{cluster}_{str_time}'
    raw_status['cluster'] = cluster
    raw_status['start_time'] = str_time
    raw_status['start_time_ts'] = strtime_to_ts(str_time)
    raw_status["status"] = status
    raw_status['msg'] = msg

    rabbitmq.send_to_rabbitmq(raw_status, rabbitmq_config)

    return str_time