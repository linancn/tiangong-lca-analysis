import os
import json

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

es = Elasticsearch(
    hosts=["http://39.105.216.221:9200/"],
    basic_auth=(os.getenv("USERNAME"), os.getenv("PASSWORD")),
)

process_mapping = {
    "mappings": {
        "properties": {
            "process_id": {"type": "keyword"},
            "process_name": {"type": "text"},
            "process_type": {"type": "keyword"},
            "process_category": {"type": "keyword"},
            "process_location": {"type": "keyword"},
            "input_flows": {
                "type": "nested",
                "properties": {
                    "flow_id": {"type": "keyword"},
                    "amount": {"type": "double"},
                    "unit": {"type": "keyword"},
                },
            },
            "output_flows": {
                "type": "nested",
                "properties": {
                    "flow_id": {"type": "keyword"},
                    "amount": {"type": "double"},
                    "unit": {"type": "keyword"},
                },
            },
        }
    }
}

if not es.indices.exists(index="process_index"):
    es.indices.create(index="process_index", body=process_mapping)


flow_mapping = {
    "mappings": {
        "properties": {
            "flow_id": {"type": "keyword"},
            "flow_name": {"type": "text"},
            "flow_type": {"type": "keyword"},
            "flow_category": {"type": "keyword"},
        }
    }
}

if not es.indices.exists(index="flow_index"):
    es.indices.create(index="flow_index", body=flow_mapping)

# load json fils to a list
process_data = []
for file in os.listdir("output/processes"):
    with open(f"output/processes/{file}", "r") as f:
        process_data.append(json.load(f))

try:
    helpers.bulk(es, process_data)
    print("Process数据批量插入成功")
except Exception as e:
    print(f"Process数据插入过程中出现错误: {e}")


flow_data = []
for file in os.listdir("output/flows"):
    with open(f"output/flows/{file}", "r") as f:
        flow_data.append(json.load(f))

try:
    helpers.bulk(es, flow_data)
    print("Flow数据批量插入成功")
except Exception as e:
    print(f"Flow数据插入过程中出现错误: {e}")
