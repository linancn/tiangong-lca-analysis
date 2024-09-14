import json
import os

import pandas as pd
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
            "flow_id": {"type": "keyword"},
            "flow_amount": {"type": "double"},
            "flow_unit": {"type": "keyword"},
            "flow_name": {"type": "text"},
            "flow_type": {"type": "keyword"},
            "flow_category": {"type": "keyword"},
        }
    }
}


if not es.indices.exists(index="process_index"):
    es.indices.create(index="process_index", body=process_mapping)

process_data = []

process_df = pd.read_pickle("output/merged_data.pkl")

for record in process_df.to_dict(orient="records"):
    output_data = {
        "_index": "process_index",
        "_source": {
            "process_id": record["process_id"],
            "process_name": record["process_name"],
            "process_type": record["process_type"],
            "process_category": record["process_category"],
            "process_location": record["process_location"],
            "flow_id": record["flow_id"],
            "flow_amount": record["amount"],
            "flow_unit": record["unit"],
            "flow_name": record["flow_name"],
            "flow_type": record["flow_type"],
            "flow_category": record["flow_category"],
        },
    }
    process_data.append(output_data)

try:
    helpers.bulk(es, process_data)
    print("Process数据批量插入成功")
except Exception as e:
    print(f"Process数据插入过程中出现错误: {e}")
