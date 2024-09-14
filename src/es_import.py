import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

es = Elasticsearch(
    hosts=["http://39.105.216.221:9200/"],
    http_auth=(os.getenv("USERNAME"), os.getenv("PASSWORD")),
)


data = [
    {
        "_index": "process_index",
        "_source": {
            "process_id": "/0afdbffb-fbe3-3788-9196-812f8b41cbf2",
            "process_name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014 | electricity, high voltage | Cutoff, U",
            "process_location": "Russia",
            "process_amount": 1,
            "process_unit": "kWh",
            "input_flows": [],
            "output_flows": ["c9641dad-17a0-4d81-a9ee-85890945c39d"],
        },
    },
    {
        "_index": "process_index",
        "_source": {
            "process_id": "0afdbffb-fbe3-3788-9196-812f8b41cbcc",
            "process_name": "heat and power co-generation, wood chips, 110 kW, state-of-the-art 2013 | electricity, high voltage | Cutoff, U",
            "process_location": "Russia",
            "process_amount": 1,
            "process_unit": "kWh",
            "input_flows": ["c9641dad-17a0-4d81-a9ee-85890945c39d"],
            "output_flows": [
                "a6b32de2-648a-4cd7-a256-9b122f68dd8b",
                "c9641dad-17a0-4d81-a9ee-85890945c39d",
            ],
        },
    },
    {
        "_index": "flow_index",
        "_source": {
            "flow_id": "c9641dad-17a0-4d81-a9ee-85890945c39d",
            "flow_name": "NOx retained, by selective catalytic reduction",
            "flow_type": "PRODUCT_FLOW",
            "flow_direction": "input",
            "flow_amount": 1.23e-03,
            "flow_unit": "kg",
        },
    },
    {
        "_index": "flow_index",
        "_source": {
            "flow_id": "a6b32de2-648a-4cd7-a256-9b122f68dd8b",
            "flow_name": "chlorine, liquid",
            "flow_type": "PRODUCT_FLOW",
            "flow_direction": "input",
            "flow_amount": 4.35e-06,
            "flow_unit": "kg",
        },
    },
    {
        "_index": "flow_index",
        "_source": {
            "flow_id": "c9641dad-17a0-4d81-a9ee-85890945c39d",
            "flow_name": "NOx retained, by selective catalytic reduction",
            "flow_type": "PRODUCT_FLOW",
            "flow_direction": "input",
            "flow_amount": 1.27e-03,
            "flow_unit": "kg",
        },
    },
]

try:
    helpers.bulk(es, data)
    print("数据批量插入成功")
except Exception as e:
    print(f"数据插入过程中出现错误: {e}")
