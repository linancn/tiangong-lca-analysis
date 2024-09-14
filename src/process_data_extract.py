import json
import os

for file in os.listdir("data/processes"):
    with open("data/processes/" + file, "r") as f:
        data = json.load(f)
        # print(data)

    output_data = {
        "_index": "process_index",
        "_source": {
            "process_id": data["@id"],
            "process_name": data["name"],
            "process_type": data["processType"],
            "process_location": data["location"]["name"],
            "process_amount": None,
            "process_unit": None,
            "input_flows": [],
            "output_flows": [],
        },
    }

    for exchange in data.get("exchanges", []):
        flow_info = {
            "flow_id": exchange["flow"]["@id"],
            "amount": exchange["amount"],
            "unit": exchange["unit"]["name"],
        }

        if exchange["isInput"]:
            output_data["_source"]["input_flows"].append(flow_info)
        else:
            output_data["_source"]["output_flows"].append(flow_info)

        # 设定 process_amount 和 process_unit
        if exchange.get("isQuantitativeReference"):
            output_data["_source"]["process_amount"] = exchange["amount"]
            output_data["_source"]["process_unit"] = exchange["unit"]["name"]

    with open("output/processes/" + file, "w") as f:
        json.dump(output_data, f, indent=2)

    # print(output_data)
