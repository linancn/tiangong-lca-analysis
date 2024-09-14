import json
import os

input_dir = "data/processes"
output_dir = "output/processes"


if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for file in os.listdir(input_dir):
    with open(os.path.join(input_dir, file), "r") as f:
        data = json.load(f)
        # print(data)

    output_data = {
        "_index": "process_index",
        "_source": {
            "process_id": data["@id"],
            "process_name": data["name"],
            "process_type": data["processType"],
            "process_category": data["category"],
            "process_location": data["location"]["name"],
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

    with open(os.path.join(output_dir, file), "w") as f:
        json.dump(output_data, f, indent=2)

    # print(output_data)
