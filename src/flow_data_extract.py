import json
import os

input_dir = "data/flows"
output_dir = "output/flows"


if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for file in os.listdir(input_dir):
    with open(os.path.join(input_dir, file), "r") as f:
        data = json.load(f)
        # print(data)

    output_data = {
        "_index": "flow_index",
        "_source": {
            "flow_id": data["@id"],
            "flow_name": data["name"],
            "flow_type": data["flowType"],
            "flow_category": data["category"],
        },
    }

    with open(os.path.join(output_dir, file), "w") as f:
        json.dump(output_data, f, indent=2)

    # print(output_data)
