import json
import os

for file in os.listdir("data/flows"):
    with open("data/flows/" + file, "r") as f:
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

    with open("output/flows/" + file, "w") as f:
        json.dump(output_data, f, indent=2)

    # print(output_data)
