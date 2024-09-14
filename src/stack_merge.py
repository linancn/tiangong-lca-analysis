import json
import os

import pandas as pd

processes_dir = "output/processes"
flows_dir = "output/flows"

flow_data = []
for file in os.listdir(flows_dir):
    with open(os.path.join(flows_dir, file), "r") as f:
        flow_data.append(json.load(f))

flow_index_df = pd.DataFrame([flow["_source"] for flow in flow_data])

result = pd.DataFrame()

for file in os.listdir(processes_dir):
    with open(os.path.join(processes_dir, file), "r") as f:
        processes_data = json.load(f)
        process_flows = (
            processes_data["_source"]["input_flows"]
            + processes_data["_source"]["output_flows"]
        )
        process_flows_df = pd.DataFrame(process_flows)
        process_flows_df["amount"] = process_flows_df["amount"].astype(float)

        merged_df = pd.merge(process_flows_df, flow_index_df, on="flow_id", how="left")

        merged_df = merged_df.assign(
            process_id=processes_data["_source"]["process_id"],
            process_name=processes_data["_source"]["process_name"],
            process_type=processes_data["_source"]["process_type"],
            process_category=processes_data["_source"]["process_category"],
            process_location=processes_data["_source"]["process_location"],
        )

        result = pd.concat([result, merged_df], ignore_index=True)

result.to_pickle("output/merged_data.pkl")
result.to_csv("output/merged_data.csv", index=False)
