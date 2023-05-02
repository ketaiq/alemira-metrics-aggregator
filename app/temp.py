import json
import os

import pandas as pd

from app import FAILURE_INJECTION_PATH, NORMAL_GCLOUD_METRICS_PATH


# agg_normal_metric_path = os.path.join(
#     NORMAL_GCLOUD_METRICS_PATH, "gcloud_aggregated-day-2"
# )
# agg_failure_metric_path = os.path.join(
#     FAILURE_INJECTION_PATH, "day-8-constant-cpu-stress-userapi-1", "gcloud_aggregated"
# )
# metric_indices = [
#     int(filename.lstrip("metric-").rstrip("-kpi-map.json"))
#     for filename in os.listdir(agg_normal_metric_path)
#     if filename.endswith("kpi-map.json")
# ]
# metric_indices.sort()
# if 12 in metric_indices:
#     metric_indices.remove(12)
# if 13 in metric_indices:
#     metric_indices.remove(13)


# def read_kpi_map(dir_path, metric_index):
#     kpi_map_path = os.path.join(dir_path, f"metric-{metric_index}-kpi-map.json")
#     with open(kpi_map_path) as fp:
#         kpi_map_list = json.load(fp)
#     kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
#     indices = [kpi_map["index"] for kpi_map in kpi_map_list]
#     return pd.DataFrame(kpi_maps, index=indices)


# sum_diff_columns = 0
# for index in metric_indices:
#     normal_kpi_map = read_kpi_map(agg_normal_metric_path, index).sort_index(axis=1)
#     failure_kpi_map = read_kpi_map(agg_failure_metric_path, index).sort_index(axis=1)
#     df_normal = pd.read_csv(os.path.join(agg_normal_metric_path, f"metric-{index}.csv"))
#     df_failure = pd.read_csv(
#         os.path.join(agg_failure_metric_path, f"metric-{index}.csv")
#     )

#     if len(df_normal.columns) != len(df_failure.columns):
#         num_columns = len(df_normal.columns) - len(df_failure.columns)
#         sum_diff_columns += num_columns
#         num_diff_kpis = len(normal_kpi_map) - len(failure_kpi_map)
#         print(index)
# print(sum_diff_columns)

print(pd.read_csv("gcloud_valuable_metrics.csv")["index"].to_list())
