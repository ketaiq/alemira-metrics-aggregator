import os
import re
import pandas as pd
import json
from app import (
    EXPERIMENTS_PATH,
    FAILURE_INJECTION_PATH,
    GCLOUD_TARGET_METRICS_PATH,
    NORMAL_GCLOUD_METRICS_PATH,
    NORMAL_PATH,
    PROMETHEUS_TARGET_METRICS_PATH,
)

from app.aggregator import Aggregator


def reindex_kpis(
    df_target_metrics: pd.DataFrame,
    aggregated_paths_list: list,
    unified_path: str,
    unified_kpi_paths_list: list,
):
    """Reindex Prometheus KPIs based on all colleced data."""
    num_metrics = df_target_metrics.index.max()
    for metric_index in df_target_metrics.index:
        metric_name = df_target_metrics.loc[metric_index]["name"]
        print(f"Processing [{metric_index}/{num_metrics}] {metric_name} ...")
        # create a unified KPI map
        df_unified_kpi_map = pd.DataFrame()
        for agg_path in aggregated_paths_list:
            df_kpi_map = Aggregator.read_df_kpi_map(metric_index, agg_path)
            df_unified_kpi_map = pd.concat(
                [df_unified_kpi_map, df_kpi_map], ignore_index=True
            )
        df_unified_kpi_map = df_unified_kpi_map.drop_duplicates().sort_index(axis=1)
        df_unified_kpi_map.sort_values(
            df_unified_kpi_map.columns.to_list(), inplace=True, ignore_index=True
        )
        df_unified_kpi_map.index += 1
        write_unified_kpi_map(df_unified_kpi_map, unified_path, metric_index)
        # rename columns in time series of KPIs
        for i in range(len(aggregated_paths_list)):
            agg_path = aggregated_paths_list[i]
            unified_kpi_path = unified_kpi_paths_list[i]
            df_kpi = pd.read_csv(
                os.path.join(agg_path, f"metric-{metric_index}.csv")
            ).set_index("timestamp")
            unified_df_kpi_list = []
            for kpi_index, row in df_kpi_map.iterrows():
                rename_candidate_kpi_columns(
                    df_unified_kpi_map, df_kpi, kpi_index, row, unified_df_kpi_list
                )
            write_unified_kpi(metric_index, unified_df_kpi_list, unified_kpi_path)


def rename_candidate_kpi_columns(
    df_unified_kpi_map: pd.DataFrame,
    df_kpi: pd.DataFrame,
    kpi_index: int,
    row: pd.Series,
    unified_df_kpi_list: list,
):
    # find KPI map
    check_same_row = (df_unified_kpi_map == row).all(axis=1)
    same_row_indices = check_same_row[check_same_row].index
    if same_row_indices.empty:
        unified_kpi_map_index = len(df_unified_kpi_map.index) + 1
        df_unified_kpi_map.loc[unified_kpi_map_index] = row
    else:
        unified_kpi_map_index = int(same_row_indices[0])
    # find candidate columns for renaming
    candidate_cols = [
        col
        for col in df_kpi.columns
        if col.startswith(f"agg-kpi-{kpi_index}-") or col == f"agg-kpi-{kpi_index}"
    ]
    # generate columns for unified KPIs
    unified_kpi_columns = []
    for col in candidate_cols:
        unified_kpi_columns.append(re.sub(r"[0-9]+", str(unified_kpi_map_index), col))
    df_candidate_kpi = df_kpi[candidate_cols].copy()
    df_candidate_kpi.rename(
        columns={
            candidate_cols[i]: unified_kpi_columns[i]
            for i in range(len(candidate_cols))
        },
        inplace=True,
    )
    unified_df_kpi_list.append(df_candidate_kpi)


def write_unified_kpi(
    metric_index: int,
    unified_df_kpi_list: list,
    unified_kpi_path: str,
):
    if not os.path.exists(unified_kpi_path):
        os.mkdir(unified_kpi_path)
    df_kpi = pd.concat(unified_df_kpi_list, axis=1).sort_index(
        axis=1, key=lambda x: x.str.extract(r"([0-9]+)", expand=False).astype(int)
    )
    df_kpi.to_csv(os.path.join(unified_kpi_path, f"metric-{metric_index}.csv"))


def write_unified_kpi_map(
    df_unified_kpi_map: pd.DataFrame, unified_path: str, metric_index: int
):
    output_folder = os.path.join(unified_path, "kpi-map")
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    df_unified_kpi_map.to_csv(
        os.path.join(output_folder, f"metric-{metric_index}-kpi-map.csv")
    )


def merge_gcloud_prometheus_metrics_in_one_experiment(
    df_gcloud_target_metrics: pd.DataFrame,
    df_prometheus_target_metrics: pd.DataFrame,
    gcloud_metrics_path: str,
    prometheus_metrics_path: str,
) -> pd.DataFrame:
    gcloud_df_list = []
    for metric_index in df_gcloud_target_metrics.index:
        metric_path = os.path.join(gcloud_metrics_path, f"metric-{metric_index}.csv")
        df_metric = pd.read_csv(metric_path).set_index("timestamp")
        gcloud_df_list.append(df_metric.add_prefix(f"gm-{metric_index}-"))
    df_gcloud = pd.concat(gcloud_df_list, axis=1)
    prometheus_df_list = []
    for metric_index in df_prometheus_target_metrics.index:
        metric_path = os.path.join(
            prometheus_metrics_path, f"metric-{metric_index}.csv"
        )
        df_metric = pd.read_csv(metric_path).set_index("timestamp")
        prometheus_df_list.append(df_metric.add_prefix(f"pm-{metric_index}-"))
    df_prometheus = pd.concat(prometheus_df_list, axis=1)
    df_gp = pd.concat([df_gcloud, df_prometheus], axis=1)
    df_gp.index = pd.to_datetime(df_gp.index)
    return df_gp


def merge_gcloud_prometheus_in_one_experiment(
    df_gcloud_target_metrics: pd.DataFrame,
    gcloud_metrics_path: str,
):
    gcloud_df_list = []
    for metric_index in df_gcloud_target_metrics.index:
        metric_path = os.path.join(gcloud_metrics_path, f"metric-{metric_index}.csv")
        df_metric = pd.read_csv(metric_path).set_index("timestamp")
        df_metric.index = pd.to_datetime(df_metric.index)
        gcloud_df_list.append(df_metric.add_prefix(f"gm-{metric_index}-"))
    return pd.concat(gcloud_df_list, axis=1).sort_index()


def merge_normal_metrics():
    gcloud_paths = [
        os.path.join(NORMAL_GCLOUD_METRICS_PATH, folder)
        for folder in os.listdir(NORMAL_GCLOUD_METRICS_PATH)
        if folder.startswith("gcloud_aggregated_day_")
    ]
    prometheus_paths = [
        os.path.join(NORMAL_PATH, f"day-{i}", "prometheus_aggregated")
        for i in range(1, 15)
    ]
    if len(gcloud_paths) != len(prometheus_paths):
        print("Two paths list have different lengths!")
    df_gcloud_target_metrics = pd.read_csv(GCLOUD_TARGET_METRICS_PATH).set_index(
        "index"
    )
    df_prometheus_target_metrics = pd.read_csv(PROMETHEUS_TARGET_METRICS_PATH)
    df_prometheus_target_metrics.index += 1
    df_gp_list = []
    for i in range(len(gcloud_paths)):
        df_gp_list.append(
            # merge_gcloud_prometheus_in_one_experiment(
            #     df_gcloud_target_metrics, gcloud_paths[i]
            # )
            merge_gcloud_prometheus_metrics_in_one_experiment(
                df_gcloud_target_metrics,
                df_prometheus_target_metrics,
                gcloud_paths[i],
                prometheus_paths[i],
            )
        )
    df_gp = pd.concat(df_gp_list).sort_index()
    df_locust = pd.read_csv(
        os.path.join(NORMAL_PATH, "locust_normal_stats.csv")
    ).set_index("timestamp")
    df_locust.index = pd.to_datetime(df_locust.index)
    df_complete = df_locust.join(df_gp)
    if len(df_complete.index) != len(df_complete.index.drop_duplicates()):
        df_complete = df_complete.groupby("timestamp").agg("mean")
    num_rows = len(df_complete)
    num_columns = len(df_complete.columns)
    print(f"{num_rows} rows x {num_columns} columns")
    df_complete.sort_index().to_csv(
        os.path.join(NORMAL_PATH, "complete_normal_time_series.csv")
    )


def merge_faulty_metrics_from_unified():
    gcloud_paths = [
        os.path.join(EXPERIMENTS_PATH, "gcloud_unified", folder)
        for folder in os.listdir(os.path.join(EXPERIMENTS_PATH, "gcloud_unified"))
        if folder.startswith("faulty-")
    ]
    prometheus_paths = [
        os.path.join(EXPERIMENTS_PATH, "prometheus_unified", folder)
        for folder in os.listdir(os.path.join(EXPERIMENTS_PATH, "prometheus_unified"))
        if folder.startswith("faulty-")
    ]
    if len(gcloud_paths) != len(prometheus_paths):
        print("Two paths list have different lengths!")
    df_gcloud_target_metrics = pd.read_csv(GCLOUD_TARGET_METRICS_PATH).set_index(
        "index"
    )
    df_prometheus_target_metrics = pd.read_csv(PROMETHEUS_TARGET_METRICS_PATH)
    df_prometheus_target_metrics.index += 1
    for i in range(len(gcloud_paths)):
        folder = os.path.basename(prometheus_paths[i]).removeprefix("faulty-")
        print(f"Processing {folder} ...")
        df_gp = merge_gcloud_prometheus_metrics_in_one_experiment(
            df_gcloud_target_metrics,
            df_prometheus_target_metrics,
            gcloud_paths[i],
            prometheus_paths[i],
        )
        df_locust = pd.read_csv(
            os.path.join(FAILURE_INJECTION_PATH, folder, "locust_aggregated_stats.csv")
        ).set_index("timestamp")
        df_complete = df_gp.join(df_locust, how="inner")
        if len(df_complete.index) != len(df_complete.index.drop_duplicates()):
            df_complete = df_complete.groupby("timestamp").agg("mean")
        num_rows = len(df_complete)
        num_columns = len(df_complete.columns)
        print(f"{num_rows} rows x {num_columns} columns")
        df_complete.to_csv(
            os.path.join(FAILURE_INJECTION_PATH, folder, f"{folder}.csv")
        )


def merge_faulty_metrics_from_aggregated(common_prefix: str):
    gcloud_paths = [
        os.path.join(FAILURE_INJECTION_PATH, folder, "gcloud_aggregated")
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith(common_prefix)
    ]
    prometheus_paths = [
        os.path.join(FAILURE_INJECTION_PATH, folder, "prometheus_aggregated")
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith(common_prefix)
    ]
    folders = [
        folder
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith(common_prefix)
    ]
    if len(gcloud_paths) != len(prometheus_paths):
        print("Two paths list have different lengths!")
    df_gcloud_target_metrics = pd.read_csv(GCLOUD_TARGET_METRICS_PATH).set_index(
        "index"
    )
    df_prometheus_target_metrics = pd.read_csv(PROMETHEUS_TARGET_METRICS_PATH)
    df_prometheus_target_metrics.index += 1
    for i in range(len(gcloud_paths)):
        folder = folders[i]
        print(f"Processing {folder} ...")
        df_gp = merge_gcloud_prometheus_metrics_in_one_experiment(
            df_gcloud_target_metrics,
            df_prometheus_target_metrics,
            gcloud_paths[i],
            prometheus_paths[i],
        )
        # df_gp = merge_gcloud_prometheus_in_one_experiment(
        #     df_gcloud_target_metrics, gcloud_paths[i]
        # )
        df_locust = pd.read_csv(
            os.path.join(FAILURE_INJECTION_PATH, folder, "locust_aggregated_stats.csv")
        ).set_index("timestamp")
        df_locust.index = pd.to_datetime(df_locust.index)
        df_complete = df_gp.join(df_locust, how="inner")
        if len(df_complete.index) != len(df_complete.index.drop_duplicates()):
            df_complete = df_complete.groupby("timestamp").agg("mean")
        num_rows = len(df_complete)
        num_columns = len(df_complete.columns)
        print(f"{num_rows} rows x {num_columns} columns")
        df_complete.to_csv(
            os.path.join(FAILURE_INJECTION_PATH, folder, f"{folder}.csv")
        )


def merge_faulty_metrics_from_one_experiment(exp_name: str):
    gcloud_path = os.path.join(FAILURE_INJECTION_PATH, exp_name, "gcloud_aggregated")
    prometheus_path = os.path.join(
        FAILURE_INJECTION_PATH, exp_name, "prometheus_aggregated"
    )
    df_gcloud_target_metrics = pd.read_csv(GCLOUD_TARGET_METRICS_PATH).set_index(
        "index"
    )
    df_prometheus_target_metrics = pd.read_csv(PROMETHEUS_TARGET_METRICS_PATH)
    df_prometheus_target_metrics.index += 1
    df_gp = merge_gcloud_prometheus_metrics_in_one_experiment(
        df_gcloud_target_metrics,
        df_prometheus_target_metrics,
        gcloud_path,
        prometheus_path,
    )
    df_locust = pd.read_csv(
        os.path.join(FAILURE_INJECTION_PATH, exp_name, "locust_aggregated_stats.csv")
    ).set_index("timestamp")
    df_locust.index = pd.to_datetime(df_locust.index)
    df_complete = df_gp.join(df_locust, how="inner")
    if len(df_complete.index) != len(df_complete.index.drop_duplicates()):
        df_complete = df_complete.groupby("timestamp").agg("mean")
    num_rows = len(df_complete)
    num_columns = len(df_complete.columns)
    print(f"{num_rows} rows x {num_columns} columns")
    df_complete.to_csv(
        os.path.join(FAILURE_INJECTION_PATH, exp_name, f"{exp_name}.csv")
    )
