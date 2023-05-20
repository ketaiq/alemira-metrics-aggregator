import os

import pandas as pd
from app import (
    EXPERIMENTS_PATH,
    FAILURE_INJECTION_PATH,
    GCLOUD_TARGET_METRICS_PATH,
    GCLOUD_UNIFIED_PATH,
    METRIC_TYPE_MAP_PATH,
    NORMAL_GCLOUD_AGGREGATED_METRICS_PATH,
    NORMAL_GCLOUD_METRICS_PATH,
    NORMAL_PATH,
    NORMAL_PROMETHEUS_AGGREGATED_METRICS_PATH,
    PROMETHEUS_UNIFIED_PATH,
    PROMETHEUS_TARGET_METRICS_PATH,
)
from app.gcloud_aggregator import GCloudAggregator
from app.locust_aggregator import LocustAggregator
from app.merger import (
    copy_merged_faulty_metrics_for_experiments,
    merge_faulty_metrics_from_aggregated,
    merge_faulty_metrics_from_one_experiment,
    merge_faulty_metrics_from_unified,
    merge_gcloud_prometheus_metrics_in_one_experiment,
    merge_normal_metrics,
    reindex_kpis,
)
from app.prometheus_aggregator import PrometheusAggregator


def aggregate_faulty_metrics_from_log(log_filename: str):
    df = pd.read_csv(os.path.join(FAILURE_INJECTION_PATH, log_filename))
    if os.path.exists(GCLOUD_UNIFIED_PATH):
        gcloud_unified_path = GCLOUD_UNIFIED_PATH
    else:
        gcloud_unified_path = None
    if os.path.exists(PROMETHEUS_UNIFIED_PATH):
        prometheus_unified_path = PROMETHEUS_UNIFIED_PATH
    else:
        prometheus_unified_path = None
    for index, row in df.iterrows():
        exp_name = row["Folder Name"]
        exp_path = os.path.join(FAILURE_INJECTION_PATH, exp_name)
        print(f"Processing {index} {exp_name} ...")
        gcloud_aggregator = GCloudAggregator(
            exp_path,
            "gcloud_metrics",
            METRIC_TYPE_MAP_PATH,
            referred_kpi_map_path=gcloud_unified_path,
        )
        gcloud_aggregator.merge_all_submetrics()
        gcloud_aggregator.aggregate_all_metrics()

        prometheus_aggregator = PrometheusAggregator(
            exp_path,
            "prometheus-metrics",
            PROMETHEUS_TARGET_METRICS_PATH,
            referred_kpi_map_path=prometheus_unified_path,
        )
        prometheus_aggregator.merge_all_submetrics()
        prometheus_aggregator.aggregate_all_metrics()

        locust_aggregator = LocustAggregator(FAILURE_INJECTION_PATH, exp_name)
        locust_aggregator.aggregate_all_metrics()


def aggregate_faulty_metrics_in_folder():
    folders = [
        folder
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith("day-")
    ]
    for folder in folders:
        exp_path = os.path.join(FAILURE_INJECTION_PATH, folder)
        print(f"Processing {folder} ...")

        # gcloud_aggregator = GCloudAggregator(
        #     exp_path,
        #     "gcloud_metrics",
        #     GCLOUD_TARGET_METRICS_PATH,
        #     referred_kpi_map_path=GCLOUD_UNIFIED_PATH,
        # )
        # gcloud_aggregator.merge_all_submetrics()
        # gcloud_aggregator.aggregate_all_metrics()

        prometheus_aggregator = PrometheusAggregator(
            exp_path,
            "prometheus-metrics",
            PROMETHEUS_TARGET_METRICS_PATH,
        )
        prometheus_aggregator.merge_all_submetrics()
        prometheus_aggregator.aggregate_all_metrics()

        # locust_aggregator = LocustAggregator(FAILURE_INJECTION_PATH, folder)
        # locust_aggregator.aggregate_all_metrics()


def aggregate_faulty_metrics_in_one_experiment(exp_name: str):
    exp_path = os.path.join(FAILURE_INJECTION_PATH, exp_name)
    print(f"Processing {exp_name} ...")
    gcloud_aggregator = GCloudAggregator(
        exp_path,
        "gcloud_metrics",
        GCLOUD_TARGET_METRICS_PATH,
    )
    gcloud_aggregator.merge_all_submetrics()
    gcloud_aggregator.aggregate_all_metrics()

    prometheus_aggregator = PrometheusAggregator(
        exp_path,
        "prometheus-metrics",
        PROMETHEUS_TARGET_METRICS_PATH,
    )
    prometheus_aggregator.merge_all_submetrics()
    prometheus_aggregator.aggregate_all_metrics()

    locust_aggregator = LocustAggregator(FAILURE_INJECTION_PATH, exp_name)
    locust_aggregator.aggregate_all_metrics()


def aggregate_normal_metrics():
    for i in range(1, 15):
        # gcloud_folder = f"gcloud_metrics-day-{i}"
        # print(f"Processing {gcloud_folder} ...")
        # gcloud_aggregator = GCloudAggregator(
        #     NORMAL_GCLOUD_METRICS_PATH,
        #     gcloud_folder,
        #     GCLOUD_TARGET_METRICS_PATH,
        #     f"_day_{i}",
        #     referred_kpi_map_path=GCLOUD_UNIFIED_PATH,
        # )
        # gcloud_aggregator.merge_all_submetrics()
        # gcloud_aggregator.aggregate_all_metrics()

        folder = f"day-{i}"
        print(f"Processing {folder} ...")
        exp_path = os.path.join(NORMAL_PATH, folder)
        prometheus_aggregator = PrometheusAggregator(
            exp_path, "metrics", PROMETHEUS_TARGET_METRICS_PATH
        )
        prometheus_aggregator.merge_all_submetrics()
        prometheus_aggregator.aggregate_all_metrics()

        # locust_aggregator = LocustAggregator(NORMAL_PATH, folder)
        # locust_aggregator.aggregate_all_metrics()
    # folders = [f"day-{i}" for i in range(1, 15)]
    # LocustAggregator.merge_normal_metrics(NORMAL_PATH, folders)


def unify_prometheus_metrics():
    normal_aggregated_paths_list = [
        os.path.join(NORMAL_PATH, folder, f"prometheus_aggregated-{folder}")
        for folder in os.listdir(NORMAL_PATH)
        if folder.startswith("day-")
    ]
    faulty_aggregated_paths_list = [
        os.path.join(FAILURE_INJECTION_PATH, folder, "prometheus_aggregated")
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith("day-8-constant")
    ]
    aggregated_paths_list = normal_aggregated_paths_list + faulty_aggregated_paths_list
    unified_path = os.path.join(EXPERIMENTS_PATH, "prometheus_unified")
    normal_unified_kpi_paths_list = [
        os.path.join(unified_path, folder)
        for folder in os.listdir(NORMAL_PATH)
        if folder.startswith("day-")
    ]
    faulty_unified_kpi_paths_list = [
        os.path.join(unified_path, "faulty-" + folder)
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith("day-8-constant")
    ]
    unified_kpi_paths_list = (
        normal_unified_kpi_paths_list + faulty_unified_kpi_paths_list
    )
    if not os.path.exists(unified_path):
        os.mkdir(unified_path)
    df_target_metrics = pd.read_csv(PROMETHEUS_TARGET_METRICS_PATH)
    df_target_metrics.index += 1
    reindex_kpis(
        df_target_metrics, aggregated_paths_list, unified_path, unified_kpi_paths_list
    )


def unify_gcloud_metrics():
    normal_aggregated_paths_list = [
        os.path.join(NORMAL_GCLOUD_METRICS_PATH, folder)
        for folder in os.listdir(NORMAL_GCLOUD_METRICS_PATH)
        if folder.startswith("gcloud_aggregated-day-")
    ]

    faulty_aggregated_paths_list = [
        os.path.join(FAILURE_INJECTION_PATH, folder, "gcloud_aggregated")
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith("day-8-constant")
    ]
    aggregated_paths_list = normal_aggregated_paths_list + faulty_aggregated_paths_list
    unified_path = os.path.join(EXPERIMENTS_PATH, "gcloud_unified")
    normal_unified_kpi_paths_list = [
        os.path.join(unified_path, folder.removeprefix("gcloud_aggregated-"))
        for folder in os.listdir(NORMAL_GCLOUD_METRICS_PATH)
        if folder.startswith("gcloud_aggregated-day-")
    ]
    faulty_unified_kpi_paths_list = [
        os.path.join(unified_path, "faulty-" + folder)
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith("day-8-constant")
    ]
    unified_kpi_paths_list = (
        normal_unified_kpi_paths_list + faulty_unified_kpi_paths_list
    )
    if not os.path.exists(unified_path):
        os.mkdir(unified_path)
    df_target_metrics = pd.read_csv(GCLOUD_TARGET_METRICS_PATH).set_index("index")
    reindex_kpis(
        df_target_metrics, aggregated_paths_list, unified_path, unified_kpi_paths_list
    )


def gen_gcloud_target_metrics():
    metric_indices = [
        int(filename.lstrip("metric-").rstrip("-kpi-map.json"))
        for filename in os.listdir(
            os.path.join(NORMAL_GCLOUD_METRICS_PATH, "gcloud_aggregated-day-1")
        )
        if filename.endswith("kpi-map.json")
    ]
    df_metric_type_map = pd.read_csv(METRIC_TYPE_MAP_PATH).set_index("index")
    df_target_metrics = (
        df_metric_type_map.loc[metric_indices]
        .copy()
        .rename(columns={"metric_type": "name"})
        .sort_index()
        .reset_index()
    )
    df_target_metrics.to_csv(
        os.path.join(NORMAL_PATH, "gcloud_target_metrics.csv"), index=False
    )


def main():
    # aggregate_normal_metrics()
    # aggregate_faulty_metrics_in_folder()
    # aggregate_faulty_metrics("alemira_failure_injection_log_linear.csv")

    # merge_normal_metrics()
    # merge_faulty_metrics_from_aggregated("day")
    # copy_merged_faulty_metrics_for_experiments("day")

    exp_name = "day-8-linear-network-corrupt-userapi-052009"
    aggregate_faulty_metrics_in_one_experiment(exp_name)
    merge_faulty_metrics_from_one_experiment(exp_name)


if __name__ == "__main__":
    main()
