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
    exp_name = "linear-network-corrupt-redis-092111"
    aggregate_faulty_metrics_in_one_experiment(exp_name)
    merge_faulty_metrics_from_one_experiment(exp_name)


if __name__ == "__main__":
    main()
