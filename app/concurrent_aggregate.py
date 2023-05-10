from multiprocessing import Pool
import os
from app import (
    FAILURE_INJECTION_PATH,
    GCLOUD_TARGET_METRICS_PATH,
    NORMAL_GCLOUD_METRICS_PATH,
    NORMAL_PATH,
    PROMETHEUS_TARGET_METRICS_PATH,
)
from app.gcloud_aggregator import GCloudAggregator
from app.locust_aggregator import LocustAggregator
from app.prometheus_aggregator import PrometheusAggregator


def perform_aggregation(
    gcloud_metrics_parent_path,
    gcloud_metrics_folder,
    prom_metrics_parent_path,
    prom_metrics_folder,
    locust_metrics_parent_path,
    locust_metrics_folder,
):
    gcloud_aggregator = GCloudAggregator(
        gcloud_metrics_parent_path,
        gcloud_metrics_folder,
        GCLOUD_TARGET_METRICS_PATH,
    )
    # gcloud_aggregator.merge_all_submetrics()
    gcloud_aggregator.aggregate_all_metrics()

    # prometheus_aggregator = PrometheusAggregator(
    #     prom_metrics_parent_path,
    #     prom_metrics_folder,
    #     PROMETHEUS_TARGET_METRICS_PATH,
    # )
    # prometheus_aggregator.merge_all_submetrics()
    # prometheus_aggregator.aggregate_all_metrics()

    # locust_aggregator = LocustAggregator(
    #     locust_metrics_parent_path, locust_metrics_folder
    # )
    # locust_aggregator.aggregate_all_metrics()


def gen_paths() -> list:
    paths = []
    # paths for normal metrics
    for i in range(1, 15):
        gcloud_metrics_parent_path = NORMAL_GCLOUD_METRICS_PATH
        gcloud_metrics_folder = f"gcloud_metrics-day-{i}"
        prom_metrics_parent_path = os.path.join(NORMAL_PATH, f"day-{i}")
        prom_metrics_folder = "metrics"
        locust_metrics_parent_path = NORMAL_PATH
        locust_metrics_folder = f"day-{i}"
        paths.append(
            (
                gcloud_metrics_parent_path,
                gcloud_metrics_folder,
                prom_metrics_parent_path,
                prom_metrics_folder,
                locust_metrics_parent_path,
                locust_metrics_folder,
            )
        )
    # paths for faulty metrics
    folders = [
        folder
        for folder in os.listdir(FAILURE_INJECTION_PATH)
        if folder.startswith("day-")
    ]
    for folder in folders:
        gcloud_metrics_parent_path = os.path.join(FAILURE_INJECTION_PATH, folder)
        gcloud_metrics_folder = "gcloud_metrics"
        prom_metrics_parent_path = os.path.join(FAILURE_INJECTION_PATH, folder)
        prom_metrics_folder = "prometheus-metrics"
        locust_metrics_parent_path = FAILURE_INJECTION_PATH
        locust_metrics_folder = folder
        paths.append(
            (
                gcloud_metrics_parent_path,
                gcloud_metrics_folder,
                prom_metrics_parent_path,
                prom_metrics_folder,
                locust_metrics_parent_path,
                locust_metrics_folder,
            )
        )
    return paths


if __name__ == "__main__":
    paths = gen_paths()
    with Pool(processes=5) as pool:
        pool.starmap(perform_aggregation, paths)
