import os


EXPERIMENTS_PATH = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments"
FAILURE_INJECTION_PATH = os.path.join(EXPERIMENTS_PATH, "failure injection")
NORMAL_PATH = os.path.join(EXPERIMENTS_PATH, "normal")
NORMAL_GCLOUD_METRICS_PATH = os.path.join(NORMAL_PATH, "gcloud-metrics")
METRIC_TYPE_MAP_PATH = os.path.join(NORMAL_GCLOUD_METRICS_PATH, "metric_type_map.csv")
PROMETHEUS_TARGET_METRICS_PATH = os.path.join(
    NORMAL_PATH, "prometheus_target_metrics.csv"
)
GCLOUD_TARGET_METRICS_PATH = os.path.join(NORMAL_PATH, "gcloud_target_metrics.csv")
NORMAL_GCLOUD_AGGREGATED_METRICS_PATH = os.path.join(
    NORMAL_GCLOUD_METRICS_PATH, "gcloud_aggregated"
)
NORMAL_PROMETHEUS_AGGREGATED_METRICS_PATH = os.path.join(
    NORMAL_PATH, "prometheus_aggregated", "day-1"
)
GCLOUD_UNIFIED_PATH = os.path.join(EXPERIMENTS_PATH, "gcloud_unified", "kpi-map")
PROMETHEUS_UNIFIED_PATH = os.path.join(
    EXPERIMENTS_PATH, "prometheus_unified", "kpi-map"
)
