import json
import os
import re
import warnings

import jsonlines
import pandas as pd
from app.aggregator import Aggregator


class GCloudAggregator(Aggregator):
    def __init__(
        self,
        metrics_parent_path: str,
        metrics_folder: str,
        metric_type_map_path: str,
        output_suffix: str = "",
        referred_kpi_map_path: str = None,
    ):
        self.metrics_path = os.path.join(metrics_parent_path, metrics_folder)
        self.merged_submetrics_path = os.path.join(
            metrics_parent_path, "gcloud_combined" + output_suffix
        )
        self.aggregated_metrics_path = os.path.join(
            metrics_parent_path, "gcloud_aggregated" + output_suffix
        )
        self.complete_time_series_path = os.path.join(
            metrics_parent_path, f"gcloud-complete-time-series{output_suffix}.csv"
        )
        self.metric_type_map = pd.read_csv(metric_type_map_path).set_index("index")
        self.referred_agg_path = referred_kpi_map_path
        if not os.path.exists(self.merged_submetrics_path):
            os.mkdir(self.merged_submetrics_path)
        if not os.path.exists(self.aggregated_metrics_path):
            os.mkdir(self.aggregated_metrics_path)

    def _merge_submetrics(self, metric_path: str, metric_index: int):
        """Merge all available KPIs in one metric to produce a dataframe."""
        # copy KPI map to destination path
        kpi_map_path = os.path.join(
            metric_path,
            "kpi_map.jsonl",
        )
        with jsonlines.open(kpi_map_path) as reader:
            kpi_map_list = [obj for obj in reader]
        with open(
            os.path.join(
                self.merged_submetrics_path, f"metric-{metric_index}-kpi-map.json"
            ),
            "w",
        ) as fp:
            json.dump(kpi_map_list, fp)
        # merge KPIs in the metric type
        kpi_list = []
        for kpi_map in kpi_map_list:
            kpi_index = kpi_map["index"]
            kpi_path = os.path.join(
                metric_path,
                f"kpi-{kpi_index}.csv",
            )
            df_kpi = pd.read_csv(kpi_path)
            # round timestamp to minute
            df_kpi["timestamp"] = pd.to_datetime(
                df_kpi["timestamp"], unit="s"
            ).dt.round("min")
            df_kpi = df_kpi.set_index("timestamp").add_prefix(f"kpi-{kpi_index}-")
            # aggregate duplicated minutes
            if len(df_kpi.index) != len(df_kpi.index.drop_duplicates()):
                df_kpi = df_kpi.groupby("timestamp").agg("mean")
            kpi_list.append(df_kpi)
        df_kpis = pd.concat(kpi_list, axis=1)
        df_kpis.to_csv(
            os.path.join(self.merged_submetrics_path, f"metric-{metric_index}.csv")
        )

    def merge_all_submetrics(self):
        metric_type_folders = [
            folder
            for folder in os.listdir(self.metrics_path)
            if folder.startswith("metric-type")
        ]
        for folder in metric_type_folders:
            metric_index = folder.removeprefix("metric-type-")
            metric_path = os.path.join(self.metrics_path, f"metric-type-{metric_index}")
            self._merge_submetrics(metric_path, metric_index)

    def get_df_metric(self, metric_index: int) -> pd.DataFrame:
        metric_path = os.path.join(
            self.merged_submetrics_path, f"metric-{metric_index}.csv"
        )
        return pd.read_csv(metric_path)

    def aggregate_one_metric(self, metric_index: int):
        """Aggregate all available KPIs in one metric to reduce dimensionality."""
        metric_name = self.metric_type_map.loc[metric_index]["metric_type"]
        df_kpi_map = Aggregator.read_df_kpi_map(
            metric_index, self.merged_submetrics_path
        )
        if metric_name.startswith("kubernetes.io/autoscaler"):
            self.adapt_no_agg(metric_index, df_kpi_map)
        elif metric_name.startswith("kubernetes.io/container"):
            self.aggregate_with_container_name(metric_index, df_kpi_map)
        elif metric_name.startswith("kubernetes.io/pod") or metric_name.startswith(
            "networking.googleapis.com/pod_flow"
        ):
            self.aggregate_with_pod_service(metric_index, df_kpi_map)
        elif metric_name.startswith("kubernetes.io/node") or metric_name.startswith(
            "networking.googleapis.com/vpc_flow"
        ):
            self.aggregate_with_all_kpis(metric_index, df_kpi_map)
        elif metric_name.startswith(
            "networking.googleapis.com/node_flow"
        ) or metric_name.startswith("networking.googleapis.com/vm_flow"):
            if "remote_network" in df_kpi_map.columns:
                self.aggregate_with_selected_labels(
                    metric_index, df_kpi_map[["remote_network"]]
                )
            else:
                self.aggregate_with_all_kpis(metric_index, df_kpi_map)
        else:
            print(f"Unsupported aggregation on {metric_name}")

    @staticmethod
    def index_list(series) -> list:
        return series.to_list()

    @staticmethod
    def is_distribution(cols: list) -> bool:
        for col in cols:
            if "count" in col or "mean" in col or "sum_of_squared_deviation" in col:
                return True
        return False

    @staticmethod
    def percentile(n):
        def percentile_(series: pd.Series):
            return series.quantile(n)

        n_int = int(n * 100)
        percentile_.__name__ = f"percentile_{n_int}"
        return percentile_

    def aggregate_with_all_kpis(self, metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with only different node names."""
        print(f"Aggregating metric {metric_index} with all KPIs ...")
        group_columns = ["project_id"]
        df_same = df_kpi_map[group_columns]
        df_same = (
            df_same.reset_index()
            .groupby(group_columns)
            .agg(GCloudAggregator.index_list)
        )
        self.aggregate(metric_index, df_same)

    def aggregate_with_container_name(
        self, metric_index: int, df_kpi_map: pd.DataFrame
    ):
        """Aggregate KPIs with same container name."""
        print(f"Aggregating metric {metric_index} with same container name ...")
        group_columns = ["container_name"]
        df_kpi_map_unique = df_kpi_map[group_columns]
        df_kpi_map_unique = (
            df_kpi_map_unique.reset_index()
            .groupby(group_columns)
            .agg(GCloudAggregator.index_list)
        )
        self.aggregate(metric_index, df_kpi_map_unique)

    def aggregate_with_pod_service(self, metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with same pod service extracted from the pod name."""
        print(f"Aggregating metric {metric_index} with same pod service ...")
        df_kpi_map["pod_name"] = df_kpi_map["pod_name"].str.extract(r"(alms[-a-z]+)-")
        group_columns = ["pod_name"]
        df_kpi_map = df_kpi_map[group_columns]
        df_kpi_map_unique = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(GCloudAggregator.index_list)
        )
        self.aggregate(metric_index, df_kpi_map_unique)

    def aggregate_with_selected_labels(
        self, metric_index: int, df_kpi_map: pd.DataFrame
    ):
        """Aggregate KPIs with selected labels."""
        print(f"Aggregating metric {metric_index} with selected labels ...")
        group_columns = df_kpi_map.columns.to_list()
        df_kpi_map_unique = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(GCloudAggregator.index_list)
        )
        self.aggregate(metric_index, df_kpi_map_unique)

    def adapt_no_agg(self, metric_index: int, df_kpi_map: pd.DataFrame):
        """Adapt metrics no need for aggregation."""
        print(f"Adapting metric {metric_index} without aggregation ...")
        df_metric = self.get_df_metric(metric_index).set_index("timestamp")
        new_kpi_map_list = []
        if self.referred_agg_path is not None:
            df_referred_kpi_map = Aggregator.read_df_kpi_map(
                metric_index, self.referred_agg_path
            )
        else:
            new_kpi_map_index = 1
        for i in df_kpi_map.index:
            original_column_name = f"kpi-{i}-value"
            if original_column_name in df_metric.columns:
                if self.referred_agg_path is not None:
                    # use referred KPI index
                    row = df_kpi_map.loc[i]
                    check_same_row = (df_referred_kpi_map == row).all(axis=1)
                    matched_row = check_same_row[check_same_row].index
                    if not matched_row.empty:
                        new_kpi_map_index = int(matched_row[0])
                    else:
                        continue
                # adapt KPI map
                new_kpi_map = {
                    "index": new_kpi_map_index,
                    "kpi": df_kpi_map.loc[i].to_dict(),
                }
                new_kpi_map_list.append(new_kpi_map)
                # adapt metric
                df_metric.rename(
                    columns={original_column_name: f"agg-kpi-{new_kpi_map_index}"},
                    inplace=True,
                )
                new_kpi_map_index += 1
        with open(
            os.path.join(
                self.aggregated_metrics_path, f"metric-{metric_index}-kpi-map.json"
            ),
            "w",
        ) as fp:
            json.dump(new_kpi_map_list, fp)
        df_metric.to_csv(
            os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
        )

    @staticmethod
    def gen_df_metric_agg(df_metric_to_agg: pd.DataFrame) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df_metric_agg = df_metric_to_agg.agg(
                [
                    "min",
                    "max",
                    "mean",
                    "median",
                    "std",
                    "count",
                ],
                axis=1,
            )
            return df_metric_agg

    def aggregate(self, metric_index: int, df_kpi_map_unique: pd.DataFrame):
        df_metric = self.get_df_metric(metric_index).set_index("timestamp")
        if self.referred_agg_path is not None:
            df_referred_kpi_map = Aggregator.read_df_kpi_map(
                metric_index, self.referred_agg_path
            )
        else:
            new_kpi_map_index = 1
        new_kpi_map_list = []
        df_agg_list = []
        df_kpi_map_unique = df_kpi_map_unique.rename(
            columns={"index": "index_list"}
        ).reset_index()
        for i in df_kpi_map_unique.index:
            if self.referred_agg_path is not None:
                # use referred KPI index
                row = df_kpi_map_unique.loc[i].drop("index_list")
                check_same_row = (df_referred_kpi_map == row).all(axis=1)
                matched_row = check_same_row[check_same_row].index
                if not matched_row.empty:
                    new_kpi_map_index = int(matched_row[0])
                else:
                    continue
            # generate new KPI map
            new_kpi_map = {
                "index": new_kpi_map_index,
                "kpi": df_kpi_map_unique.loc[i].drop("index_list").to_dict(),
            }
            new_kpi_map_list.append(new_kpi_map)
            # select columns to merge
            indices_with_metrics = [
                int(re.search(r"kpi-([0-9]+)-.*", column)[1])
                for column in df_metric.columns.to_list()
            ]
            valid_indices = list(
                set(df_kpi_map_unique.loc[i]["index_list"]) & set(indices_with_metrics)
            )
            if GCloudAggregator.is_distribution(df_metric.columns):
                for suffix in ["count", "mean", "sum_of_squared_deviation"]:
                    columns_to_merge = [f"kpi-{i}-{suffix}" for i in valid_indices]
                    df_metric_to_agg = df_metric[columns_to_merge]
                    df_metric_agg = (
                        GCloudAggregator.gen_df_metric_agg(df_metric_to_agg)
                        .add_prefix(f"agg-kpi-{new_kpi_map_index}-")
                        .add_suffix(f"-{suffix}")
                    )
                    df_agg_list.append(df_metric_agg)
            else:
                columns_to_merge = [f"kpi-{i}-value" for i in valid_indices]
                df_metric_to_agg = df_metric[columns_to_merge]
                df_metric_agg = GCloudAggregator.gen_df_metric_agg(
                    df_metric_to_agg
                ).add_prefix(f"agg-kpi-{new_kpi_map_index}-")
                df_agg_list.append(df_metric_agg)
            new_kpi_map_index += 1
        df_complete_agg = pd.concat(df_agg_list, axis=1)
        if not df_complete_agg.empty:
            with open(
                os.path.join(
                    self.aggregated_metrics_path, f"metric-{metric_index}-kpi-map.json"
                ),
                "w",
            ) as fp:
                json.dump(new_kpi_map_list, fp)
            df_complete_agg.to_csv(
                os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
            )

    def aggregate_all_metrics(self):
        metric_indices = self.get_metric_indices()
        for metric_index in metric_indices:
            self.aggregate_one_metric(metric_index)

    def merge_metrics(self):
        df_all_list = []
        metric_indices = self.get_metric_indices()
        for metric_index in metric_indices:
            print(f"Processing metric {metric_index} ...")
            metric_path = os.path.join(
                self.aggregated_metrics_path, f"metric-{metric_index}.csv"
            )
            df_metric = pd.read_csv(metric_path).set_index("timestamp")
            df_all_list.append(df_metric.add_prefix(f"metric-{metric_index}-"))
        df_all = pd.concat(df_all_list, axis=1)
        num_cols = len(df_all.columns)
        num_rows = len(df_all)
        print(f"{num_rows} rows x {num_cols} columns")
        df_all.to_csv(self.complete_time_series_path)

    def get_metric_indices(self) -> list:
        metric_indices = [
            int(filename.removeprefix("metric-").removesuffix("-kpi-map.json"))
            for filename in os.listdir(self.merged_submetrics_path)
            if filename.endswith("kpi-map.json")
        ]
        if 12 in metric_indices:
            metric_indices.remove(12)
        if 13 in metric_indices:
            metric_indices.remove(13)
        metric_indices.sort()
        return metric_indices
