import json
import os
import warnings
import pandas as pd
from app.aggregator import Aggregator


class Metric:
    def __init__(self, name: str, data):
        self.metric_name = name
        self.metric_items = []
        if self.check_data(data):
            if type(data) is dict:
                for item in data["result"]:
                    if item["values"]:
                        self.metric_items.append(MetricItem(item))
            elif type(data) is list:
                self.metric_items = data
        self.num_metric_items = len(self.metric_items)

    def check_data(self, data) -> bool:
        if not data:
            print(f"Empty data in {self.metric_name}!")
            return False
        if type(data) is dict:
            if not data["result"]:
                print(f"Empty results in {self.metric_name}!")
                return False
            elif data["resultType"] != "matrix":
                print(
                    f"The format of input data is not supported in {self.metric_name}!"
                )
                return False
            else:
                return True
        elif type(data) is list:
            for item in data:
                if type(item) is not MetricItem:
                    print(f"The type of {item} is not supported in metric collection!")
                    return False
            return True
        else:
            print(f"The type of {data} is not supported in metric data!")
            return False


class MetricItem:
    def __init__(self, result_item: dict):
        self.metadata = result_item["metric"]
        self.values = pd.DataFrame(
            result_item["values"], columns=["timestamp", "value"]
        ).astype({"timestamp": "int64", "value": "float64"})
        self.values["timestamp"] = pd.to_datetime(
            self.values["timestamp"], unit="s"
        ).dt.round("min")
        if len(self.values["timestamp"]) != len(
            self.values["timestamp"].drop_duplicates()
        ):
            self.values = self.values.groupby("timestamp").agg("mean").reset_index()


class PrometheusAggregator(Aggregator):
    def __init__(
        self,
        metrics_parent_path: str,
        metrics_folder: str,
        target_metrics_path: str,
        output_suffix: str = "",
        referred_kpi_map_path: str = None,
    ):
        self.metrics_path = os.path.join(metrics_parent_path, metrics_folder)
        self.merged_submetrics_path = os.path.join(
            metrics_parent_path, "prometheus_combined" + output_suffix
        )
        self.aggregated_metrics_path = os.path.join(
            metrics_parent_path, "prometheus_aggregated" + output_suffix
        )
        self.complete_time_series_path = os.path.join(
            metrics_parent_path, f"prometheus-complete-time-series{output_suffix}.csv"
        )
        self.target_metrics = pd.read_csv(target_metrics_path)
        self.target_metrics.index += 1
        self.referred_agg_path = referred_kpi_map_path
        if not os.path.exists(self.merged_submetrics_path):
            os.mkdir(self.merged_submetrics_path)
        if not os.path.exists(self.aggregated_metrics_path):
            os.mkdir(self.aggregated_metrics_path)

    def _get_metric_index(self, metric_name: str):
        """Get metric index in metric names map."""
        metric_names_map_path = os.path.join(self.metrics_path, "metric_names_map.json")
        with open(metric_names_map_path) as fp:
            metric_names_map = list(json.load(fp).values())
        return metric_names_map.index(metric_name) + 1

    def _get_metric(self, metric_name: str) -> Metric:
        metric_index = self._get_metric_index(metric_name)
        metric_path = os.path.join(
            self.metrics_path, f"metric-{metric_index}-day-1.json"
        )
        metric_data = None
        try:
            with open(metric_path) as fp:
                metric_data = json.load(fp)
        except json.JSONDecodeError as e:
            print(f"{metric_name} in {self.metrics_path} cannot be decoded!")
        return Metric(metric_name, metric_data)

    def merge_all_submetrics(self):
        num_metrics = len(self.target_metrics)
        for metric_index in self.target_metrics.index:
            metric_name = self.target_metrics.loc[metric_index]["name"]
            print(f"Processing {metric_index}/{num_metrics} {metric_name} ...")
            metric = self._get_metric(metric_name)
            kpi_map_list = []  # contains each metadata from metric items
            metric_items_df_list = []  # contains each dataframe from metric items
            for i in range(metric.num_metric_items):
                item = metric.metric_items[i]
                metric_items_df_list.append(
                    item.values.set_index("timestamp").add_suffix(f"-{i}")
                )
                kpi_map_list.append(item.metadata)
            df_kpi_map = pd.DataFrame(kpi_map_list)
            df_kpi = pd.concat(metric_items_df_list, axis=1)
            df_kpi_map.to_csv(
                os.path.join(
                    self.merged_submetrics_path, f"metric-{metric_index}-kpi-map.csv"
                ),
                index=False,
            )
            df_kpi.to_csv(
                os.path.join(self.merged_submetrics_path, f"metric-{metric_index}.csv")
            )

    def aggregate_one_metric(self, metric_index: int):
        metric_name = self.target_metrics.loc[metric_index]["name"]
        print(f"Aggregating prometheus metric {metric_index} {metric_name} ...")
        df_kpi_map = pd.read_csv(
            os.path.join(
                self.merged_submetrics_path, f"metric-{metric_index}-kpi-map.csv"
            )
        )
        df_kpi = pd.read_csv(
            os.path.join(self.merged_submetrics_path, f"metric-{metric_index}.csv")
        ).set_index("timestamp")
        if metric_name == "ALERTS":
            self.aggregate_alerts_time_series(metric_index, df_kpi_map, df_kpi)
        elif metric_name == "ALERTS_FOR_STATE":
            self.aggregate_alerts_for_state_time_series(
                metric_index, df_kpi_map, df_kpi
            )
        elif metric_name.startswith("container"):
            self.aggregate_container_time_series(metric_index, df_kpi_map, df_kpi)
        elif metric_name.startswith("instance") or metric_name.startswith("node_"):
            self.aggregate_time_series_in_one(metric_index, df_kpi_map, df_kpi)
        elif metric_name.startswith("kube"):
            self.aggregate_kube_time_series(metric_index, df_kpi_map, df_kpi)
        elif (
            metric_name.startswith("namespace")
            or metric_name.startswith(":node")
            or metric_name.startswith("node:")
        ):
            self.adapt_no_agg_time_series(metric_index, df_kpi_map, df_kpi)

    @staticmethod
    def index_list(series) -> list:
        return series.to_list()

    def aggregate_alerts_time_series(
        self, metric_index: int, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
    ):
        # ALERTS contain only counts of alerts
        # drop KPIs with namespace not alms
        useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
        for i in useless_kpi_indices:
            df_kpi.drop(columns=f"value-{i}", inplace=True)
        df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
        # create group columns for aggregation
        group_columns = ["alertname", "container"]
        df_kpi_map["container"] = df_kpi_map["container"].fillna("undefined")
        df_kpi_map = df_kpi_map[group_columns]
        # group indices by labels
        df_kpi_indices_to_agg = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(PrometheusAggregator.index_list)
        )
        self.aggregate(
            metric_index,
            df_kpi_indices_to_agg,
            df_kpi.fillna(0),
            ["sum", "count"],
        )

    def aggregate_alerts_for_state_time_series(
        self, metric_index: int, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
    ):
        # ALERTS_FOR_STATE contain only timestamps of alerts
        # drop KPIs with namespace not alms
        useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
        for i in useless_kpi_indices:
            df_kpi.drop(columns=f"value-{i}", inplace=True)
        df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
        # transform df_kpi to boolean values according to timestamps in values
        df_kpi = df_kpi.notnull().astype("int")
        # create group columns for aggregation
        group_columns = ["alertname", "container"]
        df_kpi_map["container"] = df_kpi_map["container"].fillna("undefined")
        df_kpi_map = df_kpi_map[group_columns]
        # group indices by labels
        df_kpi_indices_to_agg = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(PrometheusAggregator.index_list)
        )
        self.aggregate(
            metric_index,
            df_kpi_indices_to_agg,
            df_kpi,
            ["sum", "count"],
        )

    def aggregate_container_time_series(
        self, metric_index: int, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
    ):
        # drop KPIs with namespace not alms
        useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
        for i in useless_kpi_indices:
            df_kpi.drop(columns=f"value-{i}", inplace=True)
        df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
        # create group columns for aggregation
        if "container" in df_kpi_map:
            group_columns = ["container"]
            df_kpi_map = df_kpi_map[group_columns]
        elif "pod" in df_kpi_map:
            df_kpi_map["pod_service"] = df_kpi_map["pod"].str.extract(r"(alms[-a-z]+)-")
            group_columns = ["pod_service"]
            df_kpi_map = df_kpi_map[group_columns]
        else:
            print(f"\tNo labels can be aggregated!")
        # group indices by labels
        df_kpi_indices_to_agg = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(PrometheusAggregator.index_list)
        )
        self.aggregate(
            metric_index,
            df_kpi_indices_to_agg,
            df_kpi,
            [
                "min",
                "max",
                "mean",
                "median",
                "std",
                "count",
            ],
        )

    def aggregate_time_series_in_one(
        self, metric_index: int, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
    ):
        isunique = df_kpi_map.nunique() == 1
        df_kpi_map = df_kpi_map[isunique.index[isunique]]
        group_columns = df_kpi_map.columns.to_list()
        df_kpi_indices_to_agg = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(PrometheusAggregator.index_list)
        )
        self.aggregate(
            metric_index,
            df_kpi_indices_to_agg,
            df_kpi,
            [
                "min",
                "max",
                "mean",
                "median",
                "std",
                "count",
            ],
        )

    def aggregate_kube_time_series(
        self, metric_index: int, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
    ):
        # drop KPIs with namespace not alms
        useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
        for i in useless_kpi_indices:
            df_kpi.drop(columns=f"value-{i}", inplace=True)
        df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
        # drop useless labels
        is_singleton_label = df_kpi_map.nunique() == 1
        df_kpi_map = df_kpi_map.drop(
            columns=is_singleton_label.index[is_singleton_label]
        )
        known_useless_labels = ["instance", "pod", "status", "condition"]
        for label in known_useless_labels:
            if label in df_kpi_map.columns:
                df_kpi_map.drop(columns=label, inplace=True)
        # create group columns for aggregation
        group_columns = df_kpi_map.columns.to_list()
        df_kpi_map = df_kpi_map[group_columns]
        # group indices by labels
        df_kpi_indices_to_agg = (
            df_kpi_map.reset_index()
            .groupby(group_columns)
            .agg(PrometheusAggregator.index_list)
        )
        self.aggregate(
            metric_index,
            df_kpi_indices_to_agg,
            df_kpi,
            [
                "min",
                "max",
                "mean",
                "median",
                "std",
                "count",
            ],
        )

    def adapt_no_agg_time_series(
        self, metric_index: int, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
    ):
        # drop KPIs with namespace not alms
        if "namespace" in df_kpi_map.columns:
            useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
            for i in useless_kpi_indices:
                df_kpi.drop(columns=f"value-{i}", inplace=True)
            df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
        # copy time series
        new_kpi_map_list = []
        if self.referred_agg_path is not None:
            df_referred_kpi_map = Aggregator.read_df_kpi_map(
                metric_index, self.referred_agg_path
            )
        new_kpi_map_index = 1
        for i in df_kpi_map.index:
            if self.referred_agg_path is not None:
                # use referred KPI index
                row = df_kpi_map.loc[i]
                check_same_row = (df_referred_kpi_map == row).all(axis=1)
                matched_row = check_same_row[check_same_row].index
                if not matched_row.empty:
                    new_kpi_map_index = int(matched_row[0])
                else:
                    continue
            new_kpi_map = {
                "index": new_kpi_map_index,
                "kpi": df_kpi_map.loc[i].to_dict(),
            }
            new_kpi_map_list.append(new_kpi_map)
            df_kpi.rename(
                columns={f"value-{i}": f"agg-kpi-{new_kpi_map_index}"}, inplace=True
            )
        if not df_kpi.empty:
            with open(
                os.path.join(
                    self.aggregated_metrics_path, f"metric-{metric_index}-kpi-map.json"
                ),
                "w",
            ) as fp:
                json.dump(new_kpi_map_list, fp)
            df_kpi.to_csv(
                os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
            )

    def aggregate(
        self,
        metric_index: int,
        df_kpi_indices_to_agg: pd.DataFrame,
        df_kpi: pd.DataFrame,
        aggregate_funcs: list,
    ):
        if self.referred_agg_path is not None:
            df_referred_kpi_map = Aggregator.read_df_kpi_map(
                metric_index, self.referred_agg_path
            )
        else:
            new_kpi_map_index = 1
        new_kpi_map_list = []
        df_agg_list = []
        df_kpi_indices_to_agg = df_kpi_indices_to_agg.rename(
            columns={"index": "index_list"}
        ).reset_index()
        for i in df_kpi_indices_to_agg.index:
            if self.referred_agg_path is not None:
                # use referred KPI index
                row = df_kpi_indices_to_agg.loc[i].drop("index_list")
                check_same_row = (df_referred_kpi_map == row).all(axis=1)
                matched_row = check_same_row[check_same_row].index
                if not matched_row.empty:
                    new_kpi_map_index = int(matched_row[0])
                else:
                    continue
            # generate new KPI
            new_kpi_map = {
                "index": new_kpi_map_index,
                "kpi": df_kpi_indices_to_agg.loc[i].drop("index_list").to_dict(),
            }
            new_kpi_map_list.append(new_kpi_map)
            # generate indices to be grouped
            indices_in_df_kpi = [
                int(column.removeprefix("value-"))
                for column in df_kpi.columns.to_list()
            ]
            valid_indices = list(
                set(df_kpi_indices_to_agg.loc[i]["index_list"]) & set(indices_in_df_kpi)
            )
            columns_to_agg = [f"value-{i}" for i in valid_indices]
            df_metric_to_agg = df_kpi[columns_to_agg]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                df_metric_agg = df_metric_to_agg.agg(
                    aggregate_funcs, axis=1
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
        metric_indices = [
            int(filename.removeprefix("metric-").removesuffix("-kpi-map.csv"))
            for filename in os.listdir(self.merged_submetrics_path)
            if filename.endswith("kpi-map.csv")
        ]
        metric_indices.sort()
        for metric_index in metric_indices:
            self.aggregate_one_metric(metric_index)

    def merge_metrics(self):
        df_all_list = []
        metric_indices = [
            filename.removeprefix("metric-").removesuffix("-kpi-map.json")
            for filename in os.listdir(self.aggregated_metrics_path)
            if filename.endswith("kpi-map.json")
        ]
        metric_indices.sort()
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
