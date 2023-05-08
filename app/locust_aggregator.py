import os
from scipy.stats import zscore
import numpy as np
import pandas as pd
from app.aggregator import Aggregator


class LocustAggregator(Aggregator):
    def __init__(
        self,
        metrics_parent_path: str,
        metrics_folder: str,
    ):
        self.metrics_parent_path = metrics_parent_path
        self.metrics_path = os.path.join(
            metrics_parent_path, metrics_folder, "alemira_stats_history.csv"
        )
        self.aggregated_metrics_path = os.path.join(
            metrics_parent_path, metrics_folder, "locust_aggregated_stats.csv"
        )

    def aggregate_all_metrics(self):
        df_stats = pd.read_csv(self.metrics_path)
        df_stats = df_stats[df_stats["Name"] == "Aggregated"].drop(
            columns=[
                "Type",
                "Name",
                "Total Request Count",
                "Total Failure Count",
                "Total Min Response Time",
                "Total Max Response Time",
            ]
        )
        df_stats["Timestamp"] = pd.to_datetime(
            df_stats["Timestamp"], unit="s"
        ).dt.round("min")
        df_stats = df_stats.groupby("Timestamp").agg(
            {
                "User Count": "max",
                "Requests/s": "mean",
                "Failures/s": "mean",
                "50%": "max",
                "66%": "max",
                "75%": "max",
                "80%": "max",
                "90%": "max",
                "95%": "max",
                "98%": "max",
                "99%": "max",
                "99.9%": "max",
                "99.99%": "max",
                "100%": "max",
                "Total Median Response Time": "median",
                "Total Average Response Time": "mean",
                "Total Average Content Size": "mean",
            }
        )
        df_stats.index.rename("timestamp", inplace=True)
        df_stats = df_stats.add_prefix("lm-").reset_index()
        df_stats.to_csv(self.aggregated_metrics_path, index=False)

    @staticmethod
    def merge_normal_metrics(metrics_parent_path, folders):
        df_list = []
        for folder in folders:
            if folder.startswith("day-"):
                agg_stats_path = os.path.join(
                    metrics_parent_path, folder, "locust_aggregated_stats.csv"
                )
                df_stats = pd.read_csv(agg_stats_path)
                df_list.append(df_stats)
        complete_df = pd.concat(df_list)
        # remove outliers
        complete_df = complete_df[
            (np.abs(zscore(complete_df[["lm-Failures/s", "lm-95%"]])) < 3).all(axis=1)
        ]
        complete_df.to_csv(
            os.path.join(metrics_parent_path, "locust_normal_stats.csv"),
            index=False,
        )
