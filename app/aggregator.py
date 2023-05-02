from abc import ABC, abstractmethod
import json
import os

import pandas as pd


class Aggregator(ABC):
    @abstractmethod
    def aggregate_all_metrics(self):
        """Aggregate all available metrics to reduce dimensionality."""

    @abstractmethod
    def merge_metrics(self):
        """Merge all metrics into one dataframe."""
        pass

    @staticmethod
    def read_df_kpi_map(metric_index: int, kpi_map_folder_path: str) -> pd.DataFrame:
        kpi_map_path_json = os.path.join(
            kpi_map_folder_path, f"metric-{metric_index}-kpi-map.json"
        )
        kpi_map_path_csv = os.path.join(
            kpi_map_folder_path, f"metric-{metric_index}-kpi-map.csv"
        )
        if os.path.exists(kpi_map_path_json):
            with open(kpi_map_path_json) as fp:
                kpi_map_list = json.load(fp)
            kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
            indices = [kpi_map["index"] for kpi_map in kpi_map_list]
            df_kpi_map = pd.DataFrame(kpi_maps, index=indices).sort_index(axis=1)
            return df_kpi_map
        elif os.path.exists(kpi_map_path_csv):
            return (
                pd.read_csv(kpi_map_path_csv).set_index("Unnamed: 0").sort_index(axis=1)
            )
