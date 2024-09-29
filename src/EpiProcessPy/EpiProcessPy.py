"""Main module."""

from typing import Literal

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


@pd.api.extensions.register_dataframe_accessor("epi_snap")
class EpiSnapAccessor:
    def __init__(self, pandas_obj: pd.DataFrame):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        must_contain = {"geo_value", "time_value"}
        if not set(obj.index.names) >= must_contain and not set(obj.columns) >= must_contain:
            raise AttributeError("Must have 'geo_value' and 'time_value'.")

    def as_epi_snap(self, as_of: pd.Timestamp | None = None, extra_keys: tuple | list = ()) -> pd.DataFrame:
        obj = self._obj
        as_of = as_of or self._obj.time_value.max()
        key_names = ["geo_value", *extra_keys, "time_value"]
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop=drop_index).set_index(key_names).sort_index()
        obj.attrs["as_of"] = as_of
        return obj

    def slide(self, f, window_size: int | pd.Timedelta) -> pd.DataFrame:
        return self._obj.rolling(window=window_size, min_periods=1, axis=0).apply(f)
        # TODO: Add slide_mean, slide_sum, slide_std, slide_var, slide_min,
        # slide_max (not using apply is faster IIRC).

    def complete(self) -> pd.DataFrame:
        """Fill in missing values.

        This will fill in missing values for all group_keys and time_values.
        """
        min_t = min(self._obj.index.get_level_values(1))
        max_t = max(self._obj.index.get_level_values(1))
        new_index = pd.MultiIndex.from_product(
            self._obj.index.get_level_values(0).unique(),
            pd.date_range(min_t, max_t, freq="D"),
        )
        return self._obj.reindex(new_index)

    def fill(self, value: str, method: Literal["ffill", "bfill"] | None = "ffill") -> pd.DataFrame:
        """Fill in missing values."""
        return self._obj.assign(value=self.group()[value].fillna(method=method))

    def sum_group(self, key: str):
        """Sum over a group index."""
        sum_df = self._obj.groupby(key).sum()
        if "geo_value" not in sum_df.columns:
            sum_df["geo_value"] = "total"
        if "time_value" not in sum_df.columns:
            sum_df["time_value"] = 0
        return sum_df.reset_index().set_index(["geo_value", "time_value"])

    def keys(self, exclude="time_value") -> list[str]:
        return [x for x in self._obj.index.names if x not in exclude]

    def group(self, exclude="time_value") -> DataFrameGroupBy:
        """group by all indices except time."""
        return self._obj.groupby(self.keys(exclude))


@pd.api.extensions.register_dataframe_accessor("epi_arch")
class EpiArchiveAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        must_contain = {"geo_value", "time_value", "version"}
        if not set(obj.index.names) >= must_contain and not set(obj.columns) >= must_contain:
            raise AttributeError("Must have 'geo_value', 'time_value', and 'version'.")

    def as_epi_arch(self, extra_keys: tuple | list = ()):
        obj = self._obj
        key_names = ["version", "geo_value", *extra_keys, "time_value"]
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop=drop_index).set_index(key_names)
        return obj

    def group(self):
        """group by all indices except time."""
        names_without_time = list(self._obj.index.names)
        names_without_time.remove("time_value")
        return self._obj.groupby(names_without_time)

    def slide(self): ...
