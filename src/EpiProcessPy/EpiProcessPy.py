"""Main module."""

from typing import Any, Literal

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


@pd.api.extensions.register_dataframe_accessor("epi_snap")
class EpiSnapAccessor:
    def __init__(self, pandas_obj: pd.DataFrame):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj: pd.DataFrame) -> None:
        must_contain = {"geo_value", "time_value"}
        if not set(obj.index.names) >= must_contain and not set(obj.columns) >= must_contain:
            raise AttributeError("Must have 'geo_value' and 'time_value'.")

    def as_epi_snap(
        self, as_of: pd.Timestamp | None = None, extra_keys: tuple[str, ...] | list[str] = ()
    ) -> pd.DataFrame:
        obj = self._obj
        # Get max time_value from index or column
        if "time_value" in obj.index.names:
            as_of = as_of or obj.index.get_level_values("time_value").max()
        else:
            as_of = as_of or obj.time_value.max()

        key_names = ["geo_value", *extra_keys, "time_value"]
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop=drop_index).set_index(key_names).sort_index()
        obj.attrs["as_of"] = as_of
        return obj

    def slide(self, f: Any, window_size: int | pd.Timedelta, columns: list[str] | None = None) -> pd.DataFrame:
        """Apply a function to a rolling window.

        Notes:
        - Groups by all indices except time.
        - Applies a rolling sliding window to each group.
        - Applies to all the numeric columns.
        - Shortens the window on the left boundary.
        - Window's right edge is at the current time.
        """
        result = self.group().rolling(window=window_size, min_periods=1).apply(f)
        return self._fix_grouped_rolling_result(result)

    def slide_mean(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling mean within each group."""
        result = self.group().rolling(window=window_size, min_periods=1).mean()
        return self._fix_grouped_rolling_result(result)

    def slide_sum(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling sum within each group."""
        result = self.group().rolling(window=window_size, min_periods=1).sum()
        return self._fix_grouped_rolling_result(result)

    def slide_std(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling standard deviation within each group."""
        result = self.group().rolling(window=window_size, min_periods=1).std()
        return self._fix_grouped_rolling_result(result)

    def slide_var(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling variance within each group."""
        result = self.group().rolling(window=window_size, min_periods=1).var()
        return self._fix_grouped_rolling_result(result)

    def slide_min(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling minimum within each group."""
        result = self.group().rolling(window=window_size, min_periods=1).min()
        return self._fix_grouped_rolling_result(result)

    def slide_max(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling maximum within each group."""
        result = self.group().rolling(window=window_size, min_periods=1).max()
        return self._fix_grouped_rolling_result(result)

    def _fix_grouped_rolling_result(self, result: pd.DataFrame) -> pd.DataFrame:
        """Fix the index structure after grouped rolling operations."""
        # Grouped rolling operations create extra index levels, we need to remove them
        # to match the original DataFrame structure
        if len(result.index.names) > len(self._obj.index.names):
            # Drop the duplicated group levels
            levels_to_drop = len(result.index.names) - len(self._obj.index.names)
            result = result.droplevel(list(range(levels_to_drop)))
        return result

    def complete(self) -> pd.DataFrame:
        """Fill in missing values.

        This will fill in missing values for all group_keys and time_values.
        """
        min_t = min(self._obj.index.get_level_values("time_value"))
        max_t = max(self._obj.index.get_level_values("time_value"))
        unique_time_values = pd.date_range(min_t, max_t, freq="D")
        unique_geo_values = self._obj.index.get_level_values("geo_value").unique()
        new_index = pd.MultiIndex.from_product(
            [unique_geo_values, unique_time_values],
            names=["geo_value", "time_value"],
        )
        return self._obj.reindex(new_index)

    def fill(self, value: str, method: Literal["ffill", "bfill"] | None = "ffill") -> pd.DataFrame:
        """Fill in missing values."""
        return self._obj.assign(value=self.group()[value].fillna(method=method))

    def sum_groups(self, key: str) -> pd.DataFrame:
        """Sum over a group index."""
        sum_df = self._obj.groupby(key).sum()
        sum_df = sum_df.reset_index()
        if "geo_value" not in sum_df.columns:
            sum_df["geo_value"] = "total"
        if "time_value" not in sum_df.columns:
            # TODO: Kinda hacky, since we don't have a time_value column.
            sum_df["time_value"] = 0
        return sum_df.set_index(["geo_value", "time_value"])

    def keys(self, exclude: list[str] | None = None) -> list[str]:
        if exclude is None:
            exclude = ["time_value"]
        return [x for x in self._obj.index.names if x not in exclude]

    def group(self, exclude: list[str] | None = None) -> DataFrameGroupBy:
        """A convenience wrapper for grouping by keys."""
        if exclude is None:
            exclude = ["time_value"]
        return self._obj.groupby(self.keys(exclude))

    def growth_rate(self, columns: list[str] | None = None, window_size: int | pd.Timedelta = 1) -> pd.DataFrame:
        """Calculate growth rate within each group.

        Growth rate is calculated as the percentage change from the previous period
        after applying a rolling mean to smooth the data.
        """
        if columns is None:
            columns = self._obj.columns.tolist()
        result = self.group()[columns].rolling(window=window_size, min_periods=1).mean().pct_change(periods=1)
        return self._fix_grouped_rolling_result(result)


@pd.api.extensions.register_dataframe_accessor("epi_arch")
class EpiArchiveAccessor:
    def __init__(self, pandas_obj: pd.DataFrame):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj: pd.DataFrame) -> None:
        must_contain = {"geo_value", "time_value", "version"}
        if not set(obj.index.names) >= must_contain and not set(obj.columns) >= must_contain:
            raise AttributeError("Must have 'geo_value', 'time_value', and 'version'.")

    def as_epi_arch(self, extra_keys: tuple[str, ...] | list[str] = ()) -> pd.DataFrame:
        obj = self._obj
        key_names = ["version", "geo_value", *extra_keys, "time_value"]
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop=drop_index).set_index(key_names)
        return obj

    def group(self) -> DataFrameGroupBy:
        """group by all indices except time."""
        names_without_time = list(self._obj.index.names)
        names_without_time.remove("time_value")
        return self._obj.groupby(names_without_time)

    def slide(self) -> None: ...
