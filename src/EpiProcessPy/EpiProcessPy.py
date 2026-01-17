"""Main module."""

from collections.abc import Callable, Iterable
from typing import Literal

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_string_dtype
from pandas.core.groupby import DataFrameGroupBy


def _get_level_or_column(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    if name in df.index.names:
        return df.index.get_level_values(name).to_series()
    raise KeyError(f"Missing required field: {name}")


@pd.api.extensions.register_dataframe_accessor("epi_snap")
class EpiSnapAccessor:
    """Extension for DataFrames with snapshot structure."""

    def __init__(self, pandas_obj: pd.DataFrame):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj: pd.DataFrame) -> None:
        geo = _get_level_or_column(obj, "geo_value")
        time = _get_level_or_column(obj, "time_value")
        if not is_string_dtype(geo):
            raise TypeError("geo_value must be string-like.")
        if not is_datetime64_any_dtype(time):
            raise TypeError("time_value must be a datetime64.")

    def as_epi_snap(
        self, as_of: pd.Timestamp | None = None, extra_keys: tuple[str, ...] | list[str] = ()
    ) -> pd.DataFrame:
        """Convert a DataFrame to an epi_snap object."""
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

    def slide(self, func: Callable, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Apply a function to a rolling window.

        Notes:
        - Groups by all indices except time.
        - Applies a rolling sliding window to each group.
        - Applies to all the numeric columns.
        - Shortens the window on the left boundary.
        - Window's right edge is at the current time.
        """
        result = self.group().rolling(window=window_size, min_periods=1).apply(func)
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
        """Create a complete index with all group_keys and time_values."""
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
            # TODO: Kinda hacky, since we must have a time_value to stay `epi_snap` object.
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

    def correlation(self, col1: str, col2: str, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling correlation between two columns within each group."""

        def corr_func(x):
            return x[col1].corr(x[col2])

        result = self.group().rolling(window=window_size, min_periods=1).apply(corr_func)
        return self._fix_grouped_rolling_result(result)

    def detect_outliers(self, column: str, z_thresh: float = 3.0) -> pd.DataFrame:
        """Detect outliers in a specified column using Z-score method within each group."""

        def outlier_func(x):
            mean = x[column].mean()
            std = x[column].std()
            z_scores = (x[column] - mean) / std
            return z_scores.abs() > z_thresh

        result = self.group().apply(outlier_func)
        return result

    def autoplot(self) -> None:
        """TODO"""
        ...

    def print(self) -> None:
        """Print a summary of the epi_snap object."""
        print("EpiSnap DataFrame")
        print(f"Shape: {self._obj.shape}")
        print(f"Index levels: {self._obj.index.names}")
        print(f"Columns: {self._obj.columns.tolist()}")
        if "as_of" in self._obj.attrs:
            print(f"As of: {self._obj.attrs['as_of']}")


@pd.api.extensions.register_dataframe_accessor("epi_arch")
class EpiArchiveAccessor:
    """Extension for DataFrames with archive structure."""

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
        # Reset and drop index if not named
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

    def compress(self) -> None:
        """TODO."""
        ...

    def as_of(self, version: pd.Timestamp) -> pd.DataFrame:
        """Subset the dataframe as of a specific version."""
        return self._obj.loc[self._obj.index.get_level_values("version") <= version]

    def as_of_current(self) -> pd.DataFrame:
        """Subset the dataframe to the current time value."""
        return self._obj.loc[self._obj.index.get_level_values("time_value") == pd.Timestamp.now()]

    def partition_by_versions(self, versions: list[pd.Timestamp]) -> Iterable[pd.DataFrame]:
        """Get an iterable of snapshots.

        Called "slide" in the R version, but trying out a different name here.
        """
        return (self.as_of(v) for v in versions)

    def merge_archive(self, other: pd.DataFrame) -> pd.DataFrame:
        """Combine two epi_arch dataframes.

        This is a side-by-side merge that uses LOCF to fill in missing values.
        """
        # Validate that `other` is an epi_arch object.
        EpiArchiveAccessor._validate(other)
        # Create a common index of keys, time_values, and versions
        combined_index = self._obj.index.union(other.index)
        # Reindex both dataframes to the combined index
        df1_reindexed = self._obj.reindex(combined_index)
        df2_reindexed = other.reindex(combined_index)
        # Use LOCF to fill in missing values in every column
        df1_filled = df1_reindexed.groupby(level=[n for n in combined_index.names if n != "time_value"]).ffill()
        df2_filled = df2_reindexed.groupby(level=[n for n in combined_index.names if n != "time_value"]).ffill()
        # If there are any value column names shared, add suffixes to distinguish them
        common_cols = df1_filled.columns.intersection(df2_filled.columns)
        if len(common_cols) > 0:
            df1_filled = df1_filled.add_suffix("_1")
            df2_filled = df2_filled.add_suffix("_2")
        # Combine the two dataframes
        merged_df = pd.concat([df1_filled, df2_filled], axis=1)
        return merged_df
