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

    def _rolling(self, window_size: int | pd.Timedelta):
        """Build a grouped rolling object that handles both int- and Timedelta-windows.

        For Timedelta windows, pandas requires the time column to be a regular
        column (not just an index level) and passed via `on=`. For int windows,
        the index-level form is fine and roughly twice as fast.
        """
        if isinstance(window_size, pd.Timedelta):
            df = self._obj.reset_index("time_value")
            return df.groupby(self.keys()).rolling(window=window_size, on="time_value", min_periods=1)
        return self.group().rolling(window=window_size, min_periods=1)

    def slide(self, func: Callable, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Apply a function to a rolling window.

        Notes:
        - Groups by all indices except time.
        - Applies a rolling sliding window to each group.
        - Applies to all the numeric columns.
        - Shortens the window on the left boundary.
        - Window's right edge is at the current time.
        """
        return self._fix_grouped_rolling_result(self._rolling(window_size).apply(func), window_size)

    def slide_mean(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling mean within each group."""
        return self._fix_grouped_rolling_result(self._rolling(window_size).mean(), window_size)

    def slide_sum(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling sum within each group."""
        return self._fix_grouped_rolling_result(self._rolling(window_size).sum(), window_size)

    def slide_std(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling standard deviation within each group."""
        return self._fix_grouped_rolling_result(self._rolling(window_size).std(), window_size)

    def slide_var(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling variance within each group."""
        return self._fix_grouped_rolling_result(self._rolling(window_size).var(), window_size)

    def slide_min(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling minimum within each group."""
        return self._fix_grouped_rolling_result(self._rolling(window_size).min(), window_size)

    def slide_max(self, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling maximum within each group."""
        return self._fix_grouped_rolling_result(self._rolling(window_size).max(), window_size)

    def _fix_grouped_rolling_result(
        self, result: pd.DataFrame, window_size: int | pd.Timedelta
    ) -> pd.DataFrame:
        """Restore the original (geo_value, ..., time_value) MultiIndex."""
        # Grouped rolling duplicates the group key levels in the output; drop those.
        n_dupes = len(result.index.names) - len(self._obj.index.names)
        if isinstance(window_size, pd.Timedelta):
            # Time-based path: time_value sits in a column, not the index.
            n_dupes += 1
        if n_dupes > 0:
            result = result.droplevel(list(range(n_dupes)))
        if isinstance(window_size, pd.Timedelta):
            result = result.set_index("time_value", append=True)
        return result

    def complete(self) -> pd.DataFrame:
        """Create a complete index with all group_keys and time_values."""
        time_vals = self._obj.index.get_level_values("time_value")
        unique_time_values = pd.date_range(time_vals.min(), time_vals.max(), freq="D")
        key_names = self.keys()  # all non-time index levels
        level_values = [self._obj.index.get_level_values(k).unique() for k in key_names]
        new_index = pd.MultiIndex.from_product(
            [*level_values, unique_time_values],
            names=[*key_names, "time_value"],
        )
        return self._obj.reindex(new_index)

    def fill(self, value: str, method: Literal["ffill", "bfill"] = "ffill") -> pd.DataFrame:
        """Fill in missing values."""
        grouped = self.group()[value]
        filled = grouped.ffill() if method == "ffill" else grouped.bfill()
        return self._obj.assign(value=filled)

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
        return self._fix_grouped_rolling_result(result, window_size)

    def correlation(self, col1: str, col2: str, window_size: int | pd.Timedelta) -> pd.DataFrame:
        """Calculate rolling correlation between two columns within each group."""

        def corr_func(x):
            return x[col1].corr(x[col2])

        result = self.group().rolling(window=window_size, min_periods=1).apply(corr_func)
        return self._fix_grouped_rolling_result(result, window_size)

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

    def as_epi_arch(
        self,
        extra_keys: tuple[str, ...] | list[str] = (),
        versions_end: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        obj = self._obj
        key_names = ["version", "geo_value", *extra_keys, "time_value"]
        # Reset and drop index if not named
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop=drop_index).set_index(key_names)
        obj.attrs["versions_end"] = versions_end or obj.index.get_level_values("version").max()
        return obj

    def group(self) -> DataFrameGroupBy:
        """group by all indices except time."""
        names_without_time = list(self._obj.index.names)
        names_without_time.remove("time_value")
        return self._obj.groupby(names_without_time)

    def compress(self, abs_tol: float = 0.0, init_nas_are_locf: bool = False) -> pd.DataFrame:
        """Remove rows that are LOCF-redundant given previous versions.

        Port of R `apply_compactify` (epiprocess/R/archive.R). For each
        `(geo_value, ..., time_value)` series, an update row is removed if every
        value column is equal (within `abs_tol` for numeric columns; exactly for
        others, treating NA == NA) to the immediately preceding version's row.
        First observation per series is always kept.

        Parameters
        ----------
        abs_tol : float
            Absolute tolerance for numeric value-column equality. Default 0.
        init_nas_are_locf : bool
            If True, the first row of a series is treated as LOCF when all its
            value columns are NA. Default False (matches R default).
        """
        index_cols = list(self._obj.index.names)
        ekt_cols = [c for c in index_cols if c != "version"]
        value_cols = [c for c in self._obj.columns if c not in index_cols]

        df = self._obj.reset_index().sort_values(ekt_cols + ["version"]).reset_index(drop=True)

        def col_is_locf(col: pd.Series, is_key: bool) -> pd.Series:
            lag = col.shift(1)
            if not is_key and pd.api.types.is_numeric_dtype(col):
                both_present = col.notna() & lag.notna()
                both_na = col.isna() & lag.isna()
                return (both_present & ((col - lag).abs() <= abs_tol)) | both_na
            # Exact equality, NA-equal.
            return (col == lag) | (col.isna() & lag.isna())

        ekt_is_locf = pd.Series(True, index=df.index)
        for c in ekt_cols:
            ekt_is_locf &= col_is_locf(df[c], is_key=True)

        value_is_locf = pd.Series(True, index=df.index)
        for c in value_cols:
            value_is_locf &= col_is_locf(df[c], is_key=False)

        if init_nas_are_locf and value_cols:
            all_na = df[value_cols].isna().all(axis=1)
            is_locf = pd.Series(
                [(e and v) if e else m for e, v, m in zip(ekt_is_locf, value_is_locf, all_na)],
                index=df.index,
            )
        else:
            is_locf = ekt_is_locf & value_is_locf

        result = df.loc[~is_locf].set_index(index_cols).sort_index()
        result.attrs = dict(self._obj.attrs)
        return result

    def truncate_versions_after(self, max_version: pd.Timestamp) -> pd.DataFrame:
        """Keep only rows with `version <= max_version`.

        Port of R `epix_truncate_versions_after`. Behaviorally equivalent to
        `as_of(max_version)` here (we don't carry the `clobberable_versions_start`
        field separately); kept for naming parity with R and to update
        `versions_end` in attrs.
        """
        versions = self._obj.index.get_level_values("version")
        if max_version > self._obj.attrs.get("versions_end", versions.max()):
            raise ValueError("`max_version` must be at most the archive's `versions_end`.")
        result = self._obj.loc[versions <= max_version].copy()
        result.attrs = dict(self._obj.attrs)
        result.attrs["versions_end"] = max_version
        return result

    def fill_through_versions(
        self,
        fill_versions_end: pd.Timestamp,
        how: Literal["na", "locf"] = "na",
    ) -> pd.DataFrame:
        """Extend the archive's `versions_end` forward.

        Port of R `epix_fill_through_version`. If `fill_versions_end` is beyond
        the current `versions_end`:

        - `"na"`: append one synthetic version (current `versions_end` + 1 day)
          containing every `(geo_value, ..., time_value)` ever observed, with NA
          for all value columns.
        - `"locf"`: no data is added; LOCF is implicit in `as_of` queries.

        Either way, `versions_end` in attrs is bumped to `fill_versions_end`.
        """
        current_end = self._obj.attrs.get("versions_end", self._obj.index.get_level_values("version").max())
        if fill_versions_end <= current_end:
            return self._obj

        if how == "locf":
            result = self._obj.copy()
        elif how == "na":
            index_cols = list(self._obj.index.names)
            ekt_cols = [c for c in index_cols if c != "version"]
            value_cols = [c for c in self._obj.columns if c not in index_cols]
            ekt_unique = self._obj.reset_index()[ekt_cols].drop_duplicates()
            # next_after for daily cadence; TODO: respect cadence once implemented.
            next_version = current_end + pd.Timedelta(days=1)
            if next_version > fill_versions_end:
                raise ValueError(
                    f"Cannot fill: next version {next_version} exceeds fill_versions_end {fill_versions_end}."
                )
            ekt_unique["version"] = next_version
            for c in value_cols:
                # Use the dtype's own missing sentinel (NaN for float, NaT for datetime, ...).
                na_val = pd.array([None], dtype=self._obj[c].dtype)[0]
                ekt_unique[c] = pd.Series([na_val] * len(ekt_unique), dtype=self._obj[c].dtype)
            new_rows = ekt_unique.set_index(index_cols)
            result = pd.concat([self._obj, new_rows]).sort_index()
        else:
            raise ValueError(f"Unknown how: {how}")

        result.attrs = dict(self._obj.attrs)
        result.attrs["versions_end"] = fill_versions_end
        return result

    def as_of(self, version: pd.Timestamp) -> pd.DataFrame:
        """Subset the dataframe as of a specific version."""
        return self._obj.loc[self._obj.index.get_level_values("version") <= version]

    def as_of_current(self) -> pd.DataFrame:
        """Subset the dataframe to the current time value."""
        return self.as_of(self._obj.index.get_level_values("version").max())

    def partition_by_versions(self, versions: list[pd.Timestamp]) -> Iterable[pd.DataFrame]:
        """Get an iterable of snapshots.

        Called "slide" in the R version, but trying out a different name here.
        """
        return (self.as_of(v) for v in versions)

    def _snapshot_at(self, version: pd.Timestamp) -> pd.DataFrame:
        """True snapshot: latest update per (geo, ..., time) with version <= version.

        Distinct from `as_of`, which returns the filtered archive (still with the
        version axis). This collapses to one row per non-version key.
        """
        sub = self._obj.loc[self._obj.index.get_level_values("version") <= version]
        if sub.empty:
            return sub.copy()
        ekt_cols = [c for c in sub.index.names if c != "version"]
        return (
            sub.reset_index()
            .sort_values("version")
            .drop_duplicates(ekt_cols, keep="last")
            .set_index(ekt_cols)
            .drop(columns="version")
            .sort_index()
        )

    def epix_slide(
        self,
        func: Callable[[pd.DataFrame], object],
        before: pd.Timedelta = pd.Timedelta.max,
        versions: list[pd.Timestamp] | None = None,
        new_col_name: str = "slide_value",
    ) -> pd.DataFrame:
        """For each version v, apply `func` to the (time-windowed) snapshot at v.

        Port of R `epix_slide`. For each version v:
          1. Take the snapshot as-of v (latest update per non-version key with
             version <= v).
          2. Filter to rows where `time_value >= v - before` (defaults to all).
          3. Group by all non-time keys and apply `func` to each group.

        Returns a long-form DataFrame with columns `[*keys, version, <new_col_name>]`.
        """
        index_cols = list(self._obj.index.names)
        ekt_cols = [c for c in index_cols if c != "version"]
        group_cols = [c for c in ekt_cols if c != "time_value"]

        if versions is None:
            versions = sorted(self._obj.index.get_level_values("version").unique())

        rows = []
        for v in versions:
            snap = self._snapshot_at(v)
            if snap.empty:
                continue
            time_vals = snap.index.get_level_values("time_value")
            try:
                snap = snap.loc[time_vals >= v - before]
            except OverflowError:
                pass  # `before = Timedelta.max` triggers overflow; means "no filter."
            if snap.empty:
                continue
            for group_vals, grp in snap.reset_index().groupby(group_cols):
                row = dict(zip(group_cols, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
                row["version"] = v
                row[new_col_name] = func(grp)
                rows.append(row)
        return pd.DataFrame(rows, columns=[*group_cols, "version", new_col_name])

    @staticmethod
    def _locf_to_index(orig: pd.DataFrame, target_index: pd.MultiIndex) -> pd.DataFrame:
        """For each row in `target_index`, return the most recent observation
        from `orig` whose version is <= that row's version (matching on the
        non-version key columns).

        Unlike pandas `ffill`, `merge_asof` picks up the exact prior row — so
        an explicit NA observation propagates forward as NA until a new update
        replaces it. This matches R epiprocess's per-(geo, time) LOCF semantics.
        """
        index_cols = list(target_index.names)
        ekt_cols = [c for c in index_cols if c != "version"]
        if orig.empty:
            return pd.DataFrame(index=target_index, columns=orig.columns)
        target = target_index.to_frame(index=False).sort_values("version")
        right = orig.reset_index().sort_values("version")
        joined = pd.merge_asof(target, right, on="version", by=ekt_cols, direction="backward")
        return joined.set_index(index_cols).sort_index()

    def merge_archive(self, other: pd.DataFrame, sync: Literal["locf", "na", "truncate"] = "locf") -> pd.DataFrame:
        """Combine two epi_arch dataframes.

        This is a side-by-side merge that uses LOCF to fill in missing values.

        Parameters
        ----------
        other : pd.DataFrame
            Another epi_archive DataFrame to merge with.
        sync : Literal["locf", "na", "truncate"]
            How to handle a `versions_end` mismatch (when one archive saw later
            updates than the other):
            - "locf" (default): LOCF both sides up to `max(versions_end)`.
            - "na": Like "locf", but the lagging side gets a synthetic NA-update
              version inserted at its old `versions_end + 1 day`, so its values
              after that point are explicitly NA rather than LOCF'd.
            - "truncate": Cap the merged archive at `min(versions_end)`.

        Returns
        -------
        pd.DataFrame
            Merged epi_archive with columns from both inputs (suffixed _x and _y
            if there are overlapping column names).
        """
        other = other.epi_arch.as_epi_arch()
        x_end = self._obj.attrs.get("versions_end", self._obj.index.get_level_values("version").max())
        y_end = other.attrs.get("versions_end", other.index.get_level_values("version").max())

        x_obj, y_obj = self._obj, other
        if sync == "na":
            # Insert synthetic NA-update on the lagging side so values past its
            # known horizon read as explicit NA rather than LOCF'd.
            merged_end = max(x_end, y_end)
            if x_end < merged_end:
                x_obj = x_obj.epi_arch.fill_through_versions(merged_end, how="na")
            if y_end < merged_end:
                y_obj = y_obj.epi_arch.fill_through_versions(merged_end, how="na")

        combined_index = x_obj.index.union(y_obj.index)
        if sync == "truncate":
            cap = min(x_end, y_end)
            combined_index = combined_index[combined_index.get_level_values("version") <= cap]

        if sync in ("locf", "na", "truncate"):
            df1_filled = self._locf_to_index(x_obj, combined_index)
            df2_filled = self._locf_to_index(y_obj, combined_index)
        else:
            raise ValueError(f"Unknown sync option: {sync}")

        common_cols = df1_filled.columns.intersection(df2_filled.columns)
        if len(common_cols) > 0:
            df1_filled = df1_filled.add_suffix("_x")
            df2_filled = df2_filled.add_suffix("_y")
        result = pd.concat([df1_filled, df2_filled], axis=1)

        x_end = self._obj.attrs.get("versions_end", self._obj.index.get_level_values("version").max())
        y_end = other.attrs.get("versions_end", other.index.get_level_values("version").max())
        result.attrs["versions_end"] = min(x_end, y_end) if sync == "truncate" else max(x_end, y_end)
        return result

    def compare_archive(
        self,
        other: pd.DataFrame,
        value_col: str | None = None,
        sync: Literal["locf", "na", "truncate"] = "locf",
        by: Literal["geo", "version", "both"] = "both",
    ) -> pd.DataFrame:
        """Compare two archives and compute difference statistics.

        Parameters
        ----------
        other : pd.DataFrame
            Another epi_archive DataFrame to compare against.
        value_col : str, optional
            The column to compare. If None, will auto-detect from common columns.
        sync : Literal["locf", "na", "truncate"]
            How to handle version misalignment (passed to merge_archive).
        by : Literal["geo", "version", "both"]
            Aggregation level:
            - "geo": per (version, geo_value) - most granular
            - "version": per version only (aggregate across geos)
            - "both": returns both levels (geo-level with _all_ marker for version aggregate)

        Returns
        -------
        pd.DataFrame
            Summary statistics: n_obs, diff_min, diff_max, diff_mean, diff_median,
            diff_abs_mean, diff_abs_max.
        """
        merged = self.merge_archive(other, sync=sync)

        # Auto-detect value column from _x/_y suffixed columns
        if value_col is None:
            x_cols = [c[:-2] for c in merged.columns if c.endswith("_x")]
            y_cols = [c[:-2] for c in merged.columns if c.endswith("_y")]
            common = set(x_cols).intersection(y_cols)
            if not common:
                raise ValueError("No common value columns found to compare")
            value_col = list(common)[0]

        col_x, col_y = f"{value_col}_x", f"{value_col}_y"
        if col_x not in merged.columns or col_y not in merged.columns:
            raise ValueError(f"Column {value_col} not found in both archives")

        merged_reset = merged.reset_index()
        merged_reset["diff"] = merged_reset[col_x] - merged_reset[col_y]
        merged_reset["abs_diff"] = merged_reset["diff"].abs()

        agg_funcs = {
            "n_obs": ("diff", "count"),
            "diff_min": ("diff", "min"),
            "diff_max": ("diff", "max"),
            "diff_mean": ("diff", "mean"),
            "diff_median": ("diff", "median"),
            "diff_abs_mean": ("abs_diff", "mean"),
            "diff_abs_max": ("abs_diff", "max"),
        }

        if by == "geo":
            return merged_reset.groupby(["version", "geo_value"]).agg(**agg_funcs).reset_index()
        elif by == "version":
            return merged_reset.groupby(["version"]).agg(**agg_funcs).reset_index()
        else:  # "both"
            geo_stats = merged_reset.groupby(["version", "geo_value"]).agg(**agg_funcs).reset_index()
            version_stats = merged_reset.groupby(["version"]).agg(**agg_funcs).reset_index()
            version_stats["geo_value"] = "_all_"
            return pd.concat([geo_stats, version_stats], ignore_index=True)
