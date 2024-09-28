"""Main module."""

import pandas as pd


@pd.api.extensions.register_dataframe_accessor("epi_snap")
class EpiSnapAccessor:
    def __init__(self, pandas_obj: pd.DataFrame):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not set(obj.columns) >= {"geo_value", "time_value"}:
            raise AttributeError("Must have 'geo_value' and 'time_value'.")

    def as_epi_snap(self, as_of: pd.Timestamp | None = None, extra_keys: tuple | list = ()) -> pd.DataFrame:
        obj = self._obj
        # default set the as_of to the max value
        if as_of is None:
            as_of = self._obj.time_value.max()
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

    def fill(self, method: str = "ffill") -> pd.DataFrame:
        """Fill in missing values."""
        return self._obj.groupby().fillna(method=method)

    def sum_group(self, key: str): ...


@pd.api.extensions.register_dataframe_accessor("epi_arch")
class EpiArchiveAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not set(obj.columns) >= {"geo_value", "time_value", "version"}:
            raise AttributeError("Must have 'geo_value', 'time_value', and 'version'.")

    def as_epi_arch(
            self,
            extra_keys: union[tuple, list] = ()
    ):
        obj = self._obj
        # default set the as_of to the max value
        if as_of is None:
            as_of = self._obj.time_value.max()
        elif not isinstance(as_of, pd.Timestamp):
            pass
        key_names = ["version", "geo_value",  *extra_keys, "time_value"]
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop = drop_index).set_index(key_names)
        obj.attrs["as_of"] = as_of

        return obj

    def group(self):
        """group by all indices except time."""
        names_without_time = list(self._obj.index.names)
        names_without_time.remove('time_value')
        return self._obj.groupby(names_without_time)

    def slide(self):
        ...
