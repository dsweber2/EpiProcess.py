"""Main module."""

import pandas as pd

@pd.api.extensions.register_dataframe_accessor("epi_snap")
class EpiSnapAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not set(obj.columns) >= {"geo_value", "time_value"}:
            raise AttributeError("Must have 'geo_value' and 'time_value'.")

    def as_epi_snap(
            self,
            as_of=None,
            extra_keys = []
    ):
        obj = self._obj
        if as_of is None:
            as_of = self._obj.time_value.max()
        elif not isinstance(as_of, pd.Timestamp):
            pass
        key_names = ["geo_value", "time_value", *extra_keys]
        if self._obj.index.names != key_names:
            if obj.index.names == [None]:
                drop_index = True
            else:
                drop_index = False
            obj = obj.reset_index(drop = drop_index).set_index(key_names)
        obj.attrs["as_of"] = as_of
        return obj


    def slide(self):
        ...

@pd.api.extensions.register_dataframe_accessor("epi_arch")
class EpiArchiveAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        if not set(obj.columns) >= {"geo_value", "time_value", "version"}:
            raise AttributeError("Must have 'geo_value', 'time_value', and 'version'.")

    def slide(self):
        ...
