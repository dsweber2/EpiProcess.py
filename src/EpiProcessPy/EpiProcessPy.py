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

