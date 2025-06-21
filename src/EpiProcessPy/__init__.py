"""Top-level package for EpiProcess."""

__author__ = """David Weber"""
__email__ = "dsweber2@pm.me"
__version__ = "0.1.0"

from .data import (
    case_rate_subset,
    dv_subset,
    incidence_num_outlier_example,
    jhu_csse_daily_subset,
)
from .EpiProcessPy import EpiArchiveAccessor, EpiSnapAccessor

__all__ = [
    "EpiSnapAccessor",
    "EpiArchiveAccessor",
    "jhu_csse_daily_subset",
    "incidence_num_outlier_example",
    "case_rate_subset",
    "dv_subset",
]
