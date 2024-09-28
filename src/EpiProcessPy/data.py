from functools import reduce
from pathlib import Path

from epidatpy import EpiDataContext, EpiRange
import pandas as pd


if not Path("dv_subset.parquet").exists():
    dv_subset = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="doctor-visits",
            signals="smoothed_adj_cli",
            geo_type="state",
            time_type="day",
            geo_values="ca,fl,ny,tx",
            time_values=EpiRange(20200601, 20211201),
            issues=EpiRange(20200601, 20211201),
        )
        .df()
        .rename(columns={"issue": "version", "value": "percent_cli"})
        .loc[:, ["geo_value", "time_value", "version", "percent_cli"]]
    )
    # TODO: as epi_archive
    dv_subset.to_parquet("data/dv_subset.parquet")
else:
    dv_subset = pd.read_parquet("data/dv_subset.parquet")


if not Path("case_rate_subset.parquet").exists():
    case_rate_subset = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="jhu-csse",
            signals="confirmed_7dav_incidence_prop",
            geo_type="state",
            time_type="day",
            geo_values="ca,fl,ny,tx",
            time_values=EpiRange(20200601, 20211201),
            issues=EpiRange(20200601, 20211201),
        )
        .df()
        .rename(columns={"issue": "version", "value": "case_rate_7d_av"})
        .loc[:, ["geo_value", "time_value", "version", "case_rate_7d_av"]]
    )
    # TODO: as epi_archive
    case_rate_subset.to_parquet("data/case_rate_subset.parquet")
else:
    case_rate_subset = pd.read_parquet("data/case_rate_subset.parquet")

# TODO: epix_merge the two above? https://github.com/cmu-delphi/epiprocess/blob/dev/data-raw/archive_cases_dv_subset.R


if not Path("incidence_num_outlier_example.parquet").exists():
    incidence_num_outlier_example = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="jhu-csse",
            signals="confirmed_incidence_num",
            geo_type="state",
            time_type="day",
            geo_values="fl,nj",
            time_values=EpiRange(20200601, 20210531),
            as_of=20211028,
        )
        .df()
        .rename(columns={"issue": "version", "value": "cases"})
        .loc[:, ["geo_value", "time_value", "cases"]]
    )
    incidence_num_outlier_example.to_parquet(
        "data/incidence_num_outlier_example.parquet"
    )
    # TODO: as epi_df
else:
    incidence_num_outlier_example = pd.read_parquet(
        "incidence_num_outlier_example.parquet"
    )

if not Path("jhu_csse_daily_subset.parquet").exists():
    confirmed_incidence_num = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="jhu-csse",
            signals="confirmed_incidence_num",
            geo_type="state",
            time_type="day",
            geo_values="ca,fl,ny,tx,ga,pa",
            time_values=EpiRange(20200301, 20211231),
        )
        .df()
        .rename(columns={"issue": "version", "value": "cases"})
        .loc[:, ["geo_value", "time_value", "cases"]]
    )
    confirmed_7dav_incidence_num = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="jhu-csse",
            signals="confirmed_7dav_incidence_num",
            geo_type="state",
            time_type="day",
            geo_values="ca,fl,ny,tx,ga,pa",
            time_values=EpiRange(20200301, 20211231),
        )
        .df()
        .rename(columns={"issue": "version", "value": "cases_7d_av"})
        .loc[:, ["geo_value", "time_value", "cases_7d_av"]]
    )
    confirmed_7dav_incidence_prop = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="jhu-csse",
            signals="confirmed_7dav_incidence_prop",
            geo_type="state",
            time_type="day",
            geo_values="ca,fl,ny,tx,ga,pa",
            time_values=EpiRange(20200301, 20211231),
        )
        .df()
        .rename(columns={"issue": "version", "value": "case_rate_7d_av"})
        .loc[:, ["geo_value", "time_value", "case_rate_7d_av"]]
    )
    deaths_7dav_incidence_prop = (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="jhu-csse",
            signals="deaths_7dav_incidence_prop",
            geo_type="state",
            time_type="day",
            geo_values="ca,fl,ny,tx,ga,pa",
            time_values=EpiRange(20200301, 20211231),
        )
        .df()
        .rename(columns={"issue": "version", "value": "death_rate_7d_av"})
        .loc[:, ["geo_value", "time_value", "death_rate_7d_av"]]
    )
    jhu_csse_daily_subset = reduce(
        pd.merge,
        [
            confirmed_incidence_num,
            confirmed_7dav_incidence_num,
            confirmed_7dav_incidence_prop,
            deaths_7dav_incidence_prop,
        ],
    )
    jhu_csse_daily_subset.to_parquet("data/jhu_csse_daily_subset.parquet")
else:
    jhu_csse_daily_subset = pd.read_parquet("data/jhu_csse_daily_subset.parquet")
