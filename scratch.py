import datetime
from pathlib import Path

import epiweeks as epi
import pandas as pd
import requests
from epidatpy import EpiDataContext
from requests.models import Response

# Import EpiProcessPy to register the pandas accessors
import EpiProcessPy  # noqa: F401

StrType = pd.StringDtype()

PARAMS = {
    "source": "nssp",
    "columns": ["geo_value", "time_value", "value", "report_ts_nominal_start"],
    "limit": -1,
    "offset": 0,
    "report_ts_actual": None,
    "versions_before": None,
    "fill_method": None,
    "time_value": None,
    "geo_value": None,
    "geo_type": "state",
}

df1 = (
    (
        EpiDataContext(use_cache=True)
        .pub_covidcast(
            data_source="nssp",
            signals="pct_ed_visits_influenza",
            geo_type="state",
            time_type="week",
            geo_values="*",
            time_values="*",
            issues="*",
        )
        .df()
    )[["geo_value", "time_value", "issue", "value"]]
    .astype({"geo_value": StrType, "time_value": StrType, "issue": StrType, "value": float})
    .rename(columns={"issue": "version"})
)

p = Path("out.csv")

if p.exists():
    df2 = (
        pd.read_csv("out.csv")
        .astype(
            {
                "geo_value": StrType,
                "time_value": StrType,
                "report_ts_nominal_start": StrType,
                "value": float,
            }
        )
        .rename(columns={"report_ts_nominal_start": "version"})
        .assign(geo_value=lambda x: x.geo_value.str.lower())
    )
else:
    resp: Response = requests.get("https://delphi.cmu.edu/cast-api/epidata/v2", params=PARAMS)
    resp.raise_for_status()
    with open("out.csv", "wb") as f:
        f.write(resp.content)
    df2 = (
        pd.read_csv("out.csv")
        .astype({"geo_value": StrType, "time_value": StrType, "report_ts_nominal_start": StrType, "value": float})
        .rename(columns={"report_ts_nominal_start": "version"})
        .assign(geo_value=lambda x: x.geo_value.str.lower())
    )


# Normalize df1:
# - issue is an epiweek int like 202401, convert to datetime (epiweek start date)
# - time_value is also an epiweek int, convert to datetime
# df1["version"] = pd.to_datetime(df1["version"].apply(lambda x: epi.Week.fromstring(str(x)).startdate()))

# Normalize df2: issue is ISO timestamp, convert to epiweek start date
df2["version"] = df2["version"].apply(lambda x: str(epi.Week.fromdate(datetime.datetime.fromisoformat(x))))

# Create epi_archive objects (deduplicate first - take first observation for duplicates)
df1 = df1.drop_duplicates(
    subset=[
        "version",
        "geo_value",
        "time_value",
    ],
    keep="first",
).reset_index(drop=True)
df2 = df2.drop_duplicates(subset=["version", "geo_value", "time_value"], keep="first").reset_index(drop=True)


# Original set comparison
snaps1 = set(df1.version.unique())
snaps2 = set(df2.version.unique())

print("Snapshots in df1 but not df2:", snaps1 - snaps2)
print("Snapshots in df2 but not df1:", snaps2 - snaps1)

# --- Archive comparison using EpiProcessPy ---

arch1 = df1.epi_arch.as_epi_arch()
arch2 = df2.epi_arch.as_epi_arch()

print(f"Archive 1 shape: {arch1.shape}")
print(f"Archive 2 shape: {arch2.shape}")

# # Compare the archives
# stats = arch1.epi_arch.compare_archive(arch2, value_col="value", by="both")

# # View per-version summary
# version_summary = stats[stats["geo_value"] == "_all_"].sort_values("version")
# print("\n=== Per-Version Summary ===")
# print(version_summary[["version", "n_obs", "diff_mean", "diff_abs_mean", "diff_abs_max"]].to_string())

# # Find snapshots with largest discrepancies
# geo_stats = stats[stats["geo_value"] != "_all_"]
# print("\n=== Top 10 Largest Discrepancies (by geo) ===")
# print(geo_stats.nlargest(10, "diff_abs_max")[["version", "geo_value", "n_obs", "diff_abs_max", "diff_mean"]].to_string())
