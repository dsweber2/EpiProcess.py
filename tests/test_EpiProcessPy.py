#!/usr/bin/env python

"""Tests for `EpiProcessPy` package."""

import pandas as pd
import pytest

import EpiProcessPy


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """


def test_epi_df():
    epi_df = EpiProcessPy.jhu_csse_daily_subset.epi_snap.as_epi_snap()
    assert epi_df.attrs["as_of"] == pd.Timestamp("2021-12-31")
    assert epi_df.index.names == ["geo_value", "time_value"]


def test_sum_group():
    epi_df = EpiProcessPy.jhu_csse_daily_subset.epi_snap.as_epi_snap()
    out = epi_df.epi_snap.sum_group("geo_value")
    assert set(out.columns) == {"cases", "cases_7d_av", "case_rate_7d_av", "death_rate_7d_av"}
    assert set(out.index) == {"ca", "fl", "ga", "ny", "pa", "tx"}
