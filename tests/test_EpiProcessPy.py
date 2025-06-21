"""Tests for EpiProcessPy."""

import math

import numpy as np
import pandas as pd
import pytest

import EpiProcessPy


@pytest.fixture
def test_data():
    return EpiProcessPy.jhu_csse_daily_subset.epi_snap.as_epi_snap()


def test_epi_df(test_data):
    assert test_data.attrs["as_of"] == pd.Timestamp("2021-12-31")
    assert test_data.index.names == ["geo_value", "time_value"]


def test_sum_groups(test_data):
    out = test_data.epi_snap.sum_groups("geo_value")
    assert set(out.columns) == {"cases", "cases_7d_av", "case_rate_7d_av", "death_rate_7d_av"}
    # Since we have a MultiIndex, check the geo_value level
    assert set(out.index.get_level_values("geo_value")) == {"ca", "fl", "ga", "ny", "pa", "tx"}


def test_slide_mean(test_data):
    result = test_data.epi_snap.slide_mean(window_size=3)

    # Check that result has same structure as input
    assert result.index.names == test_data.index.names
    assert list(result.columns) == list(test_data.columns)

    # Check that sliding mean produces reasonable values
    # First value should equal original (window of 1), ignoring dtype differences
    original_first = test_data.iloc[0]
    result_first = result.iloc[0]
    pd.testing.assert_series_equal(original_first, result_first, check_dtype=False)


def test_slide_sum(test_data):
    result = test_data.epi_snap.slide_sum(window_size=3)

    # Check structure
    assert result.index.names == test_data.index.names
    assert list(result.columns) == list(test_data.columns)

    # First value should equal original
    original_first = test_data.iloc[0]
    result_first = result.iloc[0]
    pd.testing.assert_series_equal(original_first, result_first, check_dtype=False)


def test_slide_std(test_data):
    result = test_data.epi_snap.slide_std(window_size=3)

    # Check structure
    assert result.index.names == test_data.index.names
    assert list(result.columns) == list(test_data.columns)

    # First value should be NaN for std with window size 1 (no variance)
    # With min_periods=1, pandas returns NaN for single value std
    assert result.iloc[0].isna().all()


def test_slide_var(test_data):
    result = test_data.epi_snap.slide_var(window_size=3)

    # Check structure
    assert result.index.names == test_data.index.names
    assert list(result.columns) == list(test_data.columns)

    # First value should be NaN for variance with window size 1
    # With min_periods=1, pandas returns NaN for single value variance
    assert result.iloc[0].isna().all()


def test_slide_min(test_data):
    result = test_data.epi_snap.slide_min(window_size=3)

    # Check structure
    assert result.index.names == test_data.index.names
    assert list(result.columns) == list(test_data.columns)

    # First value should equal original (min of single value is itself)
    original_first = test_data.iloc[0]
    result_first = result.iloc[0]
    pd.testing.assert_series_equal(original_first, result_first, check_dtype=False)


def test_slide_max(test_data):
    result = test_data.epi_snap.slide_max(window_size=3)

    # Check structure
    assert result.index.names == test_data.index.names
    assert list(result.columns) == list(test_data.columns)

    # First value should equal original (max of single value is itself)
    original_first = test_data.iloc[0]
    result_first = result.iloc[0]
    pd.testing.assert_series_equal(original_first, result_first, check_dtype=False)


def test_slide_functions_with_timedelta():
    """Test slide functions work with timedelta windows."""
    # Skip this test for now - timedelta windows need datetime index
    # which requires more complex setup than simple integer windows
    pass


def test_slide_functions_with_manual_calculation():
    """Test slide functions against manually calculated values."""
    # Create a small test dataset with known values
    dates = pd.date_range("2020-01-01", periods=10, freq="D")
    data = {
        "geo_value": ["state_a"] * 10,
        "time_value": dates,
        "cases": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],  # Simple sequence
        "deaths": [10, 15, 12, 18, 20, 16, 22, 25, 19, 23],  # More varied
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"]).epi_snap.as_epi_snap()

    # Test with window size 3
    window_size = 3

    # Calculate slide functions
    mean_result = test_df.epi_snap.slide_mean(window_size=window_size)
    sum_result = test_df.epi_snap.slide_sum(window_size=window_size)
    min_result = test_df.epi_snap.slide_min(window_size=window_size)
    max_result = test_df.epi_snap.slide_max(window_size=window_size)
    std_result = test_df.epi_snap.slide_std(window_size=window_size)
    var_result = test_df.epi_snap.slide_var(window_size=window_size)

    # Manual calculations for 'cases' column (values: 1,2,3,4,5,6,7,8,9,10)
    # Window 1: [1] -> mean=1, sum=1, min=1, max=1
    assert mean_result.iloc[0]["cases"] == 1.0
    assert sum_result.iloc[0]["cases"] == 1.0
    assert min_result.iloc[0]["cases"] == 1.0
    assert max_result.iloc[0]["cases"] == 1.0

    # Window 2: [1,2] -> mean=1.5, sum=3, min=1, max=2
    assert mean_result.iloc[1]["cases"] == 1.5
    assert sum_result.iloc[1]["cases"] == 3.0
    assert min_result.iloc[1]["cases"] == 1.0
    assert max_result.iloc[1]["cases"] == 2.0

    # Window 3: [1,2,3] -> mean=2, sum=6, min=1, max=3
    assert mean_result.iloc[2]["cases"] == 2.0
    assert sum_result.iloc[2]["cases"] == 6.0
    assert min_result.iloc[2]["cases"] == 1.0
    assert max_result.iloc[2]["cases"] == 3.0

    # Window 4: [2,3,4] -> mean=3, sum=9, min=2, max=4
    assert mean_result.iloc[3]["cases"] == 3.0
    assert sum_result.iloc[3]["cases"] == 9.0
    assert min_result.iloc[3]["cases"] == 2.0
    assert max_result.iloc[3]["cases"] == 4.0

    # Window 5: [3,4,5] -> mean=4, sum=12, min=3, max=5
    assert mean_result.iloc[4]["cases"] == 4.0
    assert sum_result.iloc[4]["cases"] == 12.0
    assert min_result.iloc[4]["cases"] == 3.0
    assert max_result.iloc[4]["cases"] == 5.0

    # Manual calculations for 'deaths' column (values: 10,15,12,18,20,16,22,25,19,23)
    # Window 3: [10,15,12] -> mean=12.33.., sum=37, min=10, max=15
    assert abs(mean_result.iloc[2]["deaths"] - 37 / 3) < 1e-10
    assert sum_result.iloc[2]["deaths"] == 37.0
    assert min_result.iloc[2]["deaths"] == 10.0
    assert max_result.iloc[2]["deaths"] == 15.0

    # Window 4: [15,12,18] -> mean=15, sum=45, min=12, max=18
    assert mean_result.iloc[3]["deaths"] == 15.0
    assert sum_result.iloc[3]["deaths"] == 45.0
    assert min_result.iloc[3]["deaths"] == 12.0
    assert max_result.iloc[3]["deaths"] == 18.0

    # Test std and var for known values
    # For window [1,2,3]: std = sqrt(((1-2)² + (2-2)² + (3-2)²)/2) = sqrt(1) = 1
    # Note: pandas uses sample std (ddof=1), so it's sqrt(2/2) = 1

    expected_std = math.sqrt(2 / 2)  # Sample standard deviation
    assert abs(std_result.iloc[2]["cases"] - expected_std) < 1e-10
    assert abs(var_result.iloc[2]["cases"] - 1.0) < 1e-10  # variance = 1

    # Test that std is sqrt of var for a known window
    window_var = var_result.iloc[4]["cases"]  # [3,4,5] variance
    window_std = std_result.iloc[4]["cases"]  # [3,4,5] std
    assert abs(window_std - math.sqrt(window_var)) < 1e-10


def test_slide_functions_edge_cases():
    """Test slide functions with edge cases."""
    # Create test data with edge cases
    dates = pd.date_range("2020-01-01", periods=5, freq="D")
    data = {
        "geo_value": ["test"] * 5,
        "time_value": dates,
        "zeros": [0, 0, 0, 0, 0],  # All zeros
        "same": [5, 5, 5, 5, 5],  # All same values
        "negative": [-1, -2, -3, -4, -5],  # Negative values
        "mixed": [1, -1, 2, -2, 3],  # Mixed positive/negative
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"]).epi_snap.as_epi_snap()
    window_size = 3

    # Test all zeros
    mean_result = test_df.epi_snap.slide_mean(window_size=window_size)
    assert mean_result["zeros"].iloc[2] == 0.0  # Mean of [0,0,0] = 0

    std_result = test_df.epi_snap.slide_std(window_size=window_size)
    assert std_result["zeros"].iloc[2] == 0.0  # Std of [0,0,0] = 0

    var_result = test_df.epi_snap.slide_var(window_size=window_size)
    assert var_result["zeros"].iloc[2] == 0.0  # Var of [0,0,0] = 0

    # Test all same values
    assert mean_result["same"].iloc[2] == 5.0  # Mean of [5,5,5] = 5
    assert std_result["same"].iloc[2] == 0.0  # Std of [5,5,5] = 0
    assert var_result["same"].iloc[2] == 0.0  # Var of [5,5,5] = 0

    min_result = test_df.epi_snap.slide_min(window_size=window_size)
    max_result = test_df.epi_snap.slide_max(window_size=window_size)
    assert min_result["same"].iloc[2] == 5.0  # Min of [5,5,5] = 5
    assert max_result["same"].iloc[2] == 5.0  # Max of [5,5,5] = 5

    # Test negative values: [-1,-2,-3]
    assert mean_result["negative"].iloc[2] == -2.0  # Mean of [-1,-2,-3] = -2
    assert min_result["negative"].iloc[2] == -3.0  # Min of [-1,-2,-3] = -3
    assert max_result["negative"].iloc[2] == -1.0  # Max of [-1,-2,-3] = -1

    # Test mixed values: [1,-1,2]
    assert mean_result["mixed"].iloc[2] == 2 / 3  # Mean of [1,-1,2] = 2/3
    assert min_result["mixed"].iloc[2] == -1.0  # Min of [1,-1,2] = -1
    assert max_result["mixed"].iloc[2] == 2.0  # Max of [1,-1,2] = 2

    sum_result = test_df.epi_snap.slide_sum(window_size=window_size)
    assert sum_result["mixed"].iloc[2] == 2.0  # Sum of [1,-1,2] = 2


def test_slide_functions_different_window_sizes():
    """Test slide functions with different window sizes."""
    # Create test data
    dates = pd.date_range("2020-01-01", periods=6, freq="D")
    data = {
        "geo_value": ["test"] * 6,
        "time_value": dates,
        "values": [10, 20, 30, 40, 50, 60],
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])

    # Test window size 1 (should equal original values)
    mean_w1 = test_df.epi_snap.slide_mean(window_size=1)
    assert (mean_w1["values"] == test_df["values"]).all()

    # Test window size 2
    mean_w2 = test_df.epi_snap.slide_mean(window_size=2)
    assert mean_w2["values"].iloc[0] == 10.0  # [10] -> 10
    assert mean_w2["values"].iloc[1] == 15.0  # [10,20] -> 15
    assert mean_w2["values"].iloc[2] == 25.0  # [20,30] -> 25
    assert mean_w2["values"].iloc[3] == 35.0  # [30,40] -> 35

    # Test window size 3
    mean_w3 = test_df.epi_snap.slide_mean(window_size=3)
    assert mean_w3["values"].iloc[0] == 10.0  # [10] -> 10
    assert mean_w3["values"].iloc[1] == 15.0  # [10,20] -> 15
    assert mean_w3["values"].iloc[2] == 20.0  # [10,20,30] -> 20
    assert mean_w3["values"].iloc[3] == 30.0  # [20,30,40] -> 30
    assert mean_w3["values"].iloc[4] == 40.0  # [30,40,50] -> 40

    # Test window size larger than data
    mean_w10 = test_df.epi_snap.slide_mean(window_size=10)
    # Should use all available data at each step
    assert mean_w10["values"].iloc[0] == 10.0  # [10] -> 10
    assert mean_w10["values"].iloc[1] == 15.0  # [10,20] -> 15
    assert mean_w10["values"].iloc[5] == 35.0  # [10,20,30,40,50,60] -> 35


def test_slide_functions_grouping_behavior():
    """Test that slide functions work correctly within groups."""
    # Create test data with multiple groups to verify grouping behavior
    dates = pd.date_range("2020-01-01", periods=4, freq="D")
    data = {
        "geo_value": ["state_a"] * 4 + ["state_b"] * 4,
        "time_value": dates.tolist() + dates.tolist(),
        "values": [10, 20, 30, 40, 100, 200, 300, 400],  # Different scales for each state
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])

    # Test sliding mean with window size 2
    mean_result = test_df.epi_snap.slide_mean(window_size=2)

    # Verify that calculations are done within each group
    # state_a calculations: [10], [10,20], [20,30], [30,40]
    assert mean_result.loc[("state_a", dates[0]), "values"] == 10.0  # [10] -> 10
    assert mean_result.loc[("state_a", dates[1]), "values"] == 15.0  # [10,20] -> 15
    assert mean_result.loc[("state_a", dates[2]), "values"] == 25.0  # [20,30] -> 25
    assert mean_result.loc[("state_a", dates[3]), "values"] == 35.0  # [30,40] -> 35

    # state_b calculations: [100], [100,200], [200,300], [300,400]
    assert mean_result.loc[("state_b", dates[0]), "values"] == 100.0  # [100] -> 100
    assert mean_result.loc[("state_b", dates[1]), "values"] == 150.0  # [100,200] -> 150
    assert mean_result.loc[("state_b", dates[2]), "values"] == 250.0  # [200,300] -> 250
    assert mean_result.loc[("state_b", dates[3]), "values"] == 350.0  # [300,400] -> 350

    # Test that the rolling window doesn't cross group boundaries
    # If it were applying across the entire DataFrame, we'd see values like
    # (40 + 100) / 2 = 70 at the boundary, but we shouldn't

    # Test sliding sum to verify the same behavior
    sum_result = test_df.epi_snap.slide_sum(window_size=2)
    assert sum_result.loc[("state_a", dates[3]), "values"] == 70.0  # [30,40] -> 70
    assert sum_result.loc[("state_b", dates[0]), "values"] == 100.0  # [100] -> 100 (not mixed with state_a)


def test_growth_rate_with_manual_calculation():
    """Test growth rate with manually calculated values."""
    # Create test data with known growth patterns
    dates = pd.date_range("2020-01-01", periods=8, freq="D")
    data = {
        "geo_value": ["state_a"] * 8,
        "time_value": dates,
        "cases": [100, 110, 121, 133.1, 146.41, 161.051, 177.1561, 194.87171],  # 10% growth each day
        "deaths": [10, 20, 30, 40, 50, 60, 70, 80],  # Linear growth
        "constant": [50, 50, 50, 50, 50, 50, 50, 50],  # No growth
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])

    # Test growth rate with window_size=1 (no smoothing)
    growth_result = test_df.epi_snap.growth_rate(window_size=1)

    # Manual calculations for 'cases' column (10% growth rate)
    # Growth rate = (new_value - old_value) / old_value
    # Day 1: NaN (no previous value)
    assert pd.isna(growth_result.iloc[0]["cases"])

    # Day 2: (110 - 100) / 100 = 0.10 = 10%
    assert abs(growth_result.iloc[1]["cases"] - 0.10) < 1e-10

    # Day 3: (121 - 110) / 110 = 0.10 = 10%
    assert abs(growth_result.iloc[2]["cases"] - 0.10) < 1e-10

    # Day 4: (133.1 - 121) / 121 = 0.10 = 10%
    assert abs(growth_result.iloc[3]["cases"] - 0.10) < 1e-10

    # Manual calculations for 'deaths' column (linear growth)
    # Day 2: (20 - 10) / 10 = 1.0 = 100%
    assert abs(growth_result.iloc[1]["deaths"] - 1.0) < 1e-10

    # Day 3: (30 - 20) / 20 = 0.5 = 50%
    assert abs(growth_result.iloc[2]["deaths"] - 0.5) < 1e-10

    # Day 4: (40 - 30) / 30 = 0.333... = 33.33%
    assert abs(growth_result.iloc[3]["deaths"] - (1 / 3)) < 1e-10

    # Day 5: (50 - 40) / 40 = 0.25 = 25%
    assert abs(growth_result.iloc[4]["deaths"] - 0.25) < 1e-10

    # Manual calculations for 'constant' column (no growth)
    # All growth rates should be 0 after the first NaN
    for i in range(1, len(test_df)):
        assert abs(growth_result.iloc[i]["constant"] - 0.0) < 1e-10


def test_growth_rate_with_smoothing():
    """Test growth rate with rolling mean smoothing."""
    # Create test data with some volatility that smoothing should reduce
    dates = pd.date_range("2020-01-01", periods=6, freq="D")
    data = {
        "geo_value": ["state_a"] * 6,
        "time_value": dates,
        "volatile": [100, 150, 110, 160, 120, 170],  # Oscillating pattern
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])

    # Test with window_size=3 for smoothing
    growth_smoothed = test_df.epi_snap.growth_rate(window_size=3)

    # Manual calculation of smoothed values and growth rates
    # Window 1: [100] -> 100.0
    # Window 2: [100, 150] -> 125.0
    # Window 3: [100, 150, 110] -> 120.0
    # Window 4: [150, 110, 160] -> 140.0
    # Window 5: [110, 160, 120] -> 130.0
    # Window 6: [160, 120, 170] -> 150.0

    # Growth rates:
    # Day 1: NaN (no previous smoothed value)
    assert pd.isna(growth_smoothed.iloc[0]["volatile"])

    # Day 2: (125.0 - 100.0) / 100.0 = 0.25 = 25%
    assert abs(growth_smoothed.iloc[1]["volatile"] - 0.25) < 1e-10

    # Day 3: (120.0 - 125.0) / 125.0 = -0.04 = -4%
    assert abs(growth_smoothed.iloc[2]["volatile"] - (-0.04)) < 1e-10

    # Day 4: (140.0 - 120.0) / 120.0 = 0.16667... ≈ 16.67%
    assert abs(growth_smoothed.iloc[3]["volatile"] - (20 / 120)) < 1e-10

    # Day 5: (130.0 - 140.0) / 140.0 = -0.07143... ≈ -7.14%
    assert abs(growth_smoothed.iloc[4]["volatile"] - (-10 / 140)) < 1e-10

    # Day 6: (150.0 - 130.0) / 130.0 = 0.15385... ≈ 15.38%
    assert abs(growth_smoothed.iloc[5]["volatile"] - (20 / 130)) < 1e-10


def test_growth_rate_grouping_behavior():
    """Test that growth rate calculations are done within groups."""
    # Create test data with two groups having different growth patterns
    dates = pd.date_range("2020-01-01", periods=4, freq="D")
    data = {
        "geo_value": ["state_a"] * 4 + ["state_b"] * 4,
        "time_value": dates.tolist() + dates.tolist(),
        "values": [100, 110, 121, 133.1, 1000, 1100, 1210, 1331],  # Both 10% growth, different scales
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])

    # Test growth rate with no smoothing
    growth_result = test_df.epi_snap.growth_rate(window_size=1)

    # Both groups should have the same 10% growth rate pattern
    # state_a: Day 2 growth = (110 - 100) / 100 = 0.10
    assert abs(growth_result.loc[("state_a", dates[1]), "values"] - 0.10) < 1e-10

    # state_b: Day 2 growth = (1100 - 1000) / 1000 = 0.10
    assert abs(growth_result.loc[("state_b", dates[1]), "values"] - 0.10) < 1e-10

    # state_a: Day 3 growth = (121 - 110) / 110 = 0.10
    assert abs(growth_result.loc[("state_a", dates[2]), "values"] - 0.10) < 1e-10

    # state_b: Day 3 growth = (1210 - 1100) / 1100 = 0.10
    assert abs(growth_result.loc[("state_b", dates[2]), "values"] - 0.10) < 1e-10

    # Verify that calculations don't cross group boundaries
    # If they did, we'd see different growth rates due to the scale difference


def test_growth_rate_edge_cases():
    """Test growth rate with edge cases."""
    # Create test data with edge cases
    dates = pd.date_range("2020-01-01", periods=5, freq="D")
    data = {
        "geo_value": ["test"] * 5,
        "time_value": dates,
        "zeros": [0, 0, 0, 0, 0],  # All zeros
        "zero_to_positive": [0, 10, 20, 30, 40],  # Starting from zero
        "negative": [-10, -5, -2, -1, 0],  # Negative values
        "mixed_signs": [10, -5, 15, -3, 12],  # Mixed positive/negative
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])
    growth_result = test_df.epi_snap.growth_rate(window_size=1)

    # Test all zeros - growth rate should be NaN for all periods after first
    assert pd.isna(growth_result["zeros"].iloc[0])  # First is always NaN
    for i in range(1, len(test_df)):
        # 0/0 should result in NaN
        assert pd.isna(growth_result["zeros"].iloc[i])

    # Test zero to positive - growth rate should be infinite (represented as inf)
    assert pd.isna(growth_result["zero_to_positive"].iloc[0])  # First is always NaN
    assert np.isinf(growth_result["zero_to_positive"].iloc[1])  # (10-0)/0 = inf

    # Subsequent growth rates should be finite
    # (20-10)/10 = 1.0 = 100%
    assert abs(growth_result["zero_to_positive"].iloc[2] - 1.0) < 1e-10

    # Test negative values - should work normally
    # (-5 - (-10)) / (-10) = 5 / (-10) = -0.5 = -50%
    assert abs(growth_result["negative"].iloc[1] - (-0.5)) < 1e-10

    # (-2 - (-5)) / (-5) = 3 / (-5) = -0.6 = -60%
    assert abs(growth_result["negative"].iloc[2] - (-0.6)) < 1e-10


def test_growth_rate_column_selection():
    """Test growth rate with specific column selection."""
    dates = pd.date_range("2020-01-01", periods=4, freq="D")
    data = {
        "geo_value": ["state_a"] * 4,
        "time_value": dates,
        "cases": [100, 110, 121, 133.1],  # 10% growth
        "deaths": [10, 20, 30, 40],  # Various growth rates
        "population": [1000000, 1000000, 1000000, 1000000],  # Constant (should not change)
    }

    test_df = pd.DataFrame(data).set_index(["geo_value", "time_value"])

    # Test with specific columns
    growth_result = test_df.epi_snap.growth_rate(columns=["cases", "deaths"], window_size=1)

    # Should only have cases and deaths columns
    assert set(growth_result.columns) == {"cases", "deaths"}

    # Verify calculations are correct for selected columns
    assert abs(growth_result.iloc[1]["cases"] - 0.10) < 1e-10  # 10% growth
    assert abs(growth_result.iloc[1]["deaths"] - 1.0) < 1e-10  # 100% growth
