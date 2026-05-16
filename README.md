# epiprocess.py

A Python port of the R package [epiprocess](https://github.com/cmu-delphi/epiprocess).

- Free software: MIT license
- Documentation: <https://EpiProcessPy.readthedocs.io>.

## Status

A simple feature port.
We hope that the Python version can be much simpler due to Pandas' better support for extensibility and without the added complexity of tidyeval.

## TODO

- [x] Base `epi_df -> EpiSnapAccessor` and `epi_archive -> EpiArchiveAccessor` classes.
- [ ] Implement basic versions of `epi_df` methods.
    - [x] `as_epi_snap` constructor.
    - [x] `slide` set of rolling functions.
    - [x] `sum_groups` sum over a group index.
    - [x] `fill` fill in missing values.
    - [x] `complete` fill in missing values.
    - [x] `keys` get the keys of the dataframe.
    - [x] `group` group by keys.
    - [x] `growth_rate` growth rate functions.
    - [x] `correlation` correlations between columns.
    - [x] `detect_outliers` detect outliers.
    - [ ] `autoplot` plot the dataframe (make a time series plot where each line is a distinct geo).
    - [x] `print` print the dataframe.
- [ ] Implement basic versions of `epi_archive` methods.
    - [x] `as_epi_arch` constructor.
    - [x] `as_of` and `as_of_current`.
    - [x] `slide` rolling functions.
    - [x] Diff-based compression (`compress`).
    - [x] `merge_archive` merge two `epi_arch` objects with LOCF sync.
    - [x] `compare_archive` compare two archives and compute difference statistics.
    - [x] `fill_through_versions` fill archive unobserved history.
    - [x] `truncate_versions_after` to keep only older versions.
    - [ ] `revision_history` revision history functions.
    - [ ] `autoplot` plot the dataframe (make a time series plot where each line is a distinct geo, but also we take in a list versions, and we print the snapshots at those versions; see epiprocess-R/R/autoplot.R, autoplot.epi_archive()).
    - [ ] `print` print the dataframe.
- [ ] General methods.
    - [ ] Datetime handling.
        - [x] daily cadence dates index
        - [ ] weekly cadence dates index
        - [ ] epiweek index
- [ ] Data.
    - [ ] Figure out how to manage sample and test data.

## Usage

Using [uv](https://github.com/astral-sh/uv):

```py
# Install the package and dependencies
uv sync
# Run tests
uv run pytest tests/ --cov=src --cov-config=pyproject.toml --cov-report=term-missing
# Build the docs
uv run mkdocs build
# Serve the docs locally
uv run mkdocs serve
```
