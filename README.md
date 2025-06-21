# epiprocess.py

A Python port of the R package [epiprocess](https://github.com/cmu-delphi/epiprocess).

-   Free software: MIT license
-   Documentation: <https://EpiProcessPy.readthedocs.io>.

## Status

A barely started port.
We hope that the Python version will be much simpler due to Pandas' better support for extensibility and because we don't need to support tidyeval.

## TODO

-   [x] Base `epi_df -> EpiSnapAccessor` and `epi_archive -> EpiArchiveAccessor` classes.
-   [ ] Implement `epi_df` methods.
    -   [x] `as_epi_snap` constructor.
    -   [x] `slide` set of rolling functions.
    -   [x] `sum_groups` sum over a group index.
    -   [x] `fill` fill in missing values.
    -   [x] `complete` fill in missing values.
        -   [x] daily cadence dates index
        -   [ ] weekly cadence dates index
        -   [ ] epiweek index
    -   [x] `keys` get the keys of the dataframe.
    -   [x] `group` group by keys.
    -   [ ] `growth_rate` growth rate functions.
    -   [ ] `correlation` correlations between columns.
    -   [ ] `detect_outliers` detect outliers.
    -   [ ] `autoplot` plot the dataframe.
    -   [ ] `print` print the dataframe.
-   [ ] Implement `epi_archive` methods.
    -   [x] `as_epi_arch` constructor.
    -   [ ] Diff-based compression.
    -   [ ] `as_of` subsetter and `epi_df` constructor and `as_of_current`.
    -   [ ] `slide` rolling functions.
    -   [ ] `epix_merge` merge two `epi_arch` objects.
    -   [ ] `epix_fill_through_versions` fill archive unobserved history.
    -   [ ] `epix_truncate_versions_after` to keep only older versions.
    -   [ ] `revision_history` revision history functions.
    -   [ ] `keys` get the keys of the dataframe.
    -   [ ] `autoplot` plot the dataframe.
    -   [ ] `print` print the dataframe.
    -   [ ] Archive -> archive transforms.
-   [ ] General methods.
    -   [ ] Datetime handling.
        -   [x] daily cadence dates index
        -   [ ] weekly cadence dates index
        -   [ ] epiweek index
    -   [ ]
