# Porting the R `epiprocess` test suite

A scoping doc for which R tests in `epiprocess/tests/testthat/` are worth porting
to this package, and why.

## What's portable, what isn't

Most of the R `epi_slide` test suite (~600 of 947 lines in `test-epi_slide.R`)
exercises the **tidyeval DSL**: `~ sum(.x$value)` formulas, `.ref_time_value`
and `.group_key` pronouns, `:=` walrus, `data.frame` vs `list` vs packed
outputs, sequential data-masking expressions, `.data`/`.env` injection. We have
no DSL here — our `slide` takes a plain `Callable` and a `window_size`. Porting
those tests requires building the DSL first, which is a much bigger project
than the tests. **Skip.**

What's left after stripping DSL tests are tests of the underlying _windowing
semantics_ — and that's where our coverage was thin. Worth porting.

## A. `epi_snap.slide` — windowing semantics — **DONE**

R's parameter matrix at `test-epi_slide.R:172` cross-products:

    {day, week, yearmonth, integer} × {extra_keys T/F} ×
    {ref_time_values: None/early/middle} × {.all_rows T/F} ×
    {.align: right/center/left} × {.window_size: 7, Inf}

Mapped to our API:

| Axis                                         | R values              | We support today       | Status                           |
| -------------------------------------------- | --------------------- | ---------------------- | -------------------------------- |
| Window alignment                             | right / center / left | right only             | Not ported (needs `.align`)      |
| Window size = Inf (expanding)                | yes                   | no                     | Not ported (needs `expanding()`) |
| Extra group keys                             | yes                   | yes (via `extra_keys`) | **Ported**                       |
| Multiple geos, non-overlapping time          | yes                   | works                  | **Ported**                       |
| Missing time rows + reported NAs distinction | yes                   | yes                    | **Ported**                       |
| Time types: weekly / yearmonth               | yes                   | weekly/epiweek TODO    | Skip until cadence work lands    |
| `.ref_time_values` filtering                 | yes                   | no                     | Not ported (needs new param)     |
| `.all_rows` (keep all rows, NA non-ref)      | yes                   | no                     | Not ported (needs new param)     |

### Tests 1–8 (added)

1. `test_slide_independent_per_geo_with_offset_time_index` — three geos, one
   with offset dates; per-geo isolation.
2. `test_slide_int_window_ignores_calendar_gap` + `test_slide_timedelta_window_respects_calendar_gap`
   — int windows count rows, Timedelta windows respect calendar gaps.
3. `test_slide_skips_nan_values` — pandas sum default skips NaN.
4. `test_slide_extra_keys_do_not_bleed` — `(geo, age_group)` isolation.
5. `test_slide_left_boundary_min_periods` — `min_periods=1` semantics.
6. `test_slide_and_slide_sum_agree` — callable-based slide matches named.
7. `test_slide_single_row_group` — no crash.
8. `test_slide_all_nan_group` — all-NaN → NaN output.

### Bugs surfaced and fixed

- **`complete()` crashed entirely with `extra_keys`** (`AssertionError` from
  pandas reindex). The product over `[geos, dates]` ignored extra index
  levels. Fixed at `EpiProcessPy.py:120` to product over all non-time keys.
  Covered by `test_complete_with_extra_keys`.
- **Timedelta windows raised `ValueError`** — pandas needs `on="time_value"`
  for time-based grouped rolling, plus the time column needs to be a column
  not just an index level. Added `_rolling()` helper at `EpiProcessPy.py:57`
  that branches on window type; `_fix_grouped_rolling_result` handles both
  shapes. Covered by `test_slide_timedelta_window_respects_calendar_gap`.

## B. `epi_arch.merge_archive` — multi-version overwrite semantics — **DONE**

The R test at `test-epix_merge.R:8` is the goldmine. Single geo "ak" with five
(geo, time) pairs that have **interleaved version updates** between x and y —
including:

- Overlapping versions (both update on same date).
- Versions on x that surround versions on y (and vice versa).
- A measurement with `NA` updates that should LOCF-as-NA, not
  LOCF-the-previous-non-NA.
- One signal missing entirely (only x or only y for some `(geo, time)`).

### Ports

1. **DONE** — `test_merge_archive_propagates_explicit_na_updates` covers the
   NA-LOCF case (simplified single-(geo, time) version of the R fixture). Fix
   replaced `_locf_across_versions` (used `ffill`, silently overwrote explicit
   NAs) with `_locf_to_index`, an asof-join keyed on the version axis. See
   `EpiProcessPy.py:382`.
2. **DONE** — three smaller `s1/s2` scenarios at `test-epix_merge.R:67-153`
   ported as `test_merge_archive_locf_scenario_{1,2,3}`. Each exercises a
   different interleaving pattern of x/y updates and asserts the right value
   appears at every (geo, time, version) in the union.
3. **DONE** — `sync="na"` and `sync="truncate"` semantics aligned with R.
    - `sync="na"`: inserts a synthetic NA-update at the lagging side's
      `versions_end + 1 day` via `fill_through_versions`, then LOCFs. Covered
      by `test_merge_archive_sync_na_inserts_synthetic_na_update` (port of
      `test-epix_merge.R:218-230`).
    - `sync="truncate"`: previously intersected observed versions; now caps the
      merged archive at `min(versions_end)`, matching R. Covered by
      `test_merge_archive_sync_truncate_caps_at_min_versions_end` (port of
      `test-epix_merge.R:241-249`). Existing `test_merge_archive_sync_options`
      updated to match.

## C. `epix_slide` — **DONE**

Added at `EpiProcessPy.py:374`. Signature:

```python
def epix_slide(self, func, before=Timedelta.max, versions=None, new_col_name="slide_value")
```

For each version v: take the snapshot via new `_snapshot_at(v)` (latest
update per non-version key with version ≤ v), filter to
`time_value >= v - before`, group by non-time keys, apply `func`. Output is
long-form `[*keys, version, <new_col_name>]`.

Tests:

- `test_epix_slide_basic_sum_window` — port of `test-epix_slide.R:21-72`
  with the exact `2^k` expected sums (12, 72, 1536, 49152).
- `test_epix_slide_default_versions_and_before` — covers defaults.

## What's still open

| Item                                                         | Priority | Notes                               |
| ------------------------------------------------------------ | -------- | ----------------------------------- |
| A axis: `.align` (left/center)                               | Low      | Needs new param + impl              |
| A axis: expanding (Inf) window                               | Low      | Needs new param + impl              |
| A axis: `.ref_time_values` / `.all_rows`                     | Low      | Biggest surface; defer until needed |
| README items: `revision_history`, `autoplot`, weekly cadence | n/a      | Tracked in README                   |

Skipping: anything testing the tidyeval/DSL, non-daily time types (until
cadence work lands), `clobberable_versions_start` (we don't model it).
