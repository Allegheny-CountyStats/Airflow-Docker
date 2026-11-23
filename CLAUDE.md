# Airflow-Docker

## Tests
- `staging-to-warehouse/tests/test-staging-2.0.R`: offline testthat suite for the `staging-to-warehouse/2.0` scripts (DBI mocked).
  Run from `staging-to-warehouse/`: `Rscript -e 'testthat::test_dir("tests")'`.
  Set `STAGING_SCRIPT_DIR` to run it against another copy of the scripts. Guards against:
  - `staging_to_warehouse.R` ignoring `TABLE` and silently moving nothing
  - any of the three scripts exiting 0 when no tables are given
  - `staging_replace_warehouse.R` dropping only the last staging table
  - `dbExistsTable(SQL(...))` causing `name@name[["table"]]: subscript out of bounds`