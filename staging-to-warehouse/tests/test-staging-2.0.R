# Offline tests for staging-to-warehouse/2.0 scripts. DBI calls are mocked; no warehouse connection.
# Run from staging-to-warehouse/: Rscript -e 'testthat::test_dir("tests")'
library(testthat)
skip_if_not_installed("DBI")
skip_if_not_installed("odbc")
skip_if_not_installed("dplyr")
skip_if_not_installed("withr")

# STAGING_SCRIPT_DIR lets the suite run against another copy of the scripts (e.g. to confirm a test fails)
script_dir <- Sys.getenv("STAGING_SCRIPT_DIR", normalizePath(file.path(testthat::test_path(), "..", "2.0")))

# Sources a real script with fake env vars and mocked DBI. existing = tables dbExistsTable reports as present.
run_script <- function(script, env, existing = character()) {
  log <- new.env()
  log$sql <- character()
  log$exists_args <- list()
  mocks <- new.env(parent = globalenv())
  mocks$dbConnect <- function(...) "fake_con"
  mocks$dbDisconnect <- function(...) TRUE
  mocks$dbExistsTable <- function(con, name, ...) {
    log$exists_args[[length(log$exists_args) + 1]] <- name
    stopifnot(is(name, "Id"))
    paste(name@name[["schema"]], name@name[["table"]], sep = ".") %in% existing
  }
  mocks$dbExecute <- function(con, sql, ...) { log$sql <- c(log$sql, sql); 1L }
  mocks$dbGetQuery <- function(con, sql, ...) data.frame(COLUMN_NAME = "id", id = 1)
  defaults <- c(DEPT = "Dept", SOURCE = "Src", TABLE = "", TABLES = "", ID_COLS = "id",
                WH_HOST = "fake", WH_DB = "fake", WH_USER = "fake", WH_PASS = "fake")
  withr::with_envvar(modifyList(as.list(defaults), as.list(env)), {
    # Empty TABLES must count as unset so the TABLE fallback applies
    if (!"TABLES" %in% names(env) || env[["TABLES"]] == "") Sys.unsetenv("TABLES")
    sys.source(file.path(script_dir, script), envir = mocks)
  })
  log
}

# Bug: staging_to_warehouse.R ignored TABLE, looped over nothing, and exited 0 without moving the table.
test_that("overwrite honours TABLE", {
  log <- run_script("staging_to_warehouse.R", c(TABLE = "T1"), existing = "Staging.Dept_Src_T1")
  expect_true(any(grepl("ALTER SCHEMA Master TRANSFER Staging.Dept_Src_T1", log$sql, fixed = TRUE)))
})

# Bug: with no tables the scripts passed silently. They must now error.
test_that("all scripts stop when no tables given", {
  for (s in c("staging_to_warehouse.R", "staging_append_warehouse.R", "staging_replace_warehouse.R")) {
    expect_error(run_script(s, c()), "No tables given", info = s)
  }
})

# Bug: replace dropped only the last table's staging copy (drop loop reused stale prel_table).
test_that("replace drops every staging table", {
  log <- run_script("staging_replace_warehouse.R", c(TABLES = "T1,T2"),
                    existing = c("Master.Dept_Src_T1", "Master.Dept_Src_T2"))
  expect_true(any(grepl("DROP TABLE IF EXISTS Staging.Dept_Src_T1", log$sql, fixed = TRUE)))
  expect_true(any(grepl("DROP TABLE IF EXISTS Staging.Dept_Src_T2", log$sql, fixed = TRUE)))
})

# Bug: dbExistsTable(SQL("Schema.Table")) raised `name@name[["table"]]: subscript out of bounds`.
test_that("overwrite and append pass Id() to dbExistsTable", {
  for (s in c("staging_to_warehouse.R", "staging_append_warehouse.R")) {
    log <- run_script(s, c(TABLES = "T1"), existing = "Staging.Dept_Src_T1")
    expect_true(all(vapply(log$exists_args, is, logical(1), "Id")), info = s)
  }
})
