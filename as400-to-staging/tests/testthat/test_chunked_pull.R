# Offline tests for as400_to_staging_chunk.R: RSQLite stands in for both JDE and the warehouse.
# Run from as400-to-staging/: Rscript -e "testthat::test_dir('tests/testthat')"
library(testthat)
library(DBI)

source(test_path("..", "..", "as400_to_staging_chunk.R"))

quiet <- function(...) invisible(NULL)

make_source <- function(n = 120) {
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  dbWriteTable(con, "F0911", data.frame(
    id = seq_len(n),
    fy = rep(c(21L, 22L, 23L), length.out = n),
    gldgj = 126001L,
    txt = c(rep("short", n - 1), strrep("x", 300)),  # longest value only in the last chunk
    stringsAsFactors = FALSE
  ))
  con
}

sqlite_opener <- function(con) {
  function(query) {
    res <- dbSendQuery(con, query)
    list(fetch = function(n) dbFetch(res, n), close = function() dbClearResult(res))
  }
}

run_copy <- function(src, wh, query, segments = "", chunk_size = 50, char_types = character(0)) {
  copy_segments(segment_queries(query, segments), sqlite_opener(src), make_writer(wh, "staging", char_types),
                chunk_size, log = quiet)
}

test_that("all rows land, including the last partial chunk", {
  src <- make_source(); wh <- dbConnect(RSQLite::SQLite(), ":memory:")
  expect_equal(run_copy(src, wh, "SELECT * FROM F0911"), 120)
  expect_equal(dbGetQuery(wh, "SELECT COUNT(*) n FROM staging")$n, 120)
  expect_equal(dbGetQuery(wh, "SELECT MAX(LENGTH(txt)) n FROM staging")$n, 300)
})

test_that("re-running overwrites instead of doubling the Staging table", {
  src <- make_source(); wh <- dbConnect(RSQLite::SQLite(), ":memory:")
  run_copy(src, wh, "SELECT * FROM F0911")
  run_copy(src, wh, "SELECT * FROM F0911")
  expect_equal(dbGetQuery(wh, "SELECT COUNT(*) n FROM staging")$n, 120)
})

test_that("char columns get source sizes, not sizes guessed from chunk 1", {
  # Before: odbc sized varchar from the first chunk, so a longer value in a later chunk failed the append
  expect_equal(char_field_types(c(a = 10L, b = 300L, c = 9000L)),
               c(a = "varchar(255)", b = "varchar(300)", c = "varchar(max)"))
  src <- make_source(); wh <- dbConnect(RSQLite::SQLite(), ":memory:")
  run_copy(src, wh, "SELECT * FROM F0911", char_types = char_field_types(c(txt = 300L)))
  ddl <- dbGetQuery(wh, "SELECT sql FROM sqlite_master WHERE name = 'staging'")$sql
  expect_match(ddl, "`txt` varchar\\(300\\)")
})

test_that("an error mid-copy fails the run", {
  calls <- 0
  failing_writer <- function(df, overwrite) {
    calls <<- calls + 1
    if (calls == 2) stop("warehouse write failed")
  }
  src <- make_source()
  expect_error(copy_segments(list(all = "SELECT * FROM F0911"), sqlite_opener(src), failing_writer, 50, log = quiet),
               "warehouse write failed")
})

test_that("segments run once each and write their union without duplicates", {
  src <- make_source(); wh <- dbConnect(RSQLite::SQLite(), ":memory:")
  query <- "SELECT * FROM F0911 WHERE fy BETWEEN SEGMENT_START AND SEGMENT_END"
  expect_equal(run_copy(src, wh, query, segments = "21-21,22-22,23-99"), 120)
  got <- dbGetQuery(wh, "SELECT COUNT(*) n, COUNT(DISTINCT id) d FROM staging")
  expect_equal(got$n, 120)
  expect_equal(got$d, 120)
})

test_that("an empty first segment still creates the table and later segments append", {
  src <- make_source(); wh <- dbConnect(RSQLite::SQLite(), ":memory:")
  query <- "SELECT * FROM F0911 WHERE fy BETWEEN SEGMENT_START AND SEGMENT_END"
  expect_equal(run_copy(src, wh, query, segments = "10-10,21-21"), 40)
  expect_equal(dbGetQuery(wh, "SELECT COUNT(*) n FROM staging")$n, 40)
  wh2 <- dbConnect(RSQLite::SQLite(), ":memory:")
  run_copy(src, wh2, query, segments = "10-10,11-11")
  expect_true(dbExistsTable(wh2, "staging"))
})

test_that("segments without tokens are refused instead of duplicating rows", {
  expect_error(segment_queries("SELECT * FROM F0911", "21-21,22-22"), "SEGMENT_START")
  expect_error(segment_queries("SELECT * WHERE fy BETWEEN SEGMENT_START AND SEGMENT_END", "21"), "start-end")
})

test_that("build_query keeps the as400_to_staging.R behavior", {
  expect_equal(build_query("", "ACPRDDTA", "F0101"), "SELECT * FROM ACPRDDTA.F0101")
  expect_equal(build_query("", "ACPRDDTA", "F08330", "BGUPMJ", ">", "126001"),
               "SELECT * FROM ACPRDDTA.F08330 WHERE BGUPMJ > '126001'")
  expect_equal(build_query("SELECT * FROM X WHERE A > VALUE", "", "X", "A", ">", "5"),
               "SELECT * FROM X WHERE A > 5")
})

test_that("JDE julian dates convert per chunk", {
  out <- add_julian_date(data.frame(GLDGJ = c(126001L, 125365L)), "GLDGJ")
  expect_equal(out$date, as.Date(c("2026-01-01", "2025-12-31")))
  expect_null(add_julian_date(data.frame(a = 1), "")$date)
})