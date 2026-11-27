#!/usr/bin/env Rscript
# Chunked variant of as400_to_staging.R (image tag r.chunk). Same env vars, plus:
#   chunk_size - rows per fetch/write (default 50000)
#   block_size - jt400 KB per network round trip (default 512, the jt400 maximum)
library(DBI)
library(dplyr)
library(rlang)

build_query <- function(sql, schema, table, append_col = "", append_sign = ">", value = NULL) {
  if (sql != '') {
    query <- sql
  } else {
    query <- paste0("SELECT * FROM ", schema, ".", table)
  }
  if (append_col != '') {
    if (sql != '') {
      query <- gsub("VALUE", value, query)
    } else {
      query <- paste0(query, " WHERE ", append_col, " ", append_sign, " '", value, "'")
    }
  }
  query
}

add_julian_date <- function(df, date_col) {
  if (date_col != "") {
    df <- df %>%
      mutate(date = as.Date(as.character(!!sym(date_col)), "1%y%j"))
  }
  df
}

# varchar lengths from the source column sizes, so a long value in a later chunk still fits.
# (odbc would otherwise size columns from chunk 1 only.) Same 255 floor odbc uses for short strings.
char_field_types <- function(precisions) {
  vapply(precisions, function(p) {
    if (is.na(p) || p > 8000) "varchar(max)" else paste0("varchar(", max(255L, p), ")")
  }, character(1))
}

jdbc_char_precisions <- function(res) {
  # java.sql.Types: CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR
  char_types <- c(1L, 12L, -1L, -15L, -9L, -16L)
  md <- res@md
  precisions <- integer(0)
  for (i in seq_len(rJava::.jcall(md, "I", "getColumnCount"))) {
    if (rJava::.jcall(md, "I", "getColumnType", i) %in% char_types) {
      precisions[rJava::.jcall(md, "S", "getColumnLabel", i)] <- rJava::.jcall(md, "I", "getPrecision", i)
    }
  }
  precisions
}

make_writer <- function(con, target, char_types = character(0)) {
  function(df, overwrite) {
    if (overwrite) {
      types <- vapply(df, function(col) dbDataType(con, col), character(1))
      known <- intersect(names(char_types), names(types))
      types[known] <- char_types[known]
      dbWriteTable(con, target, df, overwrite = TRUE, field.types = types)
    } else {
      dbWriteTable(con, target, df, append = TRUE)
    }
  }
}

elapsed <- function(since) as.numeric(difftime(Sys.time(), since, units = "secs"))

# Fetch and write until the result is exhausted. The first chunk overwrites the Staging table
# (even if empty, so the table always exists); later chunks append. Errors propagate so the task fails.
copy_in_chunks <- function(fetch_chunk, write_chunk, chunk_size, date_col = "", first_write = TRUE, log = message) {
  rows <- 0
  repeat {
    t0 <- Sys.time()
    chunk <- fetch_chunk(chunk_size)
    fetched <- nrow(chunk)
    if (fetched == 0 && !first_write) break
    fetch_secs <- elapsed(t0)
    chunk <- add_julian_date(chunk, date_col)
    t1 <- Sys.time()
    write_chunk(chunk, overwrite = first_write)
    log(sprintf("  chunk: %d rows, fetch %.1fs, write %.1fs", fetched, fetch_secs, elapsed(t1)))
    first_write <- FALSE
    rows <- rows + fetched
    rm(chunk)
    gc(verbose = FALSE)
    if (fetched == 0) break
  }
  rows
}

main <- function() {
  dept <- Sys.getenv("dept")
  host <- Sys.getenv("host")
  uid <- Sys.getenv("uid")
  pwd <- Sys.getenv("pwd")
  source <- Sys.getenv("source")
  schema <- Sys.getenv("schema")
  table <- Sys.getenv('table')
  sql <- Sys.getenv("sql")

  wh_host <- Sys.getenv('wh_host')
  wh_db <- Sys.getenv('wh_db')
  wh_user <- Sys.getenv('wh_user')
  wh_pass <- Sys.getenv('wh_pass')
  append_col <- Sys.getenv('append_col')
  append_type <- Sys.getenv('append_type', "MAX")
  append_sign <- Sys.getenv('append_sign', ">")
  date_col <- Sys.getenv('date_col', append_col)

  chunk_size <- as.integer(Sys.getenv('chunk_size', "50000"))
  block_size <- Sys.getenv('block_size', "512")

  # JVM heap must be set before rJava starts
  options(java.parameters = Sys.getenv('java_heap', "-Xmx2g"))
  library(RJDBC)

  t_start <- Sys.time()
  jdbcDriver <- JDBC(driverClass="com.ibm.as400.access.AS400JDBCDriver", classPath="/lib/jt400-jdk9-10.1.jar")
  ibm <- dbConnect(jdbcDriver, host, user = uid, password = pwd, secure = "true",
                   "block size" = block_size, prefetch = "true", "data compression" = "true")
  on.exit(dbDisconnect(ibm), add = TRUE)
  wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)
  on.exit(dbDisconnect(wh_con), add = TRUE)
  message(sprintf("connected in %.1fs", elapsed(t_start)))

  table_name <- paste(dept, source, table, sep = "_")

  value <- NULL
  if (append_col != '') {
    value_sql <- paste0("SELECT ", append_type, "(", append_col, ") as value FROM Master.", table_name)
    value <- dbGetQuery(wh_con, value_sql)$value
  }
  query <- build_query(sql, schema, table, append_col, append_sign, value)

  t0 <- Sys.time()
  res <- dbSendQuery(ibm, query)
  message(sprintf("query opened in %.1fs", elapsed(t0)))
  writer <- make_writer(wh_con, SQL(paste0("Staging.", table_name)), char_field_types(jdbc_char_precisions(res)))
  total <- tryCatch(
    copy_in_chunks(function(n) fetch(res, n, block = min(n, 32768L)), writer, chunk_size, date_col),
    finally = dbClearResult(res)
  )
  message(sprintf("done: %d rows to Staging.%s in %.1fs", total, table_name, elapsed(t_start)))
}

if (sys.nframe() == 0L) main()