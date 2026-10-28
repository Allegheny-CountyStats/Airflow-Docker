#!/usr/bin/env Rscript
require(DBI)
require(dplyr)

# dotenv::load_dot_env()

char_types <- c("char", "varchar", "nchar", "nvarchar")

# Column info query for one table
column_info_sql <- function(table_name, schema) {
  paste0("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = '", schema, "'")
}

# Char columns where Staging is declared wider than Master (-1 = MAX), so values could be truncated on insert
wider_char_cols <- function(stg_cols, mst_cols) {
  inner_join(stg_cols, mst_cols, by = "COLUMN_NAME", suffix = c("_STG", "_MST")) %>%
    filter(DATA_TYPE_STG %in% char_types,
           DATA_TYPE_MST %in% char_types,
           CHARACTER_MAXIMUM_LENGTH_MST != -1,
           CHARACTER_MAXIMUM_LENGTH_STG == -1 | CHARACTER_MAXIMUM_LENGTH_STG > CHARACTER_MAXIMUM_LENGTH_MST) %>%
    transmute(COLUMN_NAME, MASTER_LEN = CHARACTER_MAXIMUM_LENGTH_MST)
}

# One query returning the longest value and number of over-limit rows for each wide column
length_check_sql <- function(prel_table, wide_cols) {
  i <- seq_len(nrow(wide_cols))
  col <- wide_cols$COLUMN_NAME
  parts <- paste0("MAX(LEN([", col, "])) AS [max_", i, "], ",
                  "ISNULL(SUM(CASE WHEN LEN([", col, "]) > ", wide_cols$MASTER_LEN, " THEN 1 ELSE 0 END), 0) AS [over_", i, "]")
  paste0("SELECT ", paste(parts, collapse = ", "), " FROM ", prel_table, ";")
}

# Wide columns that actually hold values longer than Master allows
length_violations <- function(wide_cols, result) {
  i <- seq_len(nrow(wide_cols))
  wide_cols$MAX_STG_LEN <- as.numeric(unlist(result[1, paste0("max_", i)]))
  wide_cols$ROWS_OVER <- as.numeric(unlist(result[1, paste0("over_", i)]))
  filter(wide_cols, ROWS_OVER > 0)
}

format_violations <- function(prel_table, new_table, violations) {
  paste0(prel_table, " has values too long for ", new_table, ":\n",
         paste0("  [", violations$COLUMN_NAME, "] master limit ", violations$MASTER_LEN,
                ", longest staging value ", violations$MAX_STG_LEN,
                ", rows over limit ", violations$ROWS_OVER, collapse = "\n"))
}

# Returns NULL if every Staging value fits in Master, otherwise a message naming the columns
check_char_lengths <- function(con, table_name, prel_table, new_table, target_schema) {
  stg_cols <- dbGetQuery(con, column_info_sql(table_name, "Staging"))
  mst_cols <- dbGetQuery(con, column_info_sql(table_name, target_schema))
  wide_cols <- wider_char_cols(stg_cols, mst_cols)
  if (nrow(wide_cols) == 0) return(NULL)

  result <- dbGetQuery(con, length_check_sql(prel_table, wide_cols))
  violations <- length_violations(wide_cols, result)
  if (nrow(violations) == 0) return(NULL)
  format_violations(prel_table, new_table, violations)
}

main <- function() {
  dept <- Sys.getenv("DEPT")
  table <- Sys.getenv('TABLE')
  tables <- Sys.getenv('TABLES', table)
  tables <- unlist(strsplit(tables, ","))

  target_schema <- Sys.getenv('TARGET_SCHEMA', "Master")

  # ID_COL accepted so DAGs on older tags can switch by changing only the image tag
  id_cols <- Sys.getenv("ID_COLS", Sys.getenv("ID_COL"))
  id_cols <- unlist(strsplit(id_cols, ","))
  if (length(id_cols) == 0) stop("ID_COLS (or ID_COL) must be set", call. = FALSE)

  source <- Sys.getenv('SOURCE')

  wh_host <- Sys.getenv('WH_HOST')
  wh_db <- Sys.getenv('WH_DB')
  wh_user <- Sys.getenv('WH_USER')
  wh_pass <- Sys.getenv('WH_PASS')

  wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)
  on.exit(dbDisconnect(wh_con))
  # Any error inside a transaction rolls the whole transaction back on the server
  dbExecute(wh_con, "SET XACT_ABORT ON;")

  join <- paste0("ON ", paste0("m.", id_cols, " = s.", id_cols, collapse = " AND "))

  table_names <- paste(dept, source, tables, sep = "_")
  prel_tables <- paste0("Staging.", table_names)
  new_tables <- paste0(target_schema, ".", table_names)
  master_exists <- vapply(table_names, function(t) dbExistsTable(wh_con, Id(schema = target_schema, table = t)), logical(1))

  # Check every table before deleting anything
  problems <- c()
  for (i in seq_along(tables)) {
    if (master_exists[i]) {
      problems <- c(problems, check_char_lengths(wh_con, table_names[i], prel_tables[i], new_tables[i], target_schema))
    }
  }
  if (length(problems) > 0) {
    stop(paste0("No rows were deleted or inserted.\n", paste(problems, collapse = "\n")), call. = FALSE)
  }

  # Run Once for each table
  for (i in seq_along(tables)) {
    table_name <- table_names[i]
    prel_table <- prel_tables[i]
    new_table <- new_tables[i]

    # Skip if No Table to Append with
    if (master_exists[i]) {
      # Gather column names
      cols <- paste0("SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'")
      cn <- dbGetQuery(wh_con, cols)$COLUMN_NAME
      col_names <- paste0("[", cn, "]", collapse = ", ")

      sql_delete <- paste0("
      DELETE m
      FROM ", new_table, " m
      INNER JOIN ", prel_table, " s ", join, ";")

      sql_insert <- paste0(
        "WITH NewData AS (SELECT ", col_names, " FROM ", prel_table, ")
         INSERT INTO ", new_table, " (", col_names, ")
         SELECT ", col_names, " FROM NewData;"
      )

      # Delete and insert commit together, so a failed insert leaves Master untouched
      counts <- tryCatch(
        dbWithTransaction(wh_con, {
          x <- dbExecute(wh_con, sql_delete)
          y <- dbExecute(wh_con, sql_insert)
          c(deleted = x, inserted = y)
        }),
        error = function(e) {
          stop(paste0("Replace into ", new_table, " failed and was rolled back; ", new_table,
                      " is unchanged and ", prel_table, " was kept.\n", conditionMessage(e)), call. = FALSE)
        }
      )
      x <- counts[["deleted"]]
      y <- counts[["inserted"]]
      print(paste0(x, " rows matched with ", prel_table, " and then deleted from ", new_table))

      if (y - x < 0) {
        print(paste("Difference of", x - y, "rows deleted within master without replacement/update from staging"))
      } else {
        print(paste(y, "records added back to", new_table))
      }
    } else {
      sql_move <- paste("ALTER SCHEMA", target_schema, "TRANSFER", prel_table)
      dbExecute(wh_con, sql_move)
    }
  }

  # Drop Staging Tables only after every table succeeded, so a failed run can be re-run
  for (prel_table in prel_tables) {
    sql_drop <- paste('DROP TABLE IF EXISTS', prel_table)
    dbExecute(wh_con, sql_drop)
  }
}

if (!interactive()) main()
