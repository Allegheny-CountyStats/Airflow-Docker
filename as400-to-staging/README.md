# as400-to-staging

Provide a list of tables within a database schema and move them to the Datawarehouse from an IBM AS400 Database. 

Image name: `countystats/as400-to-staging:r`

* dept: Department Name*
* table: Table to be pulled from Data source*
* sql: Custom SQL code for pulling out a table.
  * Note: if you are using this feature then you should only pass one table name in the `TABLES` field.
* uid: Source Database login Connection value *
* pwd: Source Database password Connection value*
* host: Source Database host Connection value*
* source: Source name*
* wh_host: Data Warehouse login Connection value*
* wh_db: Data Warehouse database Connection value*
* wh_un: Data Warehouse login Connection value*
* wh_pw: Data Warehouse password Connection value*
* append_col: Column to check for new values
* date_col: Name of column needed to transform date from Julian Calendar
  * Default: `append_col`
* append_type: SQL Function to use to find new values
  * Default value `MAX`
* append_sign: WHERE statement sign for appending new data.
  * Default: `>`

## Chunked pulls: `countystats/as400-to-staging:r.chunk`

For large pulls (100k+ rows). Built from `Tags/r.chunk/Dockerfile`, runs `Tags/r.chunk/as400_to_staging_chunk.R`. Takes the same variables as above, plus:

* chunk_size: Rows fetched from JDE and written to Staging per batch
  * Default: `50000`
* block_size: jt400 block size in KB per network round trip (8-512)
  * Default: `512`
* java_heap: JVM options for rJava
  * Default: `-Xmx2g`

DATE and TIMESTAMP columns arrive as the driver's text (varchar), the same as `:r`. The r-basic 4.2 base has RJDBC 0.2-10, which otherwise converts them with ISO formats that don't match jt400's output, turning every value into NULL. The script turns that conversion off; `:r` needs the same if it ever moves to the 4.2 base. The log's first `columns:` line shows each column's type and NA count.

The first chunk replaces the Staging table and later chunks append to it. If any chunk fails, the task fails, so a partial table is never handed to the next task. The log prints the time spent opening the query, fetching and writing, so you can tell whether JDE or the warehouse write is the bottleneck.

Chunking speeds up the transfer, not the query itself. If opening the query is slow, check whether the WHERE clause can use an F0911 index (see `QSYS2.SYSTABLEINDEXSTAT`): splitting one query into several (e.g. by fiscal year) only helps when each piece can use an index, otherwise each piece re-scans the table.

### Tests

Offline (RSQLite stands in for JDE and the warehouse; no Java needed). From the repo root (where `Airflow-Docker.Rproj` is):

```
Rscript -e "testthat::test_dir('as400-to-staging/tests/testthat')"
```

## Example:
```
connection = BaseHook.get_connection("as400_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
Dept = 'ExampleDept
source = 'Example'
...
pull_example = DockerOperator(
                task_id='pull_example,
                image='countystats/as400-to-staging:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'dept': dept,
                    'host': connection.host,
                    'uid': connection.login,
                    'pwd': connection.password,
                    'source': source,
                    'table': 'Example',
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```
