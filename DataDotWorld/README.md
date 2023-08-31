# DataDotWorld Templates

Container for housing scripts that interact with the DataDotWorld API. 

Image name: `countystats/datadotworld:r`

### Query Template (DDW_SPARQL_Query.R)
 
Query template for interacting with DataDotWorld API. Allows specification of a query (called as an airflow variable that's stored as a .json [no line breaks]) and can be utilized for looping (if specified through the environment variable) over collections if a %LOOP_LOOP% character is used within the query. 

### Collections Query (DDW_Collections_Query.R <mark>NOT IMAGED</mark> )

Used to collect all collection names housed within DDW catalog.