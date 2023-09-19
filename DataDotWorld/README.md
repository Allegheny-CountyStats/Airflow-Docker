# DataDotWorld Templates

Container for housing scripts that interact with the DataDotWorld API. 

Image name: `countystats/datadotworld:r`

### Query Template (DDW_SPARQL_Query.R)
 
Query template for interacting with DataDotWorld API. Allows specification of a query (called as an airflow variable that's stored as a .json [no line breaks]) and can be utilized for looping (if specified through the environment variable) over collections if a %LOOP_LOOP% character is used within the query. 

### Collections Query (DDW_Collections_Query.R <mark>NOT IMAGED</mark> )

Used to collect all collection names housed within DDW catalog.

### Send_DDW_Email.py

Template pulls DDW sourced tables from CountyStat warehouse, produces a list of stewards to email, formats urls that link to datatable/column catalog record(s), inserts urls into email message based on specified html template, then sends emails out.

The email sender imports **send_email.py** which houses function used in the email step.  

Requirements:
+ Parameters

Associated projects:
+ [DDW_Steward_Check](https://github.com/Allegheny-CountyStats/DDW_Steward_Check)
  + UnFilled Metadata
