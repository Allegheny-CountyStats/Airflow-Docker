# Documentation on Kerberos Authentication within CountyStats

Steps:
- Write Keytab File to image folder using ktutil (WriteKeytab.sh) from Putty
  - Can include multiple users from Allegheny.County.Local domain 
  - Requires interactive bash session to enter password for account listed in keytab (below code uses T115235 AND sa00427 as examples):
```shell
ktutil
addent -password -p T115235@COUNTY.ALLEGHENY.LOCAL -k 1 -e rc4-hmac
addent -password -p sa00427@COUNTY.ALLEGHENY.LOCAL -k 1 -e rc4-hmac
wkt sa00427.keytab
exit
```
- Enusre dockerfile references image with Kerberos packages installed
- Add a following lines to Dockerfile of project/image:
```dockerfile
COPY sa00427.keytab /
RUN kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t sa00427.keytab
```
- This configuration allows for subsequent connections to used a 'Trusted Connection', like this example R script:
```r
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass, Trusted_Connection = "Yes")
```