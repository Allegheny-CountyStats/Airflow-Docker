#  Instructions

This image is for transfering data to the Move It Server in a spatial file format (currently only GEOJSON files are supported).

Image Name: `countystats/moveit-geo:r`
* Tested command: `Rscript moveit-transfer.R`
* Not working command: `Rscript moveit-download.R`

## Enviornmental Variables
* TABLE: Full Table Name*
* SNAKECASE: Transform column names to snakecase.
* COORDS: Latitude/Longitude columns in the source tabled separated by a comma `(,)`*
* FILENAME: Name for geojson file
  * Default: Table Name + .geojson
* FOLDER: Folder name on the MoveIt server that file will be transferred to.*
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*

*Required Variable

## Example Dag Usage:
```
wh_connection = BaseHook.get_connection("data_warehouse")
...
mi_geo = DockerOperator(
                task_id='mi_geo',
                image='countystats/moveit-geo:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'TABLE': 'Example_V',
                    'FILENAME': "sourcesites.geojson",
                    'SNAKECASE': 'TRUE',
                    'COORDS': 'Longitude,Latitude',
                    'FOLDER': moveit_folder,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password,
                    'MI_USER': Variable.get('moveit_username'),
                    'MI_PASS': Variable.get('moveit_password')
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript moveit-transfer.R',
                network_mode="bridge"
        )
```
