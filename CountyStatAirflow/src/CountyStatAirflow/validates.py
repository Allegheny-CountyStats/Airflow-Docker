from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable, task_group

def validate_task(variable, dept = dept, schema = connection.schema, pool = 'default', url_var = 'docker_remote'):
    validate_schema = DockerOperator(
        task_id=f'Validate_{variable}',
        image='countystats/data-validate:r',
        api_version=Variable.get("docker_api_version"),
        auto_remove='force',
        environment={
            'DEPT': dept,
            'TABLE': variable,
            'COL_SCHEMA': Variable.get("""{}_schema""".format(variable)),
            'SOURCE': schema,
            'WH_HOST': wh_connection.host,
            'WH_DB': wh_connection.schema,
            'WH_USER': wh_connection.login,
            'WH_PASS': wh_connection.password
        },
        docker_url= Variable.get(url_var),
        command='Rscript schema-validate.R',
        network_mode="bridge",
        pool=pool
    )
    return validate_schema

@task_group(group_id='data_validations', dept = dept, schema = connection.schema, pool = 'default')
def data_validation(table_list, ):
    """
    :rtype: DependencyMixIn
    """
    for tables_l in table_list[0:]:
        task_iterate = validate_task(tables_l, dept, schema, pool)
        task_iterate