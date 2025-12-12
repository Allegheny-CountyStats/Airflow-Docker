from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable, task_group
from airflow.models import connection


def validate_task(variable, department_name, schema, connection_name: connection,
                  pool='Remote_Pool', url_var='docker_remote'):
    validate_schema = DockerOperator(
        task_id=f'Validate_{variable}',
        image='countystats/data-validate:r',
        api_version=Variable.get("docker_api_version"),
        auto_remove='force',
        environment={
            'DEPT': department_name,
            'TABLE': variable,
            'COL_SCHEMA': Variable.get("""{}_schema""".format(variable)),
            'SOURCE': schema,
            'WH_HOST': connection_name.host,
            'WH_DB': connection_name.schema,
            'WH_USER': connection_name.login,
            'WH_PASS': connection_name.password
        },
        docker_url=Variable.get(url_var),
        command='Rscript schema-validate.R',
        network_mode="bridge",
        mount_tmp_dir=False,
        pool=pool
    )
    return validate_schema


@task_group(group_id='data_validations')
def data_validation(table_list, department_name, schema, connection_name, pool, url_var):
    """
    :rtype: DependencyMixIn
    """
    for tables_l in table_list[0:]:
        task_iterate = validate_task(variable=tables_l, department_name=department_name, schema=schema,
                                     connection_name=connection_name, pool=pool, url_var=url_var)
        task_iterate
