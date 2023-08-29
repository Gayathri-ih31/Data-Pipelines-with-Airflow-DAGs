from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.secrets.metastore import MetastoreBackend
from udacity.common import create_tables


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
    """

    @apply_defaults
    def __init__(self,
                 file_path='',
                 table='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.file_path=file_path
        self.table=table

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")

        redshift_hook.run(f"DROP TABLE IF EXISTS {self.table}")


        if self.task_id == "Stage_events":
            redshift_hook.run(create_tables.CREATE_TABLE_STAGING_EVENTS)
        elif self.task_id == "Stage_songs":
            redshift_hook.run(create_tables.CREATE_TABLE_STAGING_SONGS)
        else:
            self.log.info('not implemented yet')
            
        redshift_hook.run(self.COPY_SQL.format(self.table, self.file_path, aws_connection.login, aws_connection.password))






