from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from udacity.common import create_tables
from airflow.secrets.metastore import MetastoreBackend


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table='',
                 query='',
                 append_only='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.query=query
        self.append_only=append_only

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")

        redshift_hook.run(f"DROP TABLE IF EXISTS {self.table}")

        
        if self.task_id == "Load_user_dim_table":
            redshift_hook.run(create_tables.CREATE_TABLE_USERS)
        elif self.task_id == "Load_song_dim_table":
            redshift_hook.run(create_tables.CREATE_TABLE_SONGS)
        elif self.task_id == "Load_artist_dim_table":
            redshift_hook.run(create_tables.CREATE_TABLE_ARTISTS)
        elif self.task_id == "Load_time_dim_table":
            redshift_hook.run(create_tables.CREATE_TABLE_TIME)
        else:
            self.log.info('not implemented yet')

        self.log.info(f'Loading Dim table: {self.table}')
        redshift_hook.run(f'INSERT INTO {self.table} {self.query}' )
