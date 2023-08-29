from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from udacity.common import create_tables
from airflow.secrets.metastore import MetastoreBackend

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                table='',
                query='',
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.query=query

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")

        redshift_hook.run(f"DROP TABLE IF EXISTS {self.table}")
        
        redshift_hook.run(create_tables.CREATE_TABLE_SONGSPLAYS)

        self.log.info(f'Loading Fact table: {self.table}')
        redshift_hook.run(f"INSERT INTO {self.table} {self.query}" )

