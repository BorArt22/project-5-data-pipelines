import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load fact table using staging area and dimension tables.
    - Connect to Redshift
    - Rendering sql script
    - Run a querry
    """
    
    ui_color = '#F98866'

    insert_query = ("""
        INSERT INTO {target_table_name} ({target_table_fields})
    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table_name="",
                 target_table_fields="",
                 target_table_key="",
                 sql_query_insert="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table_name = target_table_name
        self.target_table_fields = target_table_fields
        self.target_table_key = target_table_key
        self.sql_query_insert = sql_query_insert

    def execute(self, context):
         # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        # Render sql script
         sqlquery = insert_query.format(target_table_name = self.target_table_name
                                       target_table_fields = self.target_table_fields)
                   + self.sql_query_insert

        # Execute UPSERT operation
        self.log.info("Executing Redshift UPSERT operation in fact table {}".format(self.target_table))
        redshift.run(self.sqlquery)
        self.log.info("Redshift UPSERT operation DONE in fact table {}.".format(self.target_table))
