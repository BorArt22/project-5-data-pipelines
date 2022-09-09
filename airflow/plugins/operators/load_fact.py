from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sqlquery="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sqlquery = sqlquery

    def execute(self, context):
         # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        # Execute UPSERT operation
        self.log.info("Executing Redshift UPSERT operation in fact table {}".format(self.target_table))
        redshift.run(self.sqlquery)
        self.log.info("Redshift UPSERT operation DONE in fact table {}.".format(self.target_table))
