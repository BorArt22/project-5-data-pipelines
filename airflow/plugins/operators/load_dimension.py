from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load dimension tables using staging area.
    - Connect to Redshift
    - Run a querry

    redshift_conn_id - name of Rendsift connection in Airflow
    target_table - name of dimension table
    sqlquery - sql query with upsert operation
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sqlquery="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sqlquery = sqlquery

    def execute(self, context):
        # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        # Execute UPSERT operation
        self.log.info("Executing Redshift UPSERT operation in dimension table {}".format(self.target_table))
        redshift.run(self.sqlquery)
        self.log.info("Redshift UPSERT operation DONE in dimension table {}.".format(self.target_table))
