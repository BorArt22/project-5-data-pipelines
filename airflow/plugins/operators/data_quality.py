from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    '''
    ui_color = '#89DA59'

    sql_template_count = """
        SELECT count(1) as count_f
        FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list

    def execute(self, context):
        self.log.info('DataQuality BEGIN')
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        for table in self.table_list:
            self.log.info('Checking DataQuality in {} BEGIN'.format(table))
            
            formatted_sql = DataQualityOperator.sql_template_count.format(table)
            records = redshift_hook.get_records(formatted_sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

        self.log.info('DataQuality DONE')