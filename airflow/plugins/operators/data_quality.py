from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs scripts to check table for number of rows

    redshift_conn_id - name of Rendsift connection in Airflow
    table_key_list - dictionary with pairs of table and key
    dq_checks - list of checks
    """
    
    ui_color = '#89DA59'

    sql_template_count = """
        SELECT count(1) as count_f
        FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_key_list="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_key_list = table_key_list
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('DataQuality BEGIN')
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        for dq_check in self.dq_checks:
            if dq_check["type"] == "fill":
                self.log.info('Check filling in tables')
                for table_key in self.table_key_list:
                    table = table_key["table_name"]
                    self.log.info('Checking filling in {} BEGIN'.format(table))
                    formatted_sql = dq_check["check_sql"].format(table_name = table)

                    records = redshift.get_records(formatted_sql)

                    if len(records) < 1 or len(records[0]) < 1:
                        raise ValueError(f"Data quality check failed. {table} returned no results")
                    num_records = records[0][0]
                    if num_records < 1:
                        raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                    self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
                    
            elif dq_check["type"] == "double":
                self.log.info('Check double in keys in tables')
                for table_key in self.table_key_list:
                    table = table_key["table_name"]
                    key = table_key["table_key"]
                    self.log.info('Checking double in {} in table {} BEGIN'.format(key, table))
                    formatted_sql = dq_check["check_sql"].format(table_key = key, table_name = table)

                    records = redshift.get_records(formatted_sql)

                    if len(records) < 1 or len(records[0]) < 1:
                        raise ValueError(f"Data quality check failed. {table} returned no results")
                    num_records = records[0][0]
                    if num_records == dq_check["exp_res"]:
                        self.log.info('No double in table {}'.format(table))
                    else:
                        self.log.info('Double in table {}'.format(table))

            else:
                self.log.info('Unexpected TYPE of data quality checks')

        self.log.info('DataQuality DONE')