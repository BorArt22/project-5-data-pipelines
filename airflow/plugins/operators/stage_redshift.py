import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Original datasets are loading into staging tables 
    from Amazon Simple Storage Service (Amazon S3) bucket 
    using command copy one to one without processing data source.
    - Connect to AWS
    - Get credentials AWS
    - Connect to Redshift
    - Delete staging table
    - Preparation sql script from template
    - Copy from S3 to staging table

    redshift_conn_id - name of Rendsift connection in Airflow
    aws_credentials_id - name of AWS connection in Airflow
    target_table - staging table
    s3_bucket - name of bucket
    s3_key - name of folder in bucket
    json_paths - name of json paths in biucket
    use_partitioned_data - variable for definitions what types of data used - partioned or not
    execution_date - execution date
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key","execution_date")

    sql_template_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json {}
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 json_paths="\'auto\'",
                 use_partitioned_data="False",
                 execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_paths = json_paths
        self.use_partitioned_data = use_partitioned_data
        self.execution_date = execution_date        

    def execute(self, context):
        # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        self.log.info("Clearing data from Redshift target table")
        redshift.run("DELETE FROM {}".format(self.target_table))

        # Prepare S3 paths
        self.log.info("Preparing Copying data from S3 to Redshift")
        exec_date_rendered = self.execution_date.format(**context)
        self.log.info("Execution_date: {}".format(exec_date_rendered))
        exec_date_obj = datetime.datetime.strptime( exec_date_rendered, \
                                                    '%Y-%m-%d')
        self.log.info("Execution_year: {}".format(exec_date_obj.year))
        self.log.info("Execution_month: {}".format(exec_date_obj.month))
        self.log.info("Execution_day: {}".format(exec_date_obj.day))

        if self.use_partitioned_data == "True":
            s3_path = "s3://{s3_bucket}/{s3_key}/{year}/{month}/{year}-{month:02d}-{day:02d}-events.json".format(
                                                            s3_bucket = self.s3_bucket, 
                                                            s3_key = self.s3_key,
                                                            year = exec_date_obj.year, 
                                                            month = exec_date_obj.month,
                                                            day = exec_date_obj.day)
        else:
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        if self.json_paths == "":
            s3_json_path = "\'auto\'"
        else:
            s3_json_path = "\'s3://{}/{}\'".format( self.s3_bucket, \
                                                    self.json_paths)

        self.log.info("S3_PATH: {}".format(s3_path))
        self.log.info("S3_JSON_PATH: {}".format(s3_json_path))

        # Copy data from S3 to Redshift
        self.log.info("Preparing for JSON input data")
        formatted_sql = StageToRedshiftOperator.sql_template_json.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            s3_json_path
        )

        # Executing COPY operation
        self.log.info("Executing Redshift COPY operation")
        redshift.run(formatted_sql)
        self.log.info("Redshift COPY operation DONE.")