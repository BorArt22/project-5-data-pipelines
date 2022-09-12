import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load fact table using staging area and dimension tables.
    - Connect to Redshift
    - Run a querry

    redshift_conn_id - name of Rendsift connection in Airflow
    target_table - name of fact table
    sqlquery - sql query with upsert operation
    """
    
    ui_color = '#F98866'

    temp_table_create = ("""
        CREATE TEMP TABLE temp_table_{target_table_name} (like {target_table_name});
            INSERT INTO temp_table_{target_table_name} ({target_table_fields})
                {sql_query_insert}
        ;
    """)

    transaction_temp = ("""
        BEGIN TRANSACTION;
            {sql_query}
        END TRANSACTION;
    """)

    temp_table_delete = ("""
            DELETE FROM temp_table_{target_table_name}
            USING {target_table_name}
            WHERE temp_table_{target_table_name}.{target_table_key} = {target_table_name}.{target_table_key}
    """)

    target_table_insert = ("""
            INSERT INTO public.{target_table_name} ({target_table_fields})
            SELECT *
            FROM temp_table_{target_table_name}
    """)

    temp_table_drop = ("""
        DROP TABLE temp_table_{target_table_name};
    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table_name="",
                 target_table_fields="",
                 target_table_key="",
                 sql_query_update="",
                 sql_query_insert="",
                 execution_date="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table_name = target_table_name
        self.target_table_fields = target_table_fields
        self.target_table_key = target_table_key
        self.sql_query_update = sql_query_update
        self.sql_query_insert = sql_query_insert
        self.execution_date = execution_date

    def execute(self, context):
         # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        # Render sql script
        exec_date_rendered = self.execution_date.format(**context)
        exec_date_obj = datetime.datetime.strptime( exec_date_rendered, \
                                                    '%Y-%m-%d')
        time_begin_obj = exec_date_obj - datetime.timedelta(hours=1)
        time_end_obj =  exec_date_obj + datetime.timedelta(hours=1)

        time_begin_string = "{year}-{month:02d}-{day:02d} {hour:02d}:00:00".format(
                                year = time_begin_obj.year,
                                month = time_begin_obj.month,
                                day = time_begin_obj.day,
                                hour = time_begin_obj.hour
        )
        time_end_string = "{year}-{month:02d}-{day:02d} {hour:02d}:00:00".format(
                                year = time_end_obj.year,
                                month = time_end_obj.month,
                                day = time_end_obj.day,
                                hour = time_end_obj.hour
        )
        sql_query_insert_rendered = self.sql_query_insert.format(time_begin, time_end)

        temp_table_create_rendered = LoadFactOperator.temp_table_create.format(
                                            target_table_name = self.target_table_name,
                                            target_table_fields = self.target_table_fields,
                                            sql_query_insert = sql_query_insert_rendered)
        
        temp_table_delete_rendered = LoadFactOperator.temp_table_delete.format(
                                            target_table_name = self.target_table_name,
                                            target_table_key = self.target_table_key)

        target_table_insert_rendered = LoadFactOperator.target_table_insert.format(
                                            target_table_name = self.target_table_name,
                                            target_table_fields = self.target_table_fields)

        sql_query = sql_query_update + temp_table_delete_rendered + target_table_insert_rendered

        transaction = LoadFactOperator.transaction_temp.format(sql_query = sql_query)

        temp_table_drop_rendered = LoadFactOperator.temp_table_drop.format(target_table_name = self.target_table_name)

        sqlquery = temp_table_create_rendered + transaction + temp_table_drop_rendered

        # Execute UPSERT operation
        self.log.info("Executing Redshift UPSERT operation in fact table {}".format(self.target_table))
        redshift.run(self.sqlquery)
        self.log.info("Redshift UPSERT operation DONE in fact table {}.".format(self.target_table))
