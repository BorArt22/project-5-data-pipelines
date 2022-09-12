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
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table_name = target_table_name
        self.target_table_fields = target_table_fields
        self.target_table_key = target_table_key
        self.sql_query_update = sql_query_update
        self.sql_query_insert = sql_query_insert

    def execute(self, context):
        # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        # Render sql script
        temp_table_create_rendered = temp_table_create.format(
                                            target_table_name = self.target_table_name,
                                            target_table_fields = self.target_table_fields,
                                            sql_query_insert = self.sql_query_insert)
        
        temp_table_delete_rendered = temp_table_delete.format(
                                            target_table_name = self.target_table_name,
                                            target_table_key = self.target_table_key)

        target_table_insert_rendered = target_table_insert.format(
                                            target_table_name = self.target_table_name,
                                            target_table_fields = self.target_table_fields)

        sql_query = sql_query_update + temp_table_delete_rendered + target_table_insert_rendered

        transaction = transaction_temp.format(sql_query = sql_query)

        temp_table_drop_rendered = temp_table_drop.format(target_table_name = self.target_table_name)

        sqlquery = temp_table_create_rendered + transaction + temp_table_drop_rendered

        # Execute UPSERT operation
        self.log.info("Executing Redshift UPSERT operation in dimension table {}".format(self.target_table))
        redshift.run(self.sqlquery)
        self.log.info("Redshift UPSERT operation DONE in dimension table {}.".format(self.target_table))
