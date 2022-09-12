from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load dimension tables using staging area.
    - Connect to Redshift
    - Rendering sql script
    - Run a querry

    redshift_conn_id - name of Rendsift connection in Airflow
    target_table_name - name of table that will be data is loading to
    target_table_fields - fields in target table
    target_table_key - primary key in target table
    sql_query_update - sql query for update in target table
    sql_query_insert - sql query for insert to target table
    insert_mode - mode for insert query 
                'append' (as default) - load data with new primary key from staging area
                'insert_delete' - truncate target table and load data from staging area
                'append_update' - load data with new primary key from staging area and run update query
    """
    
    ui_color = '#80BD9E'

    insert_query = ("""
        INSERT INTO {target_table_name} ({target_table_fields})
    """)

    append_mode_query_where = ("""
        NOT EXISTS (SELECT {target_table_key} 
                    FROM {target_table_name} 
                    WHERE {target_table_name}.{target_table_key} = stage.{target_table_key})
    """)

    delete_query = ("TRUNCATE TABLE {target_table_name};")

    insert_delete_mode_query_where = ("1=1")

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table_name="",
                 target_table_fields="",
                 target_table_key="",
                 sql_query_update="",
                 sql_query_insert="",
                 insert_mode = "append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table_name = target_table_name
        self.target_table_fields = target_table_fields
        self.target_table_key = target_table_key
        self.sql_query_update = sql_query_update
        self.sql_query_insert = sql_query_insert
        self.insert_mode = insert_mode

    def execute(self, context):
        # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")


        insert_query = self.insert_query.format(target_table_name = self.target_table_name,
                                                target_table_fields = self.target_table_fields)
        # Render sql script
        self.log.info("Rendering sql script for {}. Insert mode = {}".format(self.target_table_name, self.insert_mode))
        if self.insert_mode == "append":
            sqlquery = insert_query + \
                       self.sql_query_insert.format(INSERT_MODE_QUERY = LoadDimensionOperator.append_mode_query_where.format(
                                                        target_table_key = self.target_table_key,
                                                        target_table_name = self.target_table_name
                       ))
        elif self.insert_mode == "insert_delete":
            sqlquery = LoadDimensionOperator.delete_query.format(target_table_name = self.target_table_name) + \
                       insert_query + \
                       self.sql_query_insert.format(INSERT_MODE_QUERY = LoadDimensionOperator.insert_delete_mode_query_where)
        elif self.insert_mode == "append_update": 
            sqlquery = insert_query + \
                       self.sql_query_insert.format(INSERT_MODE_QUERY = LoadDimensionOperator.append_mode_query_where.format(
                                                        target_table_key = self.target_table_key,
                                                        target_table_name = self.target_table_name
                       )) + \
                       self.sql_query_update
        else:
            self.log.info("Invalid value in insert_mode = {}".format(self.insert_mode))
            sqlquery = ""


        # Execute SQL operation
        self.log.info("Executing Redshift SQL operation in dimension table {}".format(self.target_table_name))
        redshift.run(sqlquery)
        self.log.info("Redshift SQL operation DONE in dimension table {}.".format(self.target_table_name))
