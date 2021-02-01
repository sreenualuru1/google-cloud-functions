import os
import pymysql
import pandas as pb
from google.cloud import bigquery

def cloudsql_to_bigquery(request):
    request_obj = request.get_json(silent=True)
    job_done = False
    if request_obj and request_obj['message'] == os.getenv('TASK_TO_EXECUTE'):
        # Connect to CloudSql
        connection = pymysql.connect(
            unix_socket=os.getenv('CONNECTION_STRING'),
            user=os.getenv('USERNAME'),
            password=os.getenv('PASSWORD'),
            db=os.getenv('DATABASE'),
            cursorclass=pymysql.cursors.DictCursor)
        print('Connection:', connection)
            
        # Query to read data from CloudSql table
        sql_query = 'SELECT * FROM <sql_table_name>'
        
        # Create dataframe reading table data
        dataframe = pb.read_sql(sql_query, connection, index_col='<col_name>Ex:id')
        print(dataframe.head(5))
        
        # Connect to BigQuery client
        bq_client = bigquery.Client()
        bq_dataset = bq_client.dataset('<bq_dataset_name>')
        bq_table_name = bq_dataset.table('<bq_table_name>')
        
        # Create job configurations
        bq_job_config = bigquery.LoadJobConfig()
        bq_job_config.autodetect = True
        bq_job_config.write_disposition = 'WRITE_TRUNCATE' # WRITE_APPEND, WRITE_EMPTY, WRITE_DISPOSITION_UNSPECIFIED
        
        # Load data into BigQuery table
        write_data = bq_client.load_table_from_dataframe(dataframe, bq_table_name, job_config=bq_job_config)
        write_data.result() # Wait for the job to finish
        
        # Print write job task id
        print('Running task {}'.format(write_data))
        job_done = True
        
    # Return a valid response
    if job_done is True:
        return ('Success', 200)
    else:
        return ('Failed', 500)
