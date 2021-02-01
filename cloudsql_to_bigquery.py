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
        query = 'SELECT * FROM salary'
        
        # Create dataframe reading table data
        result = pb.read_sql(query, connection, index_col='id')
        print(result.head(5))
        
        # Connect to BigQuery client
        client = bigquery.Client()
        dataset_id = 'test'
        dataset = client.dataset(dataset_id)
        table_name = dataset.table('salary')
        
        # Create job configurations
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = 'WRITE_TRUNCATE'
        
        # Load data into BigQuery table
        load_data = client.load_table_from_dataframe(result, table_name, job_config=job_config)
        load_data.result() # Wait for the job to finish
        
        # Print loading data task id
        print('Running task {}'.format(load_data))
        job_done = True
        
    # Return a valid response
    if job_done is True:
        return ('Success', 200)
    else:
        return ('Failed', 500)
