import os
import pymysql
import pandas as pb
from google.cloud import bigquery

def multisources_to_bigquery(request):
    req_obj = request.get_json(silent=True)
    job_done = False
    if req_obj and req_obj['message'] == os.getenv('TASK_TO_EXECUTE'):
        connection = pymysql.connect(
            unix_socket = os.getenv('CONNECTION_STRING'),
            user = os.getenv('USERNAME'),
            password = os.getenv('PASSWORD'),
            db = os.getenv('DATABASE'),
            cursorclass = pymysql.cursors.DictCursor)
        print('Connection:', connection)
        
        sql_query = 'SELECT e.empId, e.name, e.email, e.created, e.deptId, d.name AS deptName, d.head AS deptHead, s.gross AS grossSalary, s.variable AS variablePay FROM employee e JOIN departments d ON e.deptId = d.deptId JOIN salary s ON s.empId = e.empId'
        
        sql_dataframe = pb.read_sql(sql_query, connection, index_col='empId')
        print('<--- SQL Dataframe --->')
        print(sql_dataframe.head(2))
        
        csv_dataframe = pb.read_csv('gs://mysql_dump_sreenu/emp_job_details.csv')
        print('<--- CSV Dataframe --->')
        print(csv_dataframe.head(2))
        
        dataframe = pb.merge(sql_dataframe, csv_dataframe, on='empId', how='inner')
        print('<--- Required Dataframe --->')
        print(dataframe.head(2))
        
        bq_client = bigquery.Client()
        bq_dataset = bq_client.dataset(os.getenv('BQ_TARGET_DATASET'))
        bq_table = bq_dataset.table(os.getenv('BQ_TARGET_TABLE'))
        
        bq_job_config = bigquery.LoadJobConfig()
        bq_job_config.autodetect = True
        bq_job_config.write_disposition = 'WRITE_TRUNCATE'
        
        write_to_bq = bq_client.load_table_from_dataframe(dataframe, bq_table, job_config=bq_job_config)
        write_to_bq.result()
        
        print('Running task {}'.format(write_to_bq))
        job_done = True
        
    # Return a valid response
    if job_done is True:
        return ('Success', 200)
    else:
        return ('Failed', 500)        
