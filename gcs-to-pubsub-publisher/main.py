import os
import logging
from sys import prefix
import pandas as pd
from google.cloud import storage

project_id = os.environ.get('project_id')
topic_id = os.environ.get('pubsub_topic_id')
bucket_name = os.environ.get('gcs_bucket_name')
path_prefix = os.environ.get('gcs_path_prefix')
print('#### project:{project}, topic:{topic}, bucket:{gcs_bucket}, prefix:{prefix}'.format(
    project=project_id, topic=topic_id, gcs_bucket=bucket_name, prefix=path_prefix))


def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    print('#### filetype:{file_type}, filename:{file}'.format(
        file_type=event['contentType'], file=event['name']))

    if event['contentType'] == 'text/csv' and event['name'].find(path_prefix):
        gcs_file_path = 'gs://'+bucket_name+'/'+event['name']
        print('#### Processing file: {file}'.format(file=gcs_file_path))
        df = pd.read_csv(filepath_or_buffer=gcs_file_path,
                         storage_options={'token': 'cloud'})
        print(df.head(n=10))


# def hello_gcs(event, context):
#     """Triggered by a change to a Cloud Storage bucket.
#     Args:
#          event (dict): Event payload.
#          context (google.cloud.functions.Context): Metadata for the event.
#     """
#     file = event
#     print(f"Processing file: {file['name']}.")
