import os
import logging
import pandas as pd
from google.cloud import storage

project_id = os.environ.get('project_id')
topic_id = os.environ.get('pubsub_topic_id')
bucket_name = os.environ.get('gcs_bucket')
logging.info('#### project:{project}, topic:{topic}, bucket:{gcs_bucket}'.format(
    project=project_id, topic=topic_id, gcs_bucket=bucket_name))


def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    gcs_path_prefix = 'test-sales-data'
    logging.info('#### filetype:{type}, filename:{name}'.format(
        event['contentType'], event['name']))

    if event['contentType'] == 'text/csv' and event['name'].find(gcs_path_prefix):
        gcs_file_path = 'gs://'+bucket_name+'/'+event['name']
        logging.info('#### Processing file: {file}'.format(file=gcs_file_path))
        df = pd.read_csv(filepath_or_buffer=gcs_file_path,
                         storage_options={'token': 'cloud'})
        logging.info(df.head(n=10))
