import os
import logging
from sys import prefix
import pandas as pd
from google.cloud import storage
from google.cloud import pubsub_v1


# main method
def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    project_id = os.environ.get('project_id')
    topic_id = os.environ.get('pubsub_topic_id')
    bucket_name = os.environ.get('gcs_bucket_name')
    path_prefix = os.environ.get('gcs_path_prefix')

    logger.info('#### filetype:{file_type}, filename:{file}'.format(
        file_type=event['contentType'], file=event['name']))

    logger.info('#### Project:{project}, Topic:{topic}, Bucket:{bucket}, Prefix:{prefix}'.format(
        project=project_id, topic=topic_id, bucket=bucket_name, prefix=path_prefix
    ))

    if event['contentType'] == 'text/csv' and event['name'].find(path_prefix):
        gcs_file_path = 'gs://'+bucket_name+'/'+event['name']
        logger.info('#### Processing file: {file}'.format(file=gcs_file_path))

        # read csv file
        df = pd.read_csv(filepath_or_buffer=gcs_file_path,
                         storage_options={'token': 'cloud'})
        logger.info(df.head(n=10))

        # convert to json string and encode
        json_str = df.to_json(orient='records')
        json_encode = json_str.encode('utf-8')
        logger.info(json_encode)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project=project_id, topic=topic_id)
        future = publisher.publish(topic=topic_path, data=json_encode)
        logger.info(future.result())
        logger.info('#### Data published to topic: {pubsub_topic}'.format(
            pubsub_topic=topic_path))


# def hello_gcs(event, context):
#     """Triggered by a change to a Cloud Storage bucket.
#     Args:
#          event (dict): Event payload.
#          context (google.cloud.functions.Context): Metadata for the event.
#     """
#     file = event
#     print(f"Processing file: {file['name']}.")
