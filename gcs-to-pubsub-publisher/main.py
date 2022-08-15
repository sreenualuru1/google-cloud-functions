import os
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import pubsub_v1

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

project_id = os.environ.get('project_id')
topic_id = os.environ.get('topic_id')
bucket_name = os.environ.get('bucket_name')
path_prefix = os.environ.get('path_prefix')


# main method
def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    file = event['name']
    logger.info('Processing file: {filename}'.format(filename=file['name']))
    if file.startswith(path_prefix) and file.endswith('.csv'):
        gcs_file_path = 'gs://'+bucket_name+'/'+event['name']
        logger.info('Blob: {blob}'.format(blob=gcs_file_path))

        # read csv file
        df = pd.read_csv(filepath_or_buffer=gcs_file_path)
        # logger.info(df.head(n=10))

        # convert to json string and encode
        json_str = df.to_json(orient='records')
        json_encode = json_str.encode('utf-8')
        # logger.info(json_encode)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project=project_id, topic=topic_id)
        future = publisher.publish(topic=topic_path, data=json_encode)
        logger.info(future.result())
        logger.info('Data published to topic: {pubsub_topic}'.format(
            pubsub_topic=topic_path))
    else:
        logger.info('Not a csv file received.')

