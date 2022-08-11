import os
import logging

# from google.cloud import storage
# from google.oauth2 import service_account

project_id = os.environ.get('project_id')
topic_id = os.environ.get('pubsub_topic_id')
logging.log(f'project_id:{project_id}, topic_id:{topic_id}')

def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    gcs_path_prefix = 'test-sales-data'
    logging.log(event['contentType'], event['name'])
    if event['contentType'] == 'text/csv' and event['name'].find(gcs_path_prefix):
        logging.log(f"Processing file: {event['name']}.")




