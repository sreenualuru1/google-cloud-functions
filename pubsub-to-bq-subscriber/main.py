import os
import json
import pandas as pd
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# required details
project_id = os.environ.get('project_id')
subscription_id = os.environ.get('subscription_id')
bq_table_name = os.environ.get('bq_table_name')
bq_table_schema = os.environ.get('bq_table_schema')

# Number of seconds the subscriber should listen for messages
timeout = 10.0


# data processing
def process_data(message: pubsub_v1.subscriber.message.Message) -> None:
    message.ack()
    print(message)

    # decode json string
    json_decode = message.data.decode('utf-8')
    json_data = json.loads(json_decode)

    # create dataframe
    new_df = pd.read_json(json.dumps(json_data))
    # print(new_df.head(n=10))

    # dump into bq table
    new_df.to_gbq(destination_table=str(bq_table_name),
                  project_id=project_id,
                  chunksize=100,
                  if_exists='append',
                  table_schema=bq_table_schema)
    print('Data pushed to BQ table successfully')


# main method
def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project=project_id, subscription=subscription_id)
    streaming_pull = subscriber.subscribe(
        subscription=subscription_path, callback=process_data)
    print('Listining for messages on {subscription}...\n'.format(
        subscription=subscription_path))

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull.result(timeout=timeout)
        except TimeoutError:
            streaming_pull.cancel()  # Trigger the shutdown.
            streaming_pull.result()  # Block until the shutdown is complete.