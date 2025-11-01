from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import requests
import time

# Configuration
UVA_ID = "njd5rd"
platform = "airflow"
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/njd5rd"
submit_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
scatter_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"

# SQS client
sqs = boto3.client('sqs', region_name='us-east-1')

# Python callables for tasks

def populate_queue(**kwargs):
    """Call scatter API to populate the SQS queue"""
    logger = kwargs['ti'].log
    try:
        logger.info(f"Calling scatter API: {scatter_url}")
        response = requests.post(scatter_url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        logger.info("Queue populated successfully.")
        logger.info(f"New SQS URL: {payload.get('sqs_url')}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error populating queue: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in populate_queue: {e}")
        raise


def fetch_and_process_messages(**kwargs):
    """Fetch messages in batches, delete them, reconstruct phrase"""
    logger = kwargs['ti'].log
    all_messages = []
    max_attempts = 100
    attempt = 0

    try:
        while attempt < max_attempts:
            attempt += 1
            try:
                response = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        'ApproximateNumberOfMessages',
                        'ApproximateNumberOfMessagesNotVisible',
                        'ApproximateNumberOfMessagesDelayed'
                    ]
                )
                attrs = response['Attributes']
                total_messages = sum(int(attrs[key]) for key in attrs)
                logger.info(f"Attempt {attempt}: Total messages = {total_messages}")

                if total_messages == 0:
                    logger.info("No messages left to process.")
                    break

                # Fetch up to 10 messages
                fetched = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    MessageAttributeNames=['All']
                ).get('Messages', [])

                if not fetched:
                    logger.info("No messages ready yet. Waiting 5 seconds...")
                    time.sleep(5)
                    continue

                for msg in fetched:
                    try:
                        order_no = int(msg['MessageAttributes']['order_no']['StringValue'])
                        word = msg['MessageAttributes']['word']['StringValue']
                        receipt_handle = msg['ReceiptHandle']
                        all_messages.append((order_no, word, receipt_handle))

                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                        logger.info(f"Processed message {order_no}: {word}")
                    except KeyError as e:
                        logger.error(f"Missing expected attribute in message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

            except Exception as e:
                logger.error(f"Error fetching messages (attempt {attempt}): {e}")
                time.sleep(5)

        # Reconstruct phrase
        all_messages.sort(key=lambda x: x[0])
        phrase = " ".join(word for _, word, _ in all_messages)
        logger.info(f"Reconstructed Phrase: {phrase}")

        return phrase

    except Exception as e:
        logger.error(f"Unexpected error in fetch_and_process_messages: {e}")
        raise


def send_solution(ti, **kwargs):
    """Send reconstructed phrase to submit queue"""
    logger = ti.log
    try:
        phrase = ti.xcom_pull(task_ids='fetch_and_process_messages')

        if not phrase:
            logger.warning("No phrase to submit.")
            return

        response = sqs.send_message(
            QueueUrl=submit_queue_url,
            MessageBody="DP2 Submission",
            MessageAttributes={
                'uvaid': {'DataType': 'String', 'StringValue': UVA_ID},
                'platform': {'DataType': 'String', 'StringValue': platform},
                'phrase': {'DataType': 'String', 'StringValue': phrase}
            }
        )
        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code == 200:
            logger.info("Message submitted successfully.")
        else:
            logger.error(f"Failed to submit message. Status code: {status_code}")

    except Exception as e:
        logger.error(f"Unexpected error in send_solution: {e}")
        raise


# Define DAG
default_args = {
    'owner': 'zoe',
    'start_date': datetime(2025, 10, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'dp2_sqs_airflow',
    default_args=default_args,
    description='DP2 SQS workflow using Airflow',
    schedule=None,
    catchup=False,
    tags=['dp2', 'sqs'],
) as dag:

    t1 = PythonOperator(
        task_id='populate_queue',
        python_callable=populate_queue
    )

    t2 = PythonOperator(
        task_id='fetch_and_process_messages',
        python_callable=fetch_and_process_messages
    )

    t3 = PythonOperator(
        task_id='send_solution',
        python_callable=send_solution
    )

    t1 >> t2 >> t3
