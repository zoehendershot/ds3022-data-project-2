import boto3
import requests
import time
from prefect import flow, task, get_run_logger

# Configuration
UVA_ID = "njd5rd"
sqs = boto3.client('sqs', region_name='us-east-1')
submit_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
platform = "prefect"
scatter_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/njd5rd"

# Tasks
@task
def populate_queue(UVA_ID):
    """Call the scatter API once per flow to populate the SQS queue"""
    logger = get_run_logger()
    try:
        logger.info(f"Calling scatter API for UVA ID: {UVA_ID}")
        payload = requests.post(scatter_url).json()
        print("Queue populated successfully.")
        print("New SQS URL:", payload['sqs_url'])
    except Exception as e:
        logger.error(f"Error populating queue: {e}")
        print(f"Error populating queue: {e}")

@task
def get_total_messages(queue_url):
    """Return total number of messages in the queue"""
    logger = get_run_logger()
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
        total = sum(int(attrs[key]) for key in attrs)
        logger.info(f"Queue counts: {attrs}, Total messages: {total}")
        return total
    except Exception as e:
        logger.error(f"Error getting queue attributes: {e}")
        return 0

@task
def fetch_messages(queue_url, max_messages=21):
    """Fetch messages from the queue"""
    logger = get_run_logger()
    try:
        max_messages = min(max_messages, 10)
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=1,
            MessageAttributeNames=['All']
        )
        messages = response.get('Messages', [])
        logger.info(f"Fetched {len(messages)} messages.")
        return messages
    except Exception as e:
        logger.error(f"Error fetching messages: {e}")
        return []

@task
def parse_message(msg):
    """Extract order_no, word, and receipt handle from a message."""
    try:
        order_no = int(msg['MessageAttributes']['order_no']['StringValue'])
        word = msg['MessageAttributes']['word']['StringValue']
        receipt_handle = msg['ReceiptHandle']
        return order_no, word, receipt_handle
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None, None, None

@task
def delete_message(queue_url, receipt_handle):
    """Delete a message from the queue after processing."""
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except Exception as e:
        print(f"Error deleting message: {e}")

@task
def reconstruct_phrase(parsed):
    """Sort messages and reconstruct the phrase"""
    parsed.sort(key=lambda x: x[0])
    phrase = " ".join(word for _, word, _ in parsed)
    print(f"\nReconstructed Phrase:\n{phrase}")
    return phrase

@task
def send_solution(UVA_ID, phrase, platform):
    """Submit the reconstructed phrase"""
    try:
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
            print("\nMessage submitted successfully. Status code: 200")
        else:
            print(f"\nFailed to submit message. Status code: {status_code}")
    except Exception as e:
        print(f"Error submitting message: {e}")

# Flow
@flow
def collect_messages_and_submit():
    logger = get_run_logger()
    logger.info("Starting Prefect flow...")

    # Step 1: Populate the queue once
    populate_queue(UVA_ID)
    time.sleep(2)  # short delay to allow messages to appear

    # Step 2: Collect and process messages
    all_messages = []
    while True:
        total_messages = get_total_messages(queue_url)
        if total_messages == 0:
            logger.info("No messages left to process.")
            break

        messages = fetch_messages(queue_url, 10)
        if not messages:
            logger.info("No messages ready yet. Waiting 10 seconds...")
            time.sleep(10)
            continue

        for msg in messages:
            order_no, word, receipt_handle = parse_message(msg)
            if order_no is not None:
                all_messages.append((order_no, word, receipt_handle))
                delete_message(queue_url, receipt_handle)

    # Step 3: Reconstruct and submit
    if all_messages:
        phrase = reconstruct_phrase(all_messages)
        send_solution(UVA_ID, phrase, platform)
    else:
        logger.info("No messages collected.")

# Run
if __name__ == "__main__":
    collect_messages_and_submit()# prefect flow goes here
