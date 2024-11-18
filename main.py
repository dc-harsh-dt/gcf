import json
import asyncio
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.api_core.exceptions import AlreadyExists

# Configuration - hardcoded values
PROJECT_ID = "project_id"
REQUEST_TOPIC_ID = "request-topic"
RESPONSE_TOPIC_ID = "response-topic"
RESPONSE_SUBSCRIPTION_ID = "response-subscription"
SERVICE_ACCOUNT_FILE = "path/to/your/service-account-file.json"  # path to your service account JSON

# Load credentials
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

# Initialize Pub/Sub clients with credentials
publisher = pubsub_v1.PublisherClient(credentials=credentials)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

# Fully qualified topic and subscription paths
REQUEST_TOPIC = publisher.topic_path(PROJECT_ID, REQUEST_TOPIC_ID)
RESPONSE_TOPIC = publisher.topic_path(PROJECT_ID, RESPONSE_TOPIC_ID)
RESPONSE_SUBSCRIPTION = subscriber.subscription_path(PROJECT_ID, RESPONSE_SUBSCRIPTION_ID)

async def create_topic(topic_path):
    """Create a Pub/Sub topic if it does not exist."""
    try:
        publisher.create_topic(name=topic_path)
        print(f"Created topic: {topic_path}")
    except AlreadyExists:
        print(f"Topic {topic_path} already exists.")

async def create_subscription(subscription_path, topic_path):
    """Create a Pub/Sub subscription if it does not exist."""
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
        print(f"Created subscription: {subscription_path}")
    except AlreadyExists:
        print(f"Subscription {subscription_path} already exists.")

async def setup_pubsub():
    """Ensure Pub/Sub topics and subscription are set up."""
    await create_topic(REQUEST_TOPIC)
    await create_topic(RESPONSE_TOPIC)
    await create_subscription(RESPONSE_SUBSCRIPTION, RESPONSE_TOPIC)

async def publish_request():
    """Publishes a message to the request topic to trigger Cloud Run function."""
    message = json.dumps({"name": "harsh"})
    future = publisher.publish(REQUEST_TOPIC, data=message.encode("utf-8"))
    print("Request published to request-topic:", message)
    await asyncio.wrap_future(future)

async def receive_response():
    """Listens to the response subscription for messages from the Cloud Run function."""
    print("Listening for responses on response-subscription...")

    # Callback function to handle the response
    def callback(message):
        print("Received response:", message.data.decode("utf-8"))
        message.ack()  # Acknowledge the message to remove it from the queue

    # Start receiving messages asynchronously
    future = subscriber.subscribe(RESPONSE_SUBSCRIPTION, callback=callback)

    # Keep the listener active for a period (e.g., 30 seconds)
    try:
        await asyncio.sleep(30)  # Adjust this duration as needed
    finally:
        future.cancel()

async def main():
    await setup_pubsub()  # Set up Pub/Sub topics and subscription if needed
    await publish_request()  # Publish request to trigger the Cloud Run function
    await receive_response()  # Wait and listen for response

# Run the async function
asyncio.run(main())
