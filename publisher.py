import google.auth
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

TOPIC_NAME = "rss-feed-fetch-requests"

credentials, project_id = google.auth.default()

publisher = pubsub_v1.PublisherClient(credentials=credentials)

project_path = publisher.project_path(project_id)
topic_path = publisher.topic_path(project_id, TOPIC_NAME)


def _topic_exists():
    try:
        publisher.get_topic(topic_path)
        return True
    except NotFound:
        return False


def publish(content):
    if not _topic_exists():
        publisher.create_topic(topic_path)
    if content:
        publisher.publish(topic_path, data=content)
        return True
    return False
