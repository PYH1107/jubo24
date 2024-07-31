import datetime
import json
import logging

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

DEFAULT_PROJECT_ID = 'jubo-ai'
DEFAULT_ENCODING = 'UTF-8'


def _make_topic_path(topic_id):
    return f"projects/{DEFAULT_PROJECT_ID}/topics/{topic_id}"


def _make_sub_path(sub_id):
    return f"projects/{DEFAULT_PROJECT_ID}/subscriptions/{sub_id}"


class Publisher:
    """
    Example Usage:
        pub = Publisher(mode='test') # publisher will connect to gcp
        topic_name = 'mock_name'
        pub.create_topic(topic_name)
        # topic_path = 'projects/jubo-ai/topics/jubo-datahub-test-mock_name'
        pub.publish(topic_name, {"test_data": 1})
        pub.delete_topic(topic_name)
    """

    def __init__(self, mode='test'):
        self.pub_prefix = f"jubo-datahub-{mode}"
        self.publisher = pubsub_v1.PublisherClient()

    def _topic_name_to_path(self, topic_name):
        topic_id = f"{self.pub_prefix}-{topic_name}"
        topic_path = _make_topic_path(topic_id)
        return topic_path

    def create_topic(self, topic_name):
        topic_path = self._topic_name_to_path(topic_name)
        try:
            self.publisher.create_topic(name=topic_path)
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                  f" Created topic: {topic_path}")
        except AlreadyExists:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                  f" [WARNING] Topic \"{topic_path}\" already exists.")

    def delete_topic(self, topic_name):
        topic_path = self._topic_name_to_path(topic_name)
        self.publisher.delete_topic(request={"topic": topic_path})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
              f" Delete topic: {topic_path}")

    def publish(self, topic_name, data):
        topic_path = self._topic_name_to_path(topic_name)
        if isinstance(data, dict):
            message = json.dumps(data).encode(DEFAULT_ENCODING)
            self.publisher.publish(topic_path, message)
            logging.debug(
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [Info] message sent, message: {message}")
        else:
            raise TypeError(f"Unsupported dtype: {data}")


class Subscriber:
    def __init__(self, topic_name, mode='test', sub_name='sub'):
        self.sub_prefix = f"jubo-datahub-{mode}"
        topic_id = f"{self.sub_prefix}-{topic_name}"
        topic_path = _make_topic_path(topic_id)

        sub_id = f"{topic_id}-{sub_name}"
        self.sub_path = _make_sub_path(sub_id)
        self.subscriber = pubsub_v1.SubscriberClient()

        try:
            self.subscriber.create_subscription(
                name=self.sub_path, topic=topic_path)
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                  f" Created subscription: {self.sub_path}")
        except AlreadyExists:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                  f' [WARNING] Subscription already exists, sub_path: {self.sub_path}')

    def pull(self):
        response = self.subscriber.pull(
            request={"subscription": self.sub_path,
                     "max_messages": 1},
        )
        # only support batch=1
        received_message = response.received_messages[0]
        data = received_message.message.data
        data = json.loads(data.decode(DEFAULT_ENCODING))
        ack_id = received_message.ack_id
        print(f"Received {ack_id}: {data}.")
        return data, ack_id

    def ack(self, ack_id):
        self.subscriber.acknowledge(
            request={"subscription": self.sub_path, "ack_ids": [ack_id]}
        )
        print(f"Acked {ack_id}!!")

    def clean(self):
        self.subscriber.delete_subscription(
            request={"subscription": self.sub_path})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
              f" Delete subscription: {self.sub_path}")
