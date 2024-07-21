import json
import logging
import traceback

from botocore.exceptions import ClientError

try:
    from alpha_library.boto3_helper.resource import Resource
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.resource import Resource


class SQSObject:
    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for SQSObject : {self.__dict__}")

    def get_sqs_by_name(self, queue_name, region_name="us-east-1", **kwargs):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        """
        sqs = Resource(aws_details=self.aws_details).return_resource('sqs', region_name=region_name)
        queue = None
        try:
            queue = sqs.get_queue_by_name(QueueName=queue_name)
            logging.info("Got queue '%s' with URL=%s", queue, queue.url)
        except ClientError as exception:
            logging.exception(f"Couldn't get queue named {queue_name}: {traceback.format_exc()} ")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
        return queue

    def receive_messages_sqs(self, queue, max_number_of_messages_to_receive, message_receive_wait_time,
                             log_minimal=False):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        Receive a batch of messages in a single request from an SQS queue.
        Variables expected in kwargs

        - max_number_of_messages_to_receive: either the polling waits for the message_receive_wait_time to get over,
        or return as soon as max_number_of_messages_to_receive messages are collected

        -  message_receive_wait_time: The duration (in seconds) for which the call waits for a message to arrive in the queue before returning.
         If a message is available, the call returns sooner than WaitTimeSeconds .
         If no messages are available and the wait time expires, the call returns successfully with an empty list of messages.

        :return: The list fof Message objects received. These each contain the body
                 of the message and metadata and custom attribute
        """
        try:
            if queue is not None:
                messages = queue.receive_messages(
                    AttributeNames=['All'],
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=max_number_of_messages_to_receive,
                    WaitTimeSeconds=message_receive_wait_time
                )
                for msg in messages:
                    if log_minimal:
                        logging.debug("Received message: %s: %s", msg.message_id, msg.body,
                                      extra={"activity": "received", "message_id": msg.message_id})
                        continue
                    logging.info("Received message: %s: %s", msg.message_id, msg.body,
                                 extra={"activity": "received", "message_id": msg.message_id})
            else:
                logging.error(f"Need SQS object to recieve messages, passed: {queue}")
                return []
        except ClientError as error:
            logging.exception(f"Couldn't receive messages from queue: {traceback.format_exc()}")
            return []
        else:
            return messages

    def send_message(self, queue, message):
        """Send message helper"""
        try:
            if isinstance(message, dict):
                message = json.dumps(message)
            else:
                message = str(message)
            if queue is not None:
                messages = queue.send_message(MessageBody=message)
            else:
                logging.error(f"Need SQS object to send message, passed: {queue}")
                return []
        except ClientError as error:
            logging.exception(f"Couldn't send message to queue: {traceback.format_exc()}")
            return []
        else:
            return messages

    def delete_message(self, message):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        Delete a message from a queue. Clients must delete messages after they
        are received and processed to remove them from the queue.

        :param message: The message to delete. The message's queue URL is contained in
                        the message's metadata.
        :return: None
        """
        try:
            message.delete()
            logging.info("Deleted message: %s", message.message_id)
        except ClientError as error:
            logging.exception("Couldn't delete message: %s", message.message_id)

    def enable_sqs_notification_for_bucket(self, s3_resource, bucket_name: str, region_name: str, prefix: str,
                                           suffixes: set, queue_arn: str, events: list = None, **kwargs):
        """
        For a given S3 bucket, enable sqs notifications
        - s3_resource: resource object of the s3 account which owns the bucket
        - bucket_name:  the bucket name for which we want to enable sqs notifications
        - prefix: the path existing inside the bucket for which we want to start generating sqs notification
        - suffixes: set of suffixes for which we want to enable notification,
        this can be file extension for example, like .csv .json etc.
        """

        if s3_resource is None:
            logging.error(f"Require s3_resource to enable sqs notification")
            return False
        events = ['s3:ObjectCreated:*'] if events is None else events
        try:
            bucket_notifications_enabled = s3_resource.BucketNotification(bucket_name)
            queue_configurations = bucket_notifications_enabled.queue_configurations
        except Exception as exception:
            logging.error(
                f"Error occured in getting topic_configurations for bucket_name:{bucket_name}: {traceback.format_exc()}")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
            return False

        suffixes_available = SQSObject.get_available_suffixes(prefix, queue_configurations)
        suffixes_unavailable = suffixes - suffixes_available
        queue_configurations = [] if not queue_configurations else queue_configurations
        for suffix in suffixes_unavailable:
            filter_rules = [{"Name": "Prefix", "Value": prefix}, {"Name": "Suffix", "Value": suffix}]
            queue_configurations.append({
                'QueueArn': queue_arn,
                'Events': events,
                "Filter": {
                    "Key": {
                        "FilterRules": filter_rules
                    }
                }
            })

        try:
            logging.info(
                f"Updating SQS notification for bucket_name: {bucket_name}, prefixes: {prefix}, suffixes: {suffixes_unavailable}")
            bucket_notifications_enabled.put(
                NotificationConfiguration={
                    'TopicConfigurations': bucket_notifications_enabled.topic_configurations if bucket_notifications_enabled.topic_configurations else [],
                    'QueueConfigurations': queue_configurations,
                    'LambdaFunctionConfigurations': bucket_notifications_enabled.lambda_function_configurations if bucket_notifications_enabled.lambda_function_configurations else [],
                    'EventBridgeConfiguration': bucket_notifications_enabled.event_bridge_configuration if bucket_notifications_enabled.event_bridge_configuration else {}
                })
        except ClientError as exception:
            logging.error(f"{exception} Can't update the SQS notification for bucket_name: {bucket_name},"
                          f" prefixes: {prefix}, suffixes: {suffixes_unavailable}"
                          f"{traceback.format_exc()}")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
            return True
        except Exception as exception:
            logging.error(
                f"{exception} Uncaught Exception in updating the SQS notification for bucket_name: {bucket_name},"
                f" prefixes: {prefix}, suffixes: {suffixes_unavailable}"
                f"{traceback.format_exc()}")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
            return False
        return True

    @staticmethod
    def get_available_suffixes(prefix, topic_configurations):
        topic_configurations = [] if not topic_configurations else topic_configurations
        suffixes_available = set()
        for topic_configuration in topic_configurations:
            prefix_in_rule = None
            suffix_in_rule = set()
            for rules in topic_configuration.get("Filter", {}).get("Key", {}).get("FilterRules", []):
                if rules["Name"] == "Prefix":
                    prefix_in_rule = rules["Value"]
                else:
                    suffix_in_rule.add(rules["Value"])
            if prefix_in_rule and prefix_in_rule == prefix:
                suffixes_available.update(suffix_in_rule)
        return suffixes_available

    def get_number_of_messages_in_the_queue(self, queue=None, queue_name=None, **kwargs):
        if queue is None and queue_name is None:
            return None
        messages_count = None
        try:
            if queue is None:
                queue = self.get_sqs_by_name(queue_name, raise_exception=True)
                if queue is None:
                    logging.error(f"Couldn't get sqs object, unable to get the count, returning 0")
                    return 0
            logging.info(f"Getting the number of messages in the queue")
            messages_count = int(queue.attributes["ApproximateNumberOfMessages"]) + int(
                queue.attributes["ApproximateNumberOfMessagesNotVisible"]) + int(
                queue.attributes["ApproximateNumberOfMessagesDelayed"])
        except Exception as exception:
            logging.info(f"Uncaught exception in getting the number of messages in the queue: {traceback.format_exc()}")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
        return messages_count
