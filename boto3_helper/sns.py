import logging
import traceback

from botocore.exceptions import ClientError


class SNSObject:
    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for SNSObject : {self.__dict__}")

    def enable_sns_notification_for_bucket(self, s3_resource, bucket_name: str, region_name: str, prefix: str,
                                           suffixes: set, sns_topic_arn: dict, events: list = None, **kwargs):

        events = ['s3:ObjectCreated:*'] if events is None else events

        topic_arn = sns_topic_arn.get(region_name)
        if not topic_arn:
            logging.error(f"SNS topic not configured for region: {region_name}",
                          extra={"bucket_name": bucket_name, "region_name": region_name})
            return False

        if s3_resource is None:
            logging.error(f"Require s3_resource to enable sns notification")
            return False

        try:
            bucket_notifications_enabled = s3_resource.BucketNotification(bucket_name)
            topic_configurations = bucket_notifications_enabled.topic_configurations
        except Exception as exception:
            logging.error(f"Error occured in getting topic_configurations"
                          f" for bucket_name:{bucket_name}: {traceback.format_exc()}")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
            return False

        suffixes_available = SNSObject.get_available_suffixes(prefix, topic_configurations)
        suffixes_unavailable = suffixes - suffixes_available
        topic_configurations = [] if not topic_configurations else topic_configurations
        for suffix in suffixes_unavailable:
            filter_rules = [{"Name": "Prefix", "Value": prefix}, {"Name": "Suffix", "Value": suffix}]
            topic_configurations.append({
                'TopicArn': topic_arn,
                'Events': events,
                "Filter": {
                    "Key": {
                        "FilterRules": filter_rules
                    }
                }
            })

        try:
            logging.info(f"Updating SNS notification for bucket_name: {bucket_name}, "
                         f"prefixes: {prefix}, suffixes: {suffixes_unavailable}")
            bucket_notifications_enabled.put(
                NotificationConfiguration={
                    'TopicConfigurations': topic_configurations,
                    'QueueConfigurations': bucket_notifications_enabled.queue_configurations \
                        if bucket_notifications_enabled.queue_configurations else [],
                    'LambdaFunctionConfigurations': bucket_notifications_enabled.lambda_function_configurations \
                        if bucket_notifications_enabled.lambda_function_configurations else [],
                    'EventBridgeConfiguration': bucket_notifications_enabled.event_bridge_configuration \
                        if bucket_notifications_enabled.event_bridge_configuration else {}
                }
            )
        except ClientError as exception:
            logging.error(f"{exception} Can't update the SNS notification for bucket_name: {bucket_name},"
                          f" prefixes: {prefix}, suffixes: {suffixes_unavailable}"
                          f"{traceback.format_exc()}")
            if kwargs.get("raise_exception") or self.__dict__.get('raise_exception'):
                raise exception
            return False
        except Exception as exception:
            logging.error(
                f"{exception} Uncaught Exception in updating the SNS notification for bucket_name: {bucket_name},"
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
