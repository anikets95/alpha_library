#!/usr/bin/python3
# coding= utf-8
"""
This scripts provides wrapper over AWS resource
"""
import boto3
import logging
import traceback

try:
    from alpha_library.boto3_helper.arn_session import assumed_role_session
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.arn_session import assumed_role_session


class Resource(object):
    """
        This Class handles creation of dynamo DB resource
    """

    def __init__(self, **kwargs):
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for Resource : {self.__dict__}")

    def return_resource(self, service_name, **kwargs):
        """
        This method creates AWS resource
        """

        resource = None

        try:
            if self.aws_details:
                # Checking if assigned_role_arn is provided or not
                if {"assigned_role_arn", "access_key", "secret_key", "region_name"}.issubset(
                        set(self.aws_details.keys())):

                    # Created base session
                    base_session = boto3.session.Session(
                        aws_access_key_id=self.aws_details["access_key"],
                        aws_secret_access_key=self.aws_details["secret_key"],
                        region_name=self.aws_details["region_name"])

                    # Created ARN session ( This is a boto3 session )
                    arn_session = assumed_role_session(role_arn=self.aws_details["assigned_role_arn"],
                                                       base_session=base_session._session,
                                                       external_id=self.aws_details.get("external_id"))

                    # Created resource from ARN session
                    resource = arn_session.resource(service_name, **kwargs)

                elif {"access_key", "secret_key", "region_name"}.issubset(set(self.aws_details.keys())):
                    # Created normal resource in case of no ARN
                    resource = boto3.Session(aws_access_key_id=self.aws_details["access_key"],
                                             aws_secret_access_key=self.aws_details["secret_key"],
                                             region_name=self.aws_details["region_name"]).resource(service_name,
                                                                                                   **kwargs)

                elif {"profile_name"}.issubset(set(self.aws_details.keys())):
                    # Created normal clients in case of no ARN using profile name
                    resource = boto3.Session(profile_name=self.aws_details["profile_name"]).resource(service_name,
                                                                                                     **kwargs)
            else:
                # Create normal resource if no credentials are provided
                resource = boto3.Session().resource(service_name, **kwargs)

        except BaseException:
            logging.error(f"Uncaught exception in boto3_helper/resource.py: {traceback.format_exc()}")
        return resource

