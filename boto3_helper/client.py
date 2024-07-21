#!/usr/bin/python3
# coding= utf-8
"""
This scripts provides wrapper over AWS client
"""
import logging
import boto3

from botocore.config import Config

try:
    from alpha_library.boto3_helper.arn_session import assumed_role_session
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.arn_session import assumed_role_session


class Client(object):
    """
        This Class handles creation of AWS client
    """

    def __init__(self, **kwargs):
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for Client : {self.__dict__}")

    def return_client(self, service_name, **kwargs):
        """
        This method creates AWS clients
        :param service_name: Service Name for client
        """
        client = None

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

                    # Created client from ARN session
                    client = arn_session.client(service_name, **kwargs)

                elif {"access_key", "secret_key", "region_name"}.issubset(set(self.aws_details.keys())):
                    # Created normal clients in case of no ARN
                    client = boto3.client(service_name, aws_access_key_id=self.aws_details["access_key"],
                                          aws_secret_access_key=self.aws_details["secret_key"],
                                          region_name=self.aws_details["region_name"], **kwargs)

                elif {"profile_name"}.issubset(set(self.aws_details.keys())):
                    # Created normal clients in case of no ARN using profile name
                    client = boto3.client(service_name, profile_name=self.aws_details["profile_name"], **kwargs)
            else:
                    client = boto3.client(service_name)

        except BaseException:
            logging.exception("Uncaught exception in client.py")
        return client
