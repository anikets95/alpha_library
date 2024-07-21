#!/usr/bin/python3
# coding= utf-8
"""
This scripts provides wrapper over AWS Session
"""
import boto3
import logging
import traceback

try:
    from alpha_library.boto3_helper.arn_session import assumed_role_session
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.arn_session import assumed_role_session


class Session(object):
    """
        This Class handles creation of AWS Session
    """

    def __init__(self, **kwargs):
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for Session : {self.__dict__}")

    def return_session(self):
        """
        This method creates AWS Session
        """
        session = None

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
                    session = assumed_role_session(role_arn=self.aws_details["assigned_role_arn"],
                                                   base_session=base_session._session,
                                                   external_id=self.aws_details.get("external_id"))

                elif {"access_key", "secret_key", "region_name"}.issubset(set(self.aws_details.keys())):
                    # Created normal session in case of no ARN
                    session = boto3.session.Session(aws_access_key_id=self.aws_details["access_key"],
                                                    aws_secret_access_key=self.aws_details["secret_key"],
                                                    region_name=self.aws_details["region_name"])

                elif {"profile_name"}.issubset(set(self.aws_details.keys())):
                    # Created normal session in case of no ARN using profile name
                    session = boto3.session.Session(profile_name=self.aws_details["profile_name"])
            else:
                # Create normal session if no credentials are provided
                session = boto3.session.Session()

        except BaseException:
            logging.error(f"Uncaught exception in boto3_helper/session.py : {traceback.format_exc()}")
        return session

