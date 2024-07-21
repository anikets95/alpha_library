#!/usr/bin/python3
# coding= utf-8
"""
This scripts creates boto3 session based Assumed Role Credentials
"""
import boto3
import datetime
from botocore import credentials, session
from dateutil.tz import tzlocal


def assumed_role_session(role_arn, base_session, region_name="us-east-1", external_id=None):
    """
    This method is ripped of from below url to get AWS session using AssumeRoleProvider
    xref: https://stackoverflow.com/questions/44171849/aws-boto3-assumerole-example-which-includes-role-usage\
    :param region_name: region name for service (default : us-east-1)
    :param role_arn: URI for role, example : "arn:aws:iam::123456789:role/role-crossaccount-xyz"
    :param base_session: Base Session for which ARP has been provided
    :param external_id: external_id
    :return: Boto3 session with ARP
    """
    fetcher = credentials.AssumeRoleCredentialFetcher(
        client_creator=base_session.create_client,
        source_credentials=base_session.get_credentials(),
        role_arn=role_arn,
        extra_args={'ExternalId': external_id} if external_id else None
    )
    creds = credentials.DeferredRefreshableCredentials(
        method="assume-role",
        refresh_using=fetcher.fetch_credentials,
        time_fetcher=lambda: datetime.datetime.now(tzlocal())
    )
    botocore_session = session.Session()
    botocore_session._credentials = creds
    return boto3.Session(botocore_session=botocore_session, region_name=region_name)
