#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over Azure clients
"""
import logging
import os
import tempfile
import traceback

from azure.core.credentials import AzureNamedKeyCredential, AzureSasCredential
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

tempfile.tempdir = "/tmp"


class AzureStorageClient(object):
    """
    This class provide storage client for Azure
    """

    def __init__(self, **kwargs):

        self.account_url = None
        self.azure_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for Client : {self.__dict__}")

    def return_blob_service_client(self) -> BlobServiceClient:
        """
        Return Storage client based on the input provided
        :return: BlobServiceClient to interact with container
        """
        client = None
        try:
            # AzureNamedKeyCredential
            if {"name", "key"}.issubset(set(self.azure_details.keys())):

                credential = AzureNamedKeyCredential(name=self.azure_details.get("name"),
                                                     key=self.azure_details.get("key"))

            # AzureSasCredential
            elif self.azure_details.get("signature"):

                credential = AzureSasCredential(signature=self.azure_details.get("signature"))

            # Most default option
            else:
                credential = DefaultAzureCredential()

            # Checking if AZURE_STORAGE_CONNECTION_STRING has been mentioned in the environment
            if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
                client = BlobServiceClient.from_connection_string(
                    conn_str=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"), credential=credential)
            else:
                client = BlobServiceClient(account_url=self.account_url, credential=credential)

        except TypeError as error:
            logging.error(f"Type error probably due to incorrect input to method: {error}")
        except BaseException:
            logging.error(f"Uncaught exception in blob_service_client : {traceback.format_exc()}")
        return client

    def return_blob_client(self):
        """
        Return Storage client based on the input provided
        :return:
        """
        client = None
        try:
            # AzureNamedKeyCredential
            if {"name", "key"}.issubset(set(self.azure_details.keys())):

                credential = AzureNamedKeyCredential(name=self.azure_details.get("name"),
                                                     key=self.azure_details.get("key"))

            # AzureSasCredential
            elif self.azure_details.get("signature"):

                credential = AzureSasCredential(signature=self.azure_details.get("signature"))

            # Most default option
            else:
                credential = DefaultAzureCredential()

            # Checking if AZURE_STORAGE_CONNECTION_STRING has been mentioned in the environment
            if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
                client = BlobServiceClient.from_connection_string(
                    conn_str=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"), credential=credential)
            else:
                client = BlobClient(account_url=self.account_url, credential=credential)

        except TypeError as error:
            logging.error(f"Type error probably due to incorrect input to method: {error}")
        except BaseException:
            logging.error(f"Uncaught exception in blob_service_client : {traceback.format_exc()}")
        return client

    def return_container_client(self, container_name):
        """
        Return Storage client based on the input provided
        :arg: container_name : Container Name
        :return:
        """
        client = None
        try:
            # AzureNamedKeyCredential
            if {"name", "key"}.issubset(set(self.azure_details.keys())):

                credential = AzureNamedKeyCredential(name=self.azure_details.get("name"),
                                                     key=self.azure_details.get("key"))

            # AzureSasCredential
            elif self.azure_details.get("signature"):

                credential = AzureSasCredential(signature=self.azure_details.get("signature"))

            # Most default option
            else:
                credential = DefaultAzureCredential()

            # Checking if AZURE_STORAGE_CONNECTION_STRING has been mentioned in the environment
            if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
                client = BlobServiceClient.from_connection_string(
                    conn_str=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"), credential=credential)
            else:
                client = ContainerClient(account_url=self.account_url, container_name=container_name,
                                         credential=credential)

        except TypeError as error:
            logging.error(f"Type error probably due to incorrect input to method: {error}")
        except BaseException:
            logging.error(f"Uncaught exception in blob_service_client : {traceback.format_exc()}")
        return client

