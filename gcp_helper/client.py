#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over GCP clients
"""
import json
import logging
import os
import tempfile
import traceback
from google.auth.exceptions import DefaultCredentialsError
from google.auth import default
from google.auth.credentials import Credentials
from google.cloud.storage import Client

tempfile.tempdir = "/tmp"


class StorageClient(object):
    """
    This class provide storage client for GCS
    """

    def __init__(self, **kwargs):
        self.in_gcp_env = None
        self.sa_json_data = None

        self.__dict__.update(kwargs)

        self.sa_json_file = None
        logging.debug(f"Instance variables for StorageClient : {self.__dict__}")

    def __del__(self):
        if self.sa_json_data and self.sa_json_file:
            self.sa_json_file.close()

    def return_client(self):
        """
        Return Storage client based on the input provided
        :return:
        """
        client = None
        try:
            if self.sa_json_data:
                self.sa_json_file = tempfile.NamedTemporaryFile()
                with open(self.sa_json_file.name, 'w+') as file:
                    file.write(json.dumps(self.sa_json_data))
                client = Client.from_service_account_json(self.sa_json_file.name)
            # Checking if GOOGLE API Token has been mentioned in the environment
            elif "GOOGLE_API_TOKEN" in os.environ:
                credentials = Credentials(token=os.environ["GOOGLE_API_TOKEN"])
                client = Client(credentials=credentials)
            # Checking if the code is running in GCS environment
            elif self.in_gcp_env:
                credentials, _ = default()
                client = Client(credentials=credentials)
            # Most default option
            else:
                client = Client()

        except DefaultCredentialsError as error:
            logging.error(f"Source credential error: {error}")
        except BaseException:
            logging.error(f"Uncaught exception in client.py : {traceback.format_exc()}")
        return client

