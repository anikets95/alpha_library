#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over Azure storage
"""
import logging
import traceback

from smart_open import open

try:
    from alpha_library.gcp_helper.client import StorageClient
    from alpha_library.azure_helper.client import AzureStorageClient
    from alpha_library.boto3_helper.client import Client

except ModuleNotFoundError:
    logging.info("Module called internally")
    from gcp_helper.client import StorageClient
    from azure_helper.client import AzureStorageClient
    from boto3_helper.client import Client


# Display
class DisplayAzureObject(object):
    """
    This class handles list of object data from Azure
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.azure_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for DisplayAzureObject : {self.__dict__}")

    def object_content(self, **kwargs) -> bytes:
        """
        This method downloads file from azure to local machine and then returns it
        """
        try:
            object_address = f"azure://{kwargs['container_details']['container_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.azure_details).return_blob_service_client()
            }
            # The return data is in binary
            return open(object_address, "rb", transport_params=transport_params).read()

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def object_content_str(self, **kwargs) -> str:
        """
        This method downloads file from local machine to azure and then prints it
        """
        try:
            object_address = f"azure://{kwargs['container_details']['container_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.azure_details).return_blob_service_client()
            }
            # The return data is in string
            return open(object_address, "r", transport_params=transport_params).read()

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# Access Azure Storage
class AzureObjectList(object):
    """
        This class creates dict of azure objects with some basic filter
    """
    # TODO : To be completed
    pass


# Copy
class CopyObjectFromLocalToAzure(object):
    """
    This class provide interface to copy from local to Azure
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.destination_azure_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyToAzure : {self.__dict__}")

    def copy_to_destination_storage(self, **kwargs):
        """
        This method downloads file to local machine from azure
        """
        try:
            # TODO : HANDLE MULTIPART UPLOAD
            object_destination_address = f"azure://{kwargs['destination_container_details']['container_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            destination_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.destination_azure_details).return_blob_service_client()
            }
            with open(object_destination_address, "wb",
                      transport_params=destination_transport_params) as f_write:
                # This expects data in bytes format
                f_write.write(kwargs["data"])

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def copy_to_destination_storage_str(self, **kwargs):
        """
        This method downloads file from local machine to azure
        """
        try:
            # TODO : HANDLE MULTIPART UPLOAD
            object_destination_address = f"azure://{kwargs['destination_container_details']['container_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            destination_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.destination_azure_details).return_blob_service_client()
            }
            with open(object_destination_address, "w",
                      transport_params=destination_transport_params) as f_write:
                # This expects data in bytes format
                f_write.write(kwargs["data"])

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromAzureToLocal(object):
    """
    This Class is to data handle from Azure to local
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.azure_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromAzureToLocal : {self.__dict__}")

    def download_content(self, **kwargs):
        """
        This method downloads file from local machine to Azure and then prints it
        """
        try:
            object_address = f"azure://{kwargs['container_details']['container_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.azure_details).return_blob_service_client()
            }

            with open(object_address, "rb", transport_params=transport_params) as azure_file:
                with open(kwargs["local_file_path"], "wb") as local_file:
                    for line in azure_file:
                        local_file.write(line)

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromURLtoAzure(object):
    """
    This class provide interface to copy object from url to azure
    """
    maximum_part_size = 5 * 1024 ** 3
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.destination_azure_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyFromURLtoAzure : {self.__dict__}")

    @staticmethod
    def read_in_chunks(file_object, chunk_size):
        """
        Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k.
        xref: https://dataroz.com/stream-large-files-between-s3-and-gcs-python/
        """
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def copy_from_source_url_to_destination_storage(self, **kwargs):
        """
        This method downloads file from url to azure
        """
        try:

            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromURLtoAzure.chunk_size
            object_destination_address = f"azure://{kwargs['destination_container_details']['container_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = kwargs.get("transport_params")
            destination_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.destination_azure_details).return_blob_service_client(),
                "min_part_size": chunk_size
            }

            with open(kwargs["url"], "rb", transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromURLtoAzure.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromAzureToAzure(object):
    """
    This class provide interface to copy object from one azure to another azure
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.source_account_url = None
        self.destination_account_url = None
        self.source_azure_details = None
        self.destination_azure_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromAzureToAzure : {self.__dict__}")

    @staticmethod
    def read_in_chunks(file_object, chunk_size):
        """
        Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k.
        xref: https://dataroz.com/stream-large-files-between-gs-and-gcs-python/
        """
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def copy_from_source_to_destination_storage(self, **kwargs):
        """
        This method downloads file from one azure to another azure
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromAzureToAzure.chunk_size
            object_original_address = f"azure://{kwargs['source_container_details']['container_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"azure://{kwargs['destination_container_details']['container_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": AzureStorageClient(account_url=self.source_account_url,
                                             azure_details=self.source_azure_details).return_blob_service_client()
            }

            destination_transport_params = {

                "client": AzureStorageClient(account_url=self.destination_account_url,
                                             azure_details=self.destination_azure_details).return_blob_service_client(),
                "min_part_size": chunk_size
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromAzureToAzure.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromAzureToS3(object):
    """
    This class provide interface to copy object from azure to s3
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.source_azure_details = None
        self.destination_aws_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromAzureToS3 : {self.__dict__}")

    @staticmethod
    def read_in_chunks(file_object, chunk_size):
        """
        Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k.
        xref: https://dataroz.com/stream-large-files-between-gs-and-gcs-python/
        """
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def copy_from_source_to_destination_storage(self, **kwargs):
        """
        This method downloads file from one azure to another s3
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromAzureToS3.chunk_size
            object_original_address = f"azure://{kwargs['source_container_details']['container_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"s3://{kwargs['destination_s3_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.source_azure_details).return_blob_service_client()
            }

            destination_transport_params = {
                "client": Client(aws_details=self.destination_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("destination_endpoint_url")),
                "min_part_size": chunk_size
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromAzureToS3.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromAzureToGS(object):
    """
    This class provide interface to copy object from azure to s3
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.source_azure_details = None
        self.destination_aws_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromAzureToS3 : {self.__dict__}")

    @staticmethod
    def read_in_chunks(file_object, chunk_size):
        """
        Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k.
        xref: https://dataroz.com/stream-large-files-between-gs-and-gcs-python/
        """
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def copy_from_source_to_destination_storage(self, **kwargs):
        """
        This method downloads file from one azure to another s3
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromAzureToS3.chunk_size
            object_original_address = f"azure://{kwargs['source_container_details']['container_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"gs://{kwargs['destination_gs_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.source_azure_details).return_blob_service_client()
            }

            destination_transport_params = {
                "client": StorageClient(aws_details=self.destination_aws_details).return_client(),
                "min_part_size": chunk_size
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromAzureToS3.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in azure/blob.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# Azure FS
class CopyObjectFromLocalToAzureCFS(object):
    """
    This class provide interface to copy from local to AzureCFS
    """
    # TODO: to be completed !
    pass


# Delete
class AzureDeleteObject(object):
    """
        This class provide wrapper to delete Azure objects
    """
    # TODO: to be completed !
    pass


