#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over GCP storage
xref : https://medium.com/@erdoganyesil/read-file-from-google-cloud-storage-with-python-cf1b913bd134
"""
import logging
import traceback

import gcsfs
from google.api_core.exceptions import NotFound
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
class DisplayGSObject(object):
    """
    This class handles list of object data from GS
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for DisplayGSObject : {self.__dict__}")

    def object_content(self, **kwargs) -> bytes:
        """
        This method downloads file from local machine to gs and then prints it
        """
        try:
            object_address = f"gs://{kwargs['gs_details']['bucket_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {"client": StorageClient(sa_json_data=self.sa_json_data).return_client()}
            # The return data is in binary
            return open(object_address, "rb", transport_params=transport_params).read()

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def object_content_str(self, **kwargs) -> str:
        """
        This method downloads file from local machine to gs and then prints it
        """
        try:
            object_address = f"gs://{kwargs['gs_details']['bucket_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {"client": StorageClient(sa_json_data=self.sa_json_data).return_client()}
            # The return data is in string
            return open(object_address, "r", transport_params=transport_params).read()

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# Access GS
class GSObjectList(object):
    """
        This class creates dict of gs objects with some basic filter
    """

    def __init__(self, **kwargs):
        # Required variables to drive this Object
        self.sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for GSObjectList : {self.__dict__}")

    class Decorator:

        @staticmethod
        def filter_items(func):
            def wrapper(self, **kwargs):
                blobs = func(self, **kwargs)
                return [blob for blob in blobs if blob.size > 0]

            return wrapper

        @staticmethod
        def dict_creation(func):
            def wrapper(self, **kwargs):
                blobs = func(self, **kwargs)
                return {blob.name: {"Key": blob.name,
                                    "LastModified": blob.updated,
                                    "ETag": blob.etag,
                                    "Size": blob.size,
                                    "StorageClass": blob.storage_class,
                                    "Blob": blob} for blob in blobs}

            return wrapper

    @Decorator.dict_creation
    @Decorator.filter_items
    def check_contents_of_storage(self, **kwargs):

        blobs = None
        try:

            storage_client = StorageClient(sa_json_data=self.sa_json_data).return_client()
            blobs = storage_client.list_blobs(bucket_or_name=kwargs["gs_details"]["bucket_name"],
                                              prefix=kwargs.get("folder_to_check"))
        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

        return blobs

    def check_file_existence(self, key, **kwargs):
        """
        This function checks if a specific key exists in a bucket,
        it uses the reload from client module of gs to check if the key exists
        and returns a boolean indicating the existence of the key
        :structure of key (pathFromRoot/file_name.extension)
        :params required
        key: str(path of the object we are looking for)
        """
        try:
            storage_client = StorageClient(sa_json_data=self.sa_json_data).return_client(). \
                bucket(bucket_name=kwargs["gs_details"]["bucket_name"])
            blob = storage_client.blob(key)
            blob.reload()
            logging.info(f"Key {key} exists in the folder {kwargs['gs_details']['bucket_name']}")
            return True
        except NotFound:
            logging.info(f"Key {key} does not exist in the folder {kwargs['gs_details']['bucket_name']}")
        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error
        return False


# Copy
class CopyObjectFromLocalToGS(object):
    """
    This class provide interface to copy from local to GS
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.destination_sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyToGS : {self.__dict__}")

    def copy_to_destination_storage(self, **kwargs):
        """
        This method downloads file from local machine to gs
        """
        try:
            # TODO : HANDLE MULTIPART UPLOAD
            object_destination_address = f"gs://{kwargs['destination_gs_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            destination_transport_params = {
                "client": StorageClient(sa_json_data=self.destination_sa_json_data).return_client()
            }
            with open(object_destination_address, "wb",
                      transport_params=destination_transport_params) as f_write:
                # This expects data in bytes format
                f_write.write(kwargs["data"])

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def copy_to_destination_storage_str(self, **kwargs):
        """
        This method downloads file from local machine to gs
        """
        try:
            # TODO : HANDLE MULTIPART UPLOAD
            object_destination_address = f"gs://{kwargs['destination_gs_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            destination_transport_params = {
                "client": StorageClient(sa_json_data=self.destination_sa_json_data).return_client()
            }
            with open(object_destination_address, "w",
                      transport_params=destination_transport_params) as f_write:
                # This expects data in bytes format
                f_write.write(kwargs["data"])

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromGSToLocal(object):
    """
    This Class is to data handle from gs to local
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromGSToLocal : {self.__dict__}")

    def download_content(self, **kwargs):
        """
        This method downloads file from local machine to gs and then prints it
        """
        try:
            object_address = f"gs://{kwargs['gs_details']['bucket_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": StorageClient(sa_json_data=self.sa_json_data).return_client()
            }

            with open(object_address, "rb", transport_params=transport_params) as gs_file:
                with open(kwargs["local_file_path"], "wb") as local_file:
                    for line in gs_file:
                        local_file.write(line)

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromURLtoGS(object):
    """
    This class provide interface to copy object from url to gs
    """
    maximum_part_size = 5 * 1024 ** 3
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.destination_sa_json_data = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyFromURLtoGS : {self.__dict__}")

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
        This method downloads file from url to gs
        """
        try:

            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromURLtoGS.chunk_size
            object_destination_address = f"gs://{kwargs['destination_gs_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = kwargs.get("transport_params")
            destination_transport_params = {
                "client": StorageClient(sa_json_data=self.destination_sa_json_data).return_client(),
                "min_part_size": chunk_size
            }

            with open(kwargs["url"], "rb", transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromURLtoGS.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromGSToGS(object):
    """
    This class provide interface to copy object from one gs to another gs
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.source_sa_json_data = None
        self.destination_sa_json_data = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromGSToGS : {self.__dict__}")

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
        This method downloads file from one gs to another gs
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromGSToGS.chunk_size
            object_original_address = f"gs://{kwargs['source_gs_details']['bucket_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"gs://{kwargs['destination_gs_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": StorageClient(sa_json_data=self.source_sa_json_data).return_client()
            }

            destination_transport_params = {
                "client": StorageClient(sa_json_data=self.destination_sa_json_data).return_client(),
                "min_part_size": chunk_size
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromGSToGS.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromGSToS3(object):
    """
    This class provide interface to copy object from gs to s3
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.source_sa_json_data = None
        self.destination_aws_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromGSToS3 : {self.__dict__}")

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
        This method downloads file from one gs to another gs
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromGSToS3.chunk_size
            object_original_address = f"gs://{kwargs['source_gs_details']['bucket_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"s3://{kwargs['destination_s3_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": StorageClient(sa_json_data=self.source_sa_json_data).return_client()
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
                    for data_line in CopyObjectFromGSToS3.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromGSToAzure(object):
    """
    This class provide interface to copy object from gs to azure
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.source_sa_json_data = None
        self.destination_azure_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromGSToAzure : {self.__dict__}")

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
        This method downloads file from one gs to another azure
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromGSToAzure.chunk_size
            object_original_address = f"gs://{kwargs['source_gs_details']['bucket_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"azure://{kwargs['destination_container_details']['container_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": StorageClient(sa_json_data=self.source_sa_json_data).return_client()
            }

            destination_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.destination_azure_details).return_blob_service_client()
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromGSToAzure.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# GCS FS
class CopyObjectFromLocalToGCSFS(object):
    """
    This class provide interface to copy from local to GSCFS
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.destination_sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyToGCSFS : {self.__dict__}")

    def copy_to_destination(self, **kwargs):
        """
        This method downloads file from local machine to gs using gcsfs
        """

        object_destination_address = f"{kwargs['destination_gs_details']['bucket_name']}/" \
                                     f"{kwargs['object_destination_path']}"

        try:
            fs = gcsfs.GCSFileSystem(project=self.destination_sa_json_data["project_id"],
                                     token=self.destination_sa_json_data)

            fs.put(kwargs.get("source_file_path"), object_destination_address)

        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# Delete
class GSDeleteObject(object):
    """
        This class provide wrapper to delete GS objects
    """

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for GSDeleteObject : {self.__dict__}")

    def delete_from_storage(self, **kwargs):
        """
        This method delete objects from GS
        :param kwargs:
        :return:
        """
        try:
            storage_client = StorageClient(sa_json_data=self.sa_json_data).return_client()
            bucket = storage_client.bucket(bucket_name=kwargs["gs_details"]["bucket_name"])
            blob = bucket.blob(kwargs["object_path"])
            blob.delete()
        except BaseException as error:
            logging.error(f"Uncaught exception in gcs/storage.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

