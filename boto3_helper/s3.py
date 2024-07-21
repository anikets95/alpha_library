# !/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over AWS S3
Good idea for multiple file download based on a prefix from S3 bucket
TODO : https://github.com/RaRe-Technologies/smart_open#iterating-over-an-s3-buckets-contents
"""
import logging
import traceback

import requests
import s3fs
from botocore.exceptions import ClientError, EndpointConnectionError
from smart_open import open

try:
    from alpha_library.boto3_helper.client import Client
    from alpha_library.gcp_helper.client import StorageClient
    from alpha_library.azure_helper.client import AzureStorageClient
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.client import Client
    from gcp_helper.client import StorageClient
    from azure_helper.client import AzureStorageClient


# Display
class DisplayS3Object(object):
    """
    This class handles content display of object from S3
    """

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for DisplayS3Object : {self.__dict__}")

    def object_content(self, **kwargs) -> bytes:
        """
        This method shows the content of a S3 object
        """
        try:
            object_address = f"s3://{kwargs['s3_details']['bucket_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": Client(aws_details=self.aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("endpoint_url"))
            }

            # The return data is in binary
            return open(object_address, "rb",
                        transport_params=transport_params).read()
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def object_content_str(self, **kwargs) -> str:
        """
        This method shows the content of a S3 object
        """
        try:
            object_address = f"s3://{kwargs['s3_details']['bucket_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": Client(aws_details=self.aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("endpoint_url"))
            }

            # The return data is in binary
            return open(object_address, "r",
                        transport_params=transport_params).read()
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def get_latest_file_from_path(self, **kwargs):
        """
            Get Latest file from s3 folder path based on extension
        """
        try:
            object_list = S3ObjectList(aws_details=self.aws_details).check_contents_of_s3(
                s3_details=kwargs['s3_details'],
                folder_to_check=kwargs['s3_details']['folder_path'])
            if object_list and "extensions" in kwargs:
                object_list = dict(filter(
                    lambda item: item[1].get('Key').endswith(tuple(kwargs["extensions"])),
                    object_list.items())
                )
            if not object_list:
                logging.error(
                    "No file found in input folder %s", kwargs['s3_details']['folder_path'])
                return None
            latest_file = max(object_list.items(), key=lambda item: item[1].get('LastModified'))
            latest_file = latest_file[0]
            return self.object_content(
                s3_details=kwargs['s3_details'],
                object_path=latest_file
            )
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error
            return None


# Access S3
class S3ObjectList(object):
    """
        This class creates dict of s3 objects with some basic filter
    """

    def __init__(self, **kwargs):

        # Required variables to drive this Object
        self.aws_details = None
        self.__dict__.update(kwargs)

        # Variable received later when method called
        self.s3_details = None
        self.s3_object_filter = None
        self.folder_to_check = ""
        self.delimiter = ""

        # S3 Client instance to use
        self.s3_instance = None

        # Object dictionary
        self.object_dict = dict()

        logging.debug(f"Instance variables for s3ObjectList : {self.__dict__}")

    def __object_filter(self, item: dict) -> bool:
        """
        This method filter based on the input provided in self.object_filter
        :param item: s3 item which needs to be checked
        :return: Boolean condition based on whether item passed the filter or not
        """
        # Default filter of checking whether the size is greater than 0B or not
        if item["Size"] > 0:
            if self.s3_object_filter:
                for key in self.s3_object_filter.keys():
                    if item[key] != self.s3_object_filter[key]:
                        return False
                return True
            else:
                logging.debug("No S3 object Filter")
                return True
        else:
            return False

    def __add_details_to_object_dict(self, list_objects_response: dict, last_modified=None):
        """
        This method recursively add data into object Dictionary
        :param list_objects_response: list_objects_response from boto3 s3 client
        """

        if list_objects_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            if "Contents" in list_objects_response.keys():
                for item in list_objects_response["Contents"]:
                    if self.__object_filter(item):
                        if last_modified and item["LastModified"] < last_modified:
                            continue
                        else:
                            object_name = item["Key"]
                            logging.debug(f"Object available in s3 in folder {self.folder_to_check} : {object_name}")
                            self.object_dict.update({object_name: item})

                # This check if the response received was truncated or not
                # If truncated then recursively call and update dictionary
                if list_objects_response["IsTruncated"]:
                    self.__add_details_to_object_dict(
                        self.s3_instance.list_objects_v2(
                            Bucket=self.s3_details["bucket_name"],
                            Prefix=self.folder_to_check,
                            Delimiter=self.delimiter,
                            ContinuationToken=list_objects_response["NextContinuationToken"]), last_modified)
        else:
            logging.error(
                f"Response from list_objects_v2 : {list_objects_response['ResponseMetadata']}")

    def check_file_existence(self, key, **kwargs):
        """
        This function checks if a specific key exists in a bucket,
        it uses the head_object from boto3 to check if the key exists
        and returns a boolean indicating the existence of the key
        :structure of key (pathFromRoot/file_name.extension)
        :params required
        key: str
        """
        if "aws_session_token" in self.aws_details:
            self.s3_instance = Client(aws_details=self.aws_details). \
                return_client(service_name="s3",
                              endpoint_url=kwargs.get("endpoint_url"),
                              aws_session_token=self.aws_details.get("aws_session_token"))
        else:
            self.s3_instance = Client(aws_details=self.aws_details). \
                return_client(service_name="s3",
                              endpoint_url=kwargs.get("endpoint_url"))
        self.s3_details = kwargs["s3_details"]
        try:
            self.s3_instance.head_object(
                Bucket=self.s3_details["bucket_name"],
                Key=key
            )
            logging.info(f"{key} exists in the folder {self.s3_details['bucket_name']}")
            return True
        except ClientError as error:
            if error.response['Error']['Code'] == '404':
                logging.info(f"{key} does not exists in the folder {self.s3_details['bucket_name']}")
            else:
                logging.error(f"Client error: [{error}]")
                if kwargs.get("throw_exception"):
                    raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error
        return False

    def check_contents_of_s3(self, **kwargs) -> dict:
        """
        Driving method which will get contents of all the objects in s3
        :return: Returns object dict containing details about s3
        """
        return self.check_contents_of_storage(**kwargs)

    def check_contents_of_storage(self, **kwargs) -> dict:
        """
        Driving method which will get contents of all the objects in s3
        :return: Returns object dict containing details about s3
        """
        if "aws_session_token" in self.aws_details:
            self.s3_instance = Client(aws_details=self.aws_details). \
                return_client(service_name="s3",
                              endpoint_url=kwargs.get("endpoint_url"),
                              aws_session_token=self.aws_details.get("aws_session_token"))
        else:
            self.s3_instance = Client(aws_details=self.aws_details). \
                return_client(service_name="s3",
                              endpoint_url=kwargs.get("endpoint_url"))

        self.folder_to_check = kwargs["folder_to_check"] if "folder_to_check" in kwargs else ""
        self.s3_object_filter = kwargs["s3_object_filter"] if "s3_object_filter" in kwargs else None
        self.delimiter = kwargs["delimiter"] if "delimiter" in kwargs else ""
        self.s3_details = kwargs["s3_details"]

        try:
            # Required parameter to call list_objects_v2
            self.__add_details_to_object_dict(
                self.s3_instance.list_objects_v2(Bucket=self.s3_details["bucket_name"],
                                                 Prefix=self.folder_to_check,
                                                 Delimiter=self.delimiter), kwargs.get("last_modified"))

            logging.debug(f"Objects matching filter criteria : {self.object_dict}")

            logging.info(f"Total No of Objects in S3 in folder {self.folder_to_check} : {len(self.object_dict)}")

        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

        return self.object_dict


# Copy
class CopyObjectFromLocalToS3(object):
    """
    This class provide interface to copy from local to S3
    """
    max_size_for_single_upload = 5 * 1024 ** 3

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.destination_aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyToS3 : {self.__dict__}")

    def copy_to_destination_storage(self, **kwargs):
        """
        This method uploads file from local machine to s3
        """

        object_destination_address = f"s3://{kwargs['destination_s3_details']['bucket_name']}/" \
                                     f"{kwargs['object_destination_path']}"

        try:
            destination_transport_params = {
                "client": Client(aws_details=self.destination_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("destination_endpoint_url")),

            }
            if len(kwargs['data']) <= CopyObjectFromLocalToS3.max_size_for_single_upload:
                destination_transport_params["multipart_upload"] = False
            with open(object_destination_address, "wb",
                      transport_params=destination_transport_params) as f_write:
                # This expects data in bytes format from user
                f_write.write(kwargs["data"])

        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error

    def copy_to_destination_storage_str(self, **kwargs):
        """
        This method uploads file from local machine to s3
        """

        object_destination_address = f"s3://{kwargs['destination_s3_details']['bucket_name']}/" \
                                     f"{kwargs['object_destination_path']}"

        try:
            destination_transport_params = {
                "client": Client(aws_details=self.destination_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("destination_endpoint_url")),

            }
            if len(kwargs['data']) <= CopyObjectFromLocalToS3.max_size_for_single_upload:
                destination_transport_params["multipart_upload"] = False
            with open(object_destination_address, "w",
                      transport_params=destination_transport_params) as f_write:
                # This expects data in bytes format from user
                f_write.write(kwargs["data"])
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("return_error"):
                return "Error"
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("return_error"):
                return "Error"
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("return_error"):
                return "Error"
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromS3ToLocal(object):
    """
    This Class is to data handle from s3 to local
    """

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromS3ToLocal : {self.__dict__}")

    def download_content(self, **kwargs):
        """
        This method downloads file from s3 to local machine
        """
        try:
            object_address = f"s3://{kwargs['s3_details']['bucket_name']}/" \
                             f"{kwargs['object_path']}"

            transport_params = {
                "client": Client(aws_details=self.aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("endpoint_url"))
            }

            with open(object_address, "rb", transport_params=transport_params) as s3_file:
                with open(kwargs["local_file_path"], "wb") as local_file:
                    for line in s3_file:
                        local_file.write(line)

        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromURLtoS3(object):
    """
    This class provide interface to copy data from url to s3
    """
    max_size_for_single_upload = 524288000  # 500 MB is the optimal limit for the ingestion
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.destination_aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyFromURLtoS3 : {self.__dict__}")

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
        This method downloads file from url to s3,
        additional argument content_type(boolean) which will extract ContentType of the file from given URL
        and use it while S3 upload
        """

        def copier():
            with open(kwargs["url"], "rb", transport_params=source_transport_params) as f_read:
                if f_read.content_length <= CopyObjectFromURLtoS3.max_size_for_single_upload and not kwargs.get(
                        "content_type"):
                    destination_transport_params.pop("client_kwargs")
                    destination_transport_params["multipart_upload"] = False
                if kwargs.get("content_type"):
                    destination_transport_params['client_kwargs']['S3.Client.create_multipart_upload'][
                        'ContentType'] = f_read.response.headers['content-type']
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromURLtoS3.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        try:

            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromURLtoS3.chunk_size
            object_destination_address = f"s3://{kwargs['destination_s3_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = kwargs.get("transport_params")
            destination_transport_params = {
                "client": Client(aws_details=self.destination_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("destination_endpoint_url")),
                "min_part_size": chunk_size,
                "client_kwargs": {
                    'S3.Client.create_multipart_upload': {}
                }
            }

            copier()
        except requests.exceptions.SSLError:
            default_ciphers = requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS
            requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'DEFAULT@SECLEVEL=1'
            copier()
            requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = default_ciphers
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception") or kwargs.get("raise_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception") or kwargs.get("raise_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception") or kwargs.get("raise_exception"):
                raise error


class CopyObjectFromS3ToS3(object):
    """
    This class provide interface to copy object from one s3 to another s3
    """
    chunk_size = 256 * 1024 ** 2
    max_size_for_single_upload = 5 * 1024 ** 3

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.source_aws_details = None
        self.destination_aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromS3ToS3 : {self.__dict__}")

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

    def copy_from_source_to_destination_storage(self, **kwargs):
        """
        This method downloads file from one s3 to another s3
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromS3ToS3.chunk_size
            object_original_address = f"s3://{kwargs['source_s3_details']['bucket_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"s3://{kwargs['destination_s3_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": Client(aws_details=self.source_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("source_endpoint_url")),
            }

            destination_transport_params = {
                "client": Client(aws_details=self.destination_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("destination_endpoint_url")),
                "min_part_size": chunk_size
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                if f_read._raw_reader._content_length <= CopyObjectFromS3ToS3.max_size_for_single_upload:
                    destination_transport_params["multipart_upload"] = False
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromS3ToS3.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromS3ToGS(object):
    """
    This class provide interface to copy object from s3 to gs
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.source_aws_details = None
        self.destination_sa_json_data = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromS3ToGS : {self.__dict__}")

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

    def copy_from_source_to_destination_storage(self, **kwargs):
        """
        This method downloads file from one s3 to another s3
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromS3ToGS.chunk_size
            object_original_address = f"s3://{kwargs['source_s3_details']['bucket_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"gs://{kwargs['destination_gs_details']['bucket_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": Client(aws_details=self.source_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("source_endpoint_url")),
            }

            destination_transport_params = {
                "client": StorageClient(sa_json_data=self.destination_sa_json_data).return_client(),
                "min_part_size": chunk_size
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromS3ToGS.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)
        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


class CopyObjectFromS3ToAzure(object):
    """
    This class provide interface to copy object from s3 to azure
    """
    chunk_size = 256 * 1024 ** 2

    def __init__(self, **kwargs):
        # Required variable to drive this Class, expected to be provided from parent Object
        self.account_url = None
        self.source_aws_details = None
        self.destination_azure_details = None

        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyObjectFromS3ToAzure : {self.__dict__}")

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
        This method downloads file from one s3 to another azure
        """

        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromS3ToAzure.chunk_size

            object_original_address = f"s3://{kwargs['source_aws_details']['bucket_name']}/" \
                                      f"{kwargs['object_original_path']}"
            object_destination_address = f"azure://{kwargs['destination_container_details']['container_name']}/" \
                                         f"{kwargs['object_destination_path']}"

            source_transport_params = {
                "client": Client(aws_details=self.source_aws_details).return_client(
                    "s3", endpoint_url=kwargs.get("source_endpoint_url")),
                "min_part_size": chunk_size
            }

            destination_transport_params = {
                "client": AzureStorageClient(account_url=self.account_url,
                                             azure_details=self.destination_azure_details).return_blob_service_client()
            }

            with open(object_original_address, "rb",
                      transport_params=source_transport_params) as f_read:
                with open(object_destination_address, "wb",
                          transport_params=destination_transport_params) as f_write:
                    for data_line in CopyObjectFromS3ToAzure.read_in_chunks(f_read, chunk_size):
                        f_write.write(data_line)

        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# S3 FS
class CopyObjectFromLocalToS3FS(object):
    """
    This class provide interface to copy from local to S3FS
    """
    default_block_size = 100 * 2 ** 20

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.destination_aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for CopyToS3FS : {self.__dict__}")

    def copy_to_destination(self, **kwargs):
        """
        This method uploads file from local machine to s3 using s3fs
        """

        object_destination_address = f"{kwargs['destination_s3_details']['bucket_name']}/" \
                                     f"{kwargs['object_destination_path']}"

        endpoint_dict = None
        block_size = int(kwargs.get("block_size")) * 2 ** 20 if kwargs.get(
            "block_size") else CopyObjectFromLocalToS3FS.default_block_size
        if kwargs.get("destination_endpoint_url"):
            endpoint_dict = {'endpoint_url': kwargs.get("destination_endpoint_url")}

        try:
            if "aws_session_token" in self.destination_aws_details:
                fs = s3fs.S3FileSystem(key=self.destination_aws_details["access_key"],
                                       secret=self.destination_aws_details["secret_key"],
                                       token=self.destination_aws_details["aws_session_token"],
                                       client_kwargs=endpoint_dict)
            else:
                fs = s3fs.S3FileSystem(key=self.destination_aws_details["access_key"],
                                       secret=self.destination_aws_details["secret_key"],
                                       client_kwargs=endpoint_dict)

            fs.put(kwargs.get("source_file_path"), object_destination_address, block_size=block_size)

        except ClientError as error:
            logging.error(f"Source credentials error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except EndpointConnectionError as error:
            logging.error(f"Endpoint connection error: [{error}]")
            if kwargs.get("throw_exception"):
                raise error
        except BaseException as error:
            logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise error


# Delete
class DeleteS3Object(object):
    """
        This class provide wrapper to delete S3 objects
    """

    def __init__(self, **kwargs):

        # Required variable to drive this Class, expected to be provided from parent Object
        self.aws_details = None
        self.__dict__.update(kwargs)

        logging.debug(f"Instance variables for S3DeleteObject : {self.__dict__}")

    @staticmethod
    def api_response_handler(response):
        """
        This method checks whether has responded with code : 204 or not
        :param response: response received after deleting object
        """

        if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
            logging.info("Deleted successfully")
        logging.error(f"Response from delete : {response['ResponseMetadata']}")

    def delete_from_storage(self, **kwargs):
        """
        Driving method which delete S3 object
        """

        if "s3_details" in kwargs:

            s3_client_instance = Client(aws_details=self.aws_details). \
                return_client(service_name="s3", endpoint_url=kwargs.get("endpoint_url"))
            try:
                DeleteS3Object.api_response_handler(
                    s3_client_instance.delete_object(Bucket=kwargs["s3_details"]["bucket_name"],
                                                     Key=kwargs["object_path"]))
            except ClientError as error:
                logging.error(f"Source credentials error: [{error}]")
                if kwargs.get("throw_exception"):
                    raise error
            except EndpointConnectionError as error:
                logging.error(f"Endpoint connection error: [{error}]")
                if kwargs.get("throw_exception"):
                    raise error
            except BaseException as error:
                logging.error(f"Uncaught exception in s3.py : {traceback.format_exc()}")
                if kwargs.get("throw_exception"):
                    raise error
        else:
            logging.error("No S3 details provided !")


