#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over storage providers in alpha_library
"""
import json
import logging

from Crypto.Cipher import AES

try:
    from alpha_library.boto3_helper.s3 import DisplayS3Object, CopyObjectFromLocalToS3, CopyObjectFromURLtoS3, \
        CopyObjectFromS3ToLocal, DeleteS3Object, S3ObjectList, CopyObjectFromS3ToS3, \
        CopyObjectFromS3ToGS, CopyObjectFromS3ToAzure, CopyObjectFromLocalToS3FS
    from alpha_library.gcp_helper.storage import CopyObjectFromLocalToGS, CopyObjectFromURLtoGS, \
        CopyObjectFromGSToLocal, GSDeleteObject, GSObjectList, DisplayGSObject, \
        CopyObjectFromGSToGS, CopyObjectFromGSToS3, CopyObjectFromGSToAzure, CopyObjectFromLocalToGCSFS
    from alpha_library.azure_helper.blob import CopyObjectFromLocalToAzure, CopyObjectFromURLtoAzure, \
        CopyObjectFromAzureToLocal, AzureDeleteObject, AzureObjectList, DisplayAzureObject, \
        CopyObjectFromAzureToAzure, CopyObjectFromAzureToS3, CopyObjectFromAzureToGS, CopyObjectFromLocalToAzureCFS
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.s3 import DisplayS3Object, CopyObjectFromLocalToS3, CopyObjectFromURLtoS3, \
        CopyObjectFromS3ToLocal, DeleteS3Object, S3ObjectList, CopyObjectFromS3ToS3, \
        CopyObjectFromS3ToGS, CopyObjectFromS3ToAzure, CopyObjectFromLocalToS3FS
    from gcp_helper.storage import CopyObjectFromLocalToGS, CopyObjectFromURLtoGS, \
        CopyObjectFromGSToLocal, GSDeleteObject, GSObjectList, DisplayGSObject, \
        CopyObjectFromGSToGS, CopyObjectFromGSToS3, CopyObjectFromGSToAzure, CopyObjectFromLocalToGCSFS
    from azure_helper.blob import CopyObjectFromLocalToAzure, CopyObjectFromURLtoAzure, \
        CopyObjectFromAzureToLocal, AzureDeleteObject, AzureObjectList, DisplayAzureObject, \
        CopyObjectFromAzureToAzure, CopyObjectFromAzureToS3, CopyObjectFromAzureToGS, CopyObjectFromLocalToAzureCFS


# Display
class DisplayStorageObject(object):

    def __init__(self, typ, **kwargs):

        self.typ = typ
        self.account_url = None
        self.azure_details = None
        self.aws_details = None
        self.sa_json_data = None
        self.init_kwargs = kwargs
        self.encryption_keys = kwargs.get("encryption_keys", {})
        if self.encryption_keys:
            key = self.encryption_keys['key']
            iv = self.encryption_keys['initialisation_vector']
            self.block_size = int(self.encryption_keys['block_size'])
            self.decryptor = AES.new(key, IV=iv)

        if self.typ == "s3":
            self.aws_details = kwargs.get("cred_details")
        elif self.typ == "gs":
            self.sa_json_data = kwargs.get("cred_details")
        elif self.typ == "azure":
            self.azure_details = kwargs.get("cred_details")

    def decrypt_data(self, data):
        de_data = self.decryptor.decrypt(data)
        de_data = de_data[0:len(de_data) // self.block_size].decode()
        return de_data

    def object_content(self, **kwargs):

        if self.typ == "s3":
            data = DisplayS3Object(aws_details=self.aws_details, **self.init_kwargs).object_content(
                s3_details=kwargs.get("storage_details"), **kwargs)

        elif self.typ == "gs":
            data = DisplayGSObject(sa_json_data=self.sa_json_data, **self.init_kwargs).object_content(
                gs_details=kwargs.get("storage_details"), **kwargs)

        elif self.typ == "azure":
            data = DisplayAzureObject(account_url=self.account_url, azure_details=self.azure_details,
                                      **self.init_kwargs).object_content(
                container_details=kwargs.get("storage_details"), **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None

        if self.encryption_keys:
            data = self.decrypt_data(data)

        return data

    def object_content_str(self, **kwargs):

        if self.typ == "s3":
            data = DisplayS3Object(aws_details=self.aws_details, **self.init_kwargs).object_content_str(
                s3_details=kwargs.get("storage_details"), **kwargs)

        elif self.typ == "gs":
            data = DisplayGSObject(sa_json_data=self.sa_json_data, **self.init_kwargs).object_content_str(
                gs_details=kwargs.get("storage_details"), **kwargs)

        elif self.typ == "azure":
            data = DisplayAzureObject(account_url=self.account_url, azure_details=self.azure_details,
                                      **self.init_kwargs).object_content_str(
                container_details=kwargs.get("storage_details"), **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None

        if self.encryption_keys:
            data = self.decrypt_data(data)

        return data


# Access Storage
class StorageObjectList(object):

    def __init__(self, typ, **kwargs):

        self.typ = typ
        self.account_url = None
        self.azure_details = None
        self.aws_details = None
        self.sa_json_data = None
        self.init_kwargs = kwargs

        if self.typ == "s3":
            self.aws_details = kwargs.get("cred_details")
        elif self.typ == "gs":
            self.sa_json_data = kwargs.get("cred_details")
        elif self.typ == "azure":
            self.azure_details = kwargs.get("cred_details")

    def check_contents_of_storage(self, **kwargs):
        if self.typ == "s3":
            return S3ObjectList(aws_details=self.aws_details, **self.init_kwargs).check_contents_of_storage(
                s3_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "gs":
            return GSObjectList(sa_json_data=self.sa_json_data, **self.init_kwargs).check_contents_of_storage(
                gs_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "azure":
            logging.info("Implementation Missing at moment !")
            return None
        else:
            logging.info("Unsupported Cloud storage provider")
            return None

    def check_existence_of_file_in_storage(self, **kwargs):
        if self.typ == "s3":
            return S3ObjectList(aws_details=self.aws_details, **self.init_kwargs).check_file_existence(
                s3_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "gs":
            return GSObjectList(sa_json_data=self.sa_json_data, **self.init_kwargs).check_file_existence(
                gs_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "azure":
            logging.info("Implementation Missing at moment !")
            return None
        else:
            logging.info("Unsupported Cloud storage provider")
            return None


# Copy
class CopyObjectFromLocalToStorage(object):

    def __init__(self, typ, **kwargs):

        self.typ = typ
        self.destination_account_url = None
        self.destination_azure_details = None
        self.destination_aws_details = None
        self.destination_sa_json_data = None
        self.init_kwargs = kwargs
        self.encryption_keys = kwargs.get("encryption_keys", {})
        if self.encryption_keys:
            key = self.encryption_keys['key']
            iv = self.encryption_keys['initialisation_vector']
            self.block_size = int(self.encryption_keys['block_size'])
            self.encryptor = AES.new(key, IV=iv)

        if self.typ == "s3":
            self.destination_aws_details = kwargs.get("destination_cred_details")
        elif self.typ == "gs":
            self.destination_sa_json_data = kwargs.get("destination_cred_details")
        elif self.typ == "azure":
            self.destination_azure_details = kwargs.get("destination_cred_details")

    def encrypt_data(self, data):
        enc_data = self.encryptor.encrypt(data * self.block_size)
        return enc_data

    def copy_to_destination_storage(self, **kwargs):
        if self.encryption_keys:
            kwargs["data"] = self.encrypt_data(kwargs.get("data", json.dumps({})))

        if self.typ == "s3":
            return CopyObjectFromLocalToS3(destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_to_destination_storage(destination_s3_details=kwargs.get("destination_storage_details"), **kwargs)

        elif self.typ == "gs":
            return CopyObjectFromLocalToGS(destination_sa_json_data=self.destination_sa_json_data, **self.init_kwargs). \
                copy_to_destination_storage(destination_gs_details=kwargs.get("destination_storage_details"), **kwargs)

        elif self.typ == "azure":
            return CopyObjectFromLocalToAzure(destination_azure_details=self.destination_azure_details,
                                              **self.init_kwargs). \
                copy_to_destination_storage(destination_container_details=kwargs.get("destination_storage_details"),
                                            **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None

    def copy_to_destination_storage_str(self, **kwargs):
        if self.encryption_keys:
            kwargs["data"] = self.encrypt_data(kwargs.get("data", json.dumps({})))

        if self.typ == "s3":
            return CopyObjectFromLocalToS3(destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_to_destination_storage_str(destination_s3_details=kwargs.get("destination_storage_details"),
                                                **kwargs)
        elif self.typ == "gs":
            return CopyObjectFromLocalToGS(destination_sa_json_data=self.destination_sa_json_data, **self.init_kwargs). \
                copy_to_destination_storage_str(destination_gs_details=kwargs.get("destination_storage_details"),
                                                **kwargs)
        elif self.typ == "azure":
            return CopyObjectFromLocalToAzure(destination_azure_details=self.destination_azure_details,
                                              **self.init_kwargs). \
                copy_to_destination_storage_str(destination_container_details=kwargs.get("destination_storage_details"),
                                                **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None


class CopyObjectFromStorageToLocal(object):

    def __init__(self, typ, **kwargs):

        self.typ = typ
        self.account_url = None
        self.azure_details = None
        self.aws_details = None
        self.sa_json_data = None
        self.init_kwargs = kwargs

        if self.typ == "s3":
            self.aws_details = kwargs.get("cred_details")
        elif self.typ == "gs":
            self.sa_json_data = kwargs.get("cred_details")
        elif self.typ == "azure":
            self.azure_details = kwargs.get("cred_details")

    def download_content(self, **kwargs):
        if self.typ == "s3":
            return CopyObjectFromS3ToLocal(aws_details=self.aws_details, **self.init_kwargs).download_content(
                s3_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "gs":
            return CopyObjectFromGSToLocal(sa_json_data=self.sa_json_data, **self.init_kwargs).download_content(
                gs_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "azure":
            return CopyObjectFromAzureToLocal(azure_details=self.sa_json_data, **self.init_kwargs).download_content(
                container_details=kwargs.get("storage_details"), **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None


class CopyObjectFromURLtoStorage(object):

    def __init__(self, typ, **kwargs):

        self.typ = typ
        self.destination_account_url = None
        self.destination_azure_details = None
        self.destination_aws_details = None
        self.destination_sa_json_data = None
        self.init_kwargs = kwargs

        if self.typ == "s3":
            self.destination_aws_details = kwargs.get("destination_cred_details")
        elif self.typ == "gs":
            self.destination_sa_json_data = kwargs.get("destination_cred_details")
        elif self.typ == "azure":
            self.destination_azure_details = kwargs.get("destination_cred_details")

    def copy_from_source_url_to_destination_storage(self, **kwargs):
        if self.typ == "s3":
            return CopyObjectFromURLtoS3(destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_from_source_url_to_destination_storage(
                destination_s3_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.typ == "gs":
            return CopyObjectFromURLtoGS(destination_sa_json_data=self.destination_sa_json_data, **self.init_kwargs). \
                copy_from_source_url_to_destination_storage(
                destination_gs_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.typ == "azure":
            return CopyObjectFromURLtoAzure(destination_azure_details=self.destination_azure_details,
                                            **self.init_kwargs). \
                copy_from_source_url_to_destination_storage(
                destination_container_details=kwargs.get("destination_storage_details"), **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None


class CopyObjectFromStorageToStorage(object):

    def __init__(self, source_type, destination_type, **kwargs):

        self.source_type = source_type
        self.destination_type = destination_type

        self.source_aws_details = None
        self.source_sa_json_data = None
        self.source_account_url = None
        self.source_azure_details = None

        self.destination_aws_details = None
        self.destination_sa_json_data = None
        self.destination_account_url = None
        self.destination_azure_details = None
        self.init_kwargs = kwargs

        if self.source_type == "s3":
            self.source_aws_details = kwargs.get("source_cred_details")
        elif self.source_type == "gs":
            self.source_sa_json_data = kwargs.get("source_cred_details")
        elif self.source_type == "azure":
            self.source_azure_details = kwargs.get("source_cred_details")

        if self.destination_type == "s3":
            self.destination_aws_details = kwargs.get("destination_cred_details")
        elif self.destination_type == "gs":
            self.destination_sa_json_data = kwargs.get("destination_cred_details")
        elif self.destination_type == "azure":
            self.destination_azure_details = kwargs.get("destination_cred_details")

    def copy_from_source_to_destination_storage(self, **kwargs):
        # S3 Combinations
        if self.source_type == "s3" and self.destination_type == "s3":
            return CopyObjectFromS3ToS3(source_aws_details=self.source_aws_details,
                                        destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_s3_details=kwargs.get("source_storage_details"),
                destination_s3_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.source_type == "s3" and self.destination_type == "gs":
            return CopyObjectFromS3ToGS(source_aws_details=self.source_aws_details,
                                        destination_sa_json_data=self.destination_sa_json_data, **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_s3_details=kwargs.get("source_storage_details"),
                destination_gs_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.source_type == "s3" and self.destination_type == "azure":
            return CopyObjectFromS3ToAzure(source_aws_details=self.source_aws_details,
                                           destination_azure_details=self.destination_azure_details,
                                           **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_s3_details=kwargs.get("source_storage_details"),
                destination_container_details=kwargs.get("destination_storage_details"), **kwargs)

        # GS Combinations
        elif self.source_type == "gs" and self.destination_type == "gs":
            return CopyObjectFromGSToGS(source_sa_json_data=self.source_sa_json_data,
                                        destination_sa_json_data=self.destination_sa_json_data, **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_gs_details=kwargs.get("source_storage_details"),
                destination_gs_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.source_type == "gs" and self.destination_type == "s3":
            return CopyObjectFromGSToS3(source_sa_json_data=self.source_sa_json_data,
                                        destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_gs_details=kwargs.get("source_storage_details"),
                destination_s3_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.source_type == "gs" and self.destination_type == "azure":
            return CopyObjectFromGSToAzure(source_sa_json_data=self.source_sa_json_data,
                                           destination_azure_details=self.destination_azure_details,
                                           **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_gs_details=kwargs.get("source_storage_details"),
                destination_container_details=kwargs.get("destination_storage_details"), **kwargs)

        # Azure Combinations
        elif self.source_type == "azure" and self.destination_type == "azure":
            return CopyObjectFromAzureToAzure(source_azure_details=self.source_azure_details,
                                              destination_azure_details=self.destination_azure_details,
                                              **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_container_details=kwargs.get("source_storage_details"),
                destination_container_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.source_type == "azure" and self.destination_type == "s3":
            return CopyObjectFromAzureToS3(source_azure_details=self.source_azure_details,
                                           destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_container_details=kwargs.get("source_storage_details"),
                destination_s3_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.source_type == "azure" and self.destination_type == "gs":
            return CopyObjectFromAzureToS3(source_azure_details=self.source_azure_details,
                                           destination_sa_json_data=self.destination_sa_json_data, **self.init_kwargs). \
                copy_from_source_to_destination_storage(
                source_container_details=kwargs.get("source_storage_details"),
                destination_gs_details=kwargs.get("destination_storage_details"), **kwargs)

        else:
            logging.info(
                f"Either source_type {self.source_type} or destination_type {self.destination_type} not supported yet")
            return None


# Object FS
class CopyObjectFromLocalToObjectFS(object):

    def __init__(self, typ, **kwargs):
        self.typ = typ
        self.destination_account_url = None
        self.destination_aws_details = None
        self.destination_sa_json_data = None
        self.destination_azure_details = None
        self.init_kwargs = kwargs

        if self.typ == "s3":
            self.destination_aws_details = kwargs.get("destination_cred_details")
        elif self.typ == "gs":
            self.destination_sa_json_data = kwargs.get("destination_cred_details")
        elif self.typ == "azure":
            self.destination_azure_details = kwargs.get("destination_cred_details")

    def copy_to_destination_storage(self, **kwargs):
        if self.typ == "s3":
            return CopyObjectFromLocalToS3FS(destination_aws_details=self.destination_aws_details, **self.init_kwargs). \
                copy_to_destination(destination_s3_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.typ == "gs":
            return CopyObjectFromLocalToGCSFS(destination_sa_json_data=self.destination_sa_json_data,
                                              **self.init_kwargs). \
                copy_to_destination(destination_gs_details=kwargs.get("destination_storage_details"), **kwargs)
        elif self.typ == "azure":
            logging.info("Yet to be implemented !")
            return None
            # return CopyObjectFromLocalToAzureCFS(destination_azure_details=self.destination_azure_details,
            #                                   **self.init_kwargs). \
            #     copy_to_destination(destination_container_details=kwargs.get("destination_storage_details"), **kwargs)
        else:
            logging.info("Unsupported Cloud storage provider")
            return None


# Delete
class StorageDeleteObject(object):

    def __init__(self, typ, **kwargs):

        self.typ = typ
        self.aws_details = None
        self.sa_json_data = None
        self.azure_details = None
        self.init_kwargs = kwargs

        if self.typ == "s3":
            self.aws_details = kwargs.get("cred_details")
        elif self.typ == "gs":
            self.sa_json_data = kwargs.get("cred_details")
        elif self.typ == "azure":
            self.azure_details = kwargs.get("cred_details")

    def delete_from_storage(self, **kwargs):
        if self.typ == "s3":
            return DeleteS3Object(aws_details=self.aws_details, **self.init_kwargs).delete_from_storage(
                s3_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "gs":
            return GSDeleteObject(sa_json_data=self.sa_json_data, **self.init_kwargs).delete_from_storage(
                gs_details=kwargs.get("storage_details"), **kwargs)
        elif self.typ == "azure":
            # return AzureDeleteObject(azure_details=self.azure_details, **self.init_kwargs).delete_from_storage(
            #     container_details=kwargs.get("storage_details"), **kwargs)
            logging.info("Yet to be implemented !")
            return None
        else:
            logging.info("Unsupported Cloud storage provider")
            return None


if __name__ == "__main__":
    # LOGGING #
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging_format = "%(asctime)s::%(filename)s::%(funcName)s::%(lineno)d::%(levelname)s:: %(message)s"
    logging.basicConfig(format=logging_format, level=logging.DEBUG, datefmt="%Y/%m/%d %H:%M:%S:%Z(%z)")
    config = {
        "storage_provider": "s3|gs",
        "cred_details": {
            "access_key": "",
            "secret_key": "",
            "region_name": "us-east-1|.."
        },
        "storage_details": {
            "bucket_name": ""
        },
        "endpoint_url": ""
    }
    CopyObjectFromURLtoStorage(typ=config["storage_provider"],
                               destination_cred_details=config["cred_details"]). \
        copy_from_source_url_to_destination_storage(
        destination_storage_details=config["storage_details"],
        object_destination_path="/path/",
        destination_endpoint_url=config["customer_details"].get("endpoint_url"),
        url="https://{{}}}"
    )
