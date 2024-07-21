#!/usr/bin/python3
# coding= utf-8
import logging

import pysftp
from retry import retry


class SFTPHelper:
    """
        This class handles sftp connection
    """
    sftp_instance = None

    def __init__(self, **kwargs):
        self.host = ''
        self.username = ''
        self.password = ''
        self.__dict__.update(kwargs)
        self.get_connection()

    @retry(ConnectionError, tries=3, delay=2)
    def get_connection(self):
        """
            This method establishes sftp connection.
            Retries 3 times with a delay of 2 sec, if connection fails
        """
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            self.sftp_instance = pysftp.Connection(self.host, username=self.username, password=self.password,
                                                   cnopts=cnopts)
        except BaseException:
            logging.error("Connection not established. Retrying...")
            raise ConnectionError

    def open_file(self, **kwargs):
        """
            This method returns file present in sftp
        """
        try:
            with self.sftp_instance.open(f"{kwargs.get('file_directory')}/{kwargs.get('filename')}") as f:
                return f.read()
        except BaseException:
            logging.error(f"Unable to read file {kwargs.get('filename')}")
            return None

    def list_directory(self, **kwargs):
        """
            This method lists a directory
        """
        return self.sftp_instance.listdir(kwargs.get('file_directory'))

    def copy_to_local(self, **kwargs):
        """
            This method copies a file from ftp to local
        """
        try:
            self.sftp_instance.get(f"{kwargs.get('file_directory')}/{kwargs.get('filename')}",
                                   kwargs.get('destination_path'))
        except BaseException:
            logging.error(f"Unable to copy file {kwargs.get('filename')}")
