#!/usr/bin/python3
# coding= utf-8
import logging
import os
import traceback

import certifi
import pycurl
from smart_open import open


class CopyObjectFromURLToLocal(object):
    """
        This class handles download from url to local
    """
    downloaded = -1
    chunk_size = 15 * 1024 ** 2

    def __init__(self, **kwargs):
        self.url = None
        self.local_file_path = None
        self.__dict__.update(kwargs)

    @staticmethod
    def read_in_chunks(file_object, chunk_size):
        """
        Lazy function (generator) to read a file piece by piece.
        xref: https://dataroz.com/stream-large-files-between-s3-and-gcs-python/
        """
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    @staticmethod
    def status(download_t, download_d, upload_t, upload_d):
        """
        This method is used to track the download progress
        :param download_t: total to download
        :param download_d: total download
        :param upload_t: total to upload
        :param upload_d: total uploaded
        """
        giga_byte = 1024 ** 3
        downloaded_curr = int(download_d / giga_byte)

        if downloaded_curr > CopyObjectFromURLToLocal.downloaded:
            CopyObjectFromURLToLocal.downloaded = downloaded_curr
            logging.info('Downloading: {}/{} GB ({}%)\r'.format(
                str(int(download_d / giga_byte)),
                str(int(download_t / giga_byte)),
                str(int(download_d / download_t * 100) if download_t > 0 else 0)
            ))

    def download_content(self, **kwargs):
        """
        This method downloads file from url to local using smartopen (default) or pycurl
        """
        if not kwargs.get('typ') or kwargs.get('typ') == "smartopen":
            self.download_content_with_smartopen(**kwargs)
        elif kwargs.get('typ') == "pycurl":
            self.download_content_with_pycurl(**kwargs)

    def download_content_with_smartopen(self, **kwargs):
        """
            This method downloads file from url to local machine using Smart-Open
        """
        try:
            chunk_size = kwargs["chunk_size"] if kwargs.get("chunk_size") else CopyObjectFromURLToLocal.chunk_size
            with open(self.url, "rb", transport_params=kwargs.get("transport_params")) as f_read:
                with open(self.local_file_path, "wb") as local_file:
                    for data_line in CopyObjectFromURLToLocal.read_in_chunks(f_read, chunk_size):
                        local_file.write(data_line)

        except BaseException:
            logging.error(f"Uncaught exception in url_content_downloader.py (smart-open) : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise BaseException("Problem in downloading file from url to local")

    def download_content_with_pycurl(self, **kwargs):
        """
            This method downloads file from url to local machine using Pycurl
        """
        try:
            status_code = False
            with open(self.local_file_path, "wb") as f:
                c = pycurl.Curl()
                c.setopt(c.URL, self.url)
                c.setopt(c.WRITEDATA, f)
                c.setopt(c.CAINFO, certifi.where())
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(pycurl.USERAGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
                                           " Chrome/96.0.4664.110 Safari/537.36")
                c.setopt(c.NOPROGRESS, False)
                c.setopt(c.XFERINFOFUNCTION, CopyObjectFromURLToLocal.status)
                c.perform()

                if c.getinfo(c.RESPONSE_CODE) in range(400, 600):
                    logging.error(f"Error in downloading file from url to local: {c.getinfo(c.RESPONSE_CODE)}")
                    status_code = True

                logging.info(f"Total time to download the asset : {c.getinfo(c.TOTAL_TIME)}")
                c.close()

            if status_code:
                logging.info(f"Deleting file {self.local_file_path}")
                os.remove(self.local_file_path)
                raise BaseException("Status Code in range (400, 600) ")

        except BaseException:
            logging.error(f"Uncaught exception in url_content_downloader.py (pycurl) : {traceback.format_exc()}")
            if kwargs.get("throw_exception"):
                raise BaseException("Problem in downloading file from url to local")


if __name__ == "__main__":
    # LOGGING #
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging_format = "%(asctime)s::%(filename)s::%(funcName)s::%(lineno)d::%(levelname)s:: %(message)s"
    logging.basicConfig(format=logging_format, level=logging.DEBUG, datefmt="%Y/%m/%d %H:%M:%S:%Z(%z)")
    logger = logging.getLogger(__name__)
    CopyObjectFromURLToLocal(url="https://www.google.com", local_file_path="/tmp/sample.txt"). \
        download_content(typ="pycurl", throw_exception=True)
    CopyObjectFromURLToLocal(url="https://www.google.com", local_file_path="/tmp/sample.txt"). \
        download_content(typ="smartopen", throw_exception=True)