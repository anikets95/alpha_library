#!/usr/bin/python3
# coding= utf-8
"""
This is a script to find possible extension of a file from a url, 
using response headers Content-Disposition
"""
import logging

import requests

try:
    from alpha_library.helper.http_requests import HTTPRequests
except ModuleNotFoundError:
    logging.info("Module called internally")
    from helper.http_requests import HTTPRequests


def guess_file_extension(input: str):
    """
    This method tries to find the file extensions based on the attached filenames
    in the given url using content-disposition header
    :param input: str
    """
    response = None
    extension = ""
    if type(input) == str:
        logging.info(f"Assuming provided data is a url : {input}")

        # Doing HEAD HTTP call
        response = HTTPRequests().call_head_requests(url=input)

        if not (response and response.status_code == 200):
            # Doing GET HTTP call with python-user-agent
            response = HTTPRequests().call_get_requests(url=input, stream=True, close_early=True)

    elif type(input) == requests.Response:
        logging.info(f"Got HTTP response : {input.__str__}")
        response = input

    if response and response.status_code == 200:
        content_disposition = response.headers.get('Content-Disposition')
        if content_disposition:
            attributes = content_disposition.split(";")
            for attribute in attributes:
                if "filename*" in attribute:
                    file = attribute.split("=")[-1].strip('"')
                    extension = file.split(".")[-1] if "." in file else ""
                    break
                elif "filename" in attribute:
                    file = attribute.split("=")[-1].strip('"')
                    extension = file.split(".")[-1] if "." in file else ""
            extension = f".{extension}" if extension else ""
    return extension

