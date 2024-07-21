#!/usr/bin/python3
# coding= utf-8
"""
This is a script to find possible extension of a file from a url
"""
import logging
from mimetypes import guess_extension, guess_type
from os import path

import requests

try:
    from alpha_library.helper.http_requests import HTTPRequests
except ModuleNotFoundError:
    logging.info("Module called internally")
    from helper.http_requests import HTTPRequests

mime_mapper = {
    "text/webvtt": ".vtt",
    "text/vtt": ".vtt"
}

mime_mapper_reversed = {
    ".vtt": "text/webvtt",
    ".m4v": "video/x-m4v"
}


def guess_file_extension(input: str, input_type: str = "url"):
    """
    This method tries to guess file extension for a url or a response header
    Source of data : mimetype or internal mapping of url
    :param input: str
    :param input_type: str
    """
    response = None
    extension, content_type = "", None

    if input_type == "filename":
        _, content_type = path.splitext(input)
        content_type = content_type.replace(".", "")
    else:
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
            content_type = response.headers['content-type']
            extension = guess_extension(content_type)

    # Trying in local mapping if not found !
    if not extension:
        extension = mime_mapper.get(content_type)

        if not extension:
            logging.error("Unable to guess extension from the response header")
    else:
        logging.info(f"Extension {extension} obtained from the url {input}")

    return extension


def guess_file_type(input: str, input_type: str = "url"):
    """
        This method tries to guess the file type from the given url/ filename 
        :param input :str
        :param input_type : str
    """
    response = None
    if input_type == "filename":
        mimetype_encoding_tuple = guess_type(input)
        if not mimetype_encoding_tuple[0]:
            _, extension = path.splitext(input)
            mimetype_encoding_tuple = mime_mapper_reversed.get(extension), None
    else:
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

        mimetype_encoding_tuple = response.headers['content-type']
    if not mimetype_encoding_tuple:
        logging.error(f"Invalid response {response} for {input}. Unable to guess type")

    return mimetype_encoding_tuple
