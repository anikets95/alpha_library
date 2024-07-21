#!/usr/bin/python3
# coding= utf-8
"""
This is a helper script for sending HTTP Requests
"""
import logging

import httpx


class HTTPXRequests(object):
    """
        This method defines http requests call and handle error condition based on that
    """
    # Class Variables
    no_response = "No Response !!"

    def __init__(self, ssl_verify=True, **kwargs):
        self.client = httpx.Client(http2=True)
        self.__dict__.update(kwargs)
        if not ssl_verify:
            self.client.verify = False
        self.log_minimally = False
        self.TLSAdapter = False

        logging.debug(f"Instance variables for HTTPXRequests : {self.__dict__}")

    def __del__(self):
        """
        Explicitly closed session object
        """
        self.client.close()

    def log_minimal(self, msg, *args, **kwargs):
        if self.log_minimally:
            logging.debug(msg, *args, **kwargs)
        else:
            logging.info(msg, *args, **kwargs)

    def call_get_requests(self, url: str, headers=None, params=None, auth=None,
                          stream=False, stream_type=None, close_early=False, cookies=None,
                          timeout=None, allow_redirects=True, error_message="Error in GET Request : "):
        """
        Static method to call requests to get response using GET calls
        :param stream_type : Type of stream to be used (binary, text, line-by-line, raw, None)
        :param auth: authentication for get call
        :param allow_redirects: allow redirects for get call
        :param timeout: timeout for get call
        :param cookies: cookies for get call
        :param headers: Headers for get call
        :param stream: If data to be streamed ?
        :param url:  URL for get call
        :param params:  Parameters for the call
        :param error_message: Error message to be printed in case of exception
        :param close_early: Close the connection early (default: False)
        :return: Response from the requests call
        """
        response = None
        self.log_minimal("Url for HTTP GET request : %s", url)
        logging.debug(f"Parameters for HTTP GET request : {params}")
        logging.debug(f"Headers for HTTP GET request : {headers}")
        logging.debug(f"Request Stream (True/False)? : {stream}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP GET request : {cookies}")
        logging.debug(f"Timeout for HTTP GET request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP GET request : {allow_redirects}")
        logging.debug(f"Authorization for HTTP GET request : {auth}")
        try:
            if stream:
                # returns a stream object (https://www.python-httpx.org/quickstart/#streaming-responses)
                # now it depends on developer how they want to take the response
                httpx_stream_object = httpx.stream(url=url, method="GET", headers=headers, params=params, auth=auth,
                                                   cookies=cookies, timeout=timeout)
                if stream_type == "binary":
                    return httpx_stream_object.iter_bytes()
                elif stream_type == "text":
                    return httpx_stream_object.iter_text()
                elif stream_type == "line-by-line":
                    return httpx_stream_object.iter_lines()
                elif stream_type == "raw":
                    return httpx_stream_object.iter_raw()
                else:
                    return httpx_stream_object

            # Non-stream !
            response = self.client.get(url, headers=headers, params=params, auth=auth,
                                       cookies=cookies, timeout=timeout, follow_redirects=allow_redirects)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of Response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response

    def call_put_requests(self, url: str, headers=None, params=None, data=None, files=None,
                          close_early=False, cookies=None, timeout=None, auth=None,
                          allow_redirects=True, error_message="Error in PUT Request : "):
        """
        Static method to call requests to get response using PUT calls
        :param files: files for put call
        :param auth: authorization for put call
        :param allow_redirects: allow redirects for put call
        :param timeout: timeout for put call
        :param cookies: cookies for put call
        :param close_early: close the connection early (default: False)
        :param data: data for put call
        :param headers: Header for put call
        :param url:  URL for put call
        :param params:  Parameters for put call
        :param error_message: Error message to be printed in case of exception
        :return: Response from the requests call
        """
        response = None
        self.log_minimal(f"Url for HTTP PUT request : {url}")
        logging.debug(f"Parameters for HTTP PUT request : {params}")
        logging.debug(f"Headers for HTTP PUT request : {headers}")
        logging.debug(f"Data for HTTP PUT request : {data}")
        logging.debug(f"Files for HTTP PUT request : {files}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP PUT request : {cookies}")
        logging.debug(f"Timeout for HTTP PUT request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP PUT request : {allow_redirects}")
        logging.debug(f"Authorization for HTTP PUT request : {auth}")
        try:
            response = self.client.put(url, headers=headers, params=params, data=data,
                                       files=files, cookies=cookies, timeout=timeout,
                                       follow_redirects=allow_redirects, auth=auth)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal(f"Status Code : {response.status_code}")
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of Response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response

    def call_post_requests(self, url: str, headers=None, params=None, data=None, files=None,
                           close_early=False, cookies=None, timeout=None, auth=None,
                           allow_redirects=True, error_message="Error in POST Request : "):
        """
        Static method to call requests to get response using POST calls
        :param allow_redirects: allow redirects for post call
        :param timeout: timeout for post call
        :param cookies: cookies for post call
        :param close_early: close the connection early (default: False)
        :param data: data for post call
        :param files: files for post call
        :param headers: Header for post call
        :param url:  URL for post call
        :param params:  Parameters for post call
        :param error_message: Error message to be printed in case of exception
        :param auth: Authorization for post call
        :return: Response from the requests call
        """
        response = None
        self.log_minimal(f"Url for HTTP POST request : {url}")
        logging.debug(f"Parameters for HTTP POST request : {params}")
        logging.debug(f"Headers for HTTP POST request : {headers}")
        logging.debug(f"Data for HTTP POST request : {data}")
        logging.debug(f"Files for HTTP POST request : {files}")
        logging.debug(f"Auth for HTTP POST request : {auth}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP POST request : {cookies}")
        logging.debug(f"Timeout for HTTP POST request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP POST request : {allow_redirects}")

        try:
            response = self.client.post(url, headers=headers, params=params, data=data,
                                        files=files, cookies=cookies, timeout=timeout,
                                        follow_redirects=allow_redirects, auth=auth)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal(f"Status Code : {response.status_code}")
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response

    def call_delete_requests(self, url: str, params=None, headers=None, close_early=False,
                             cookies=None, timeout=None, auth=None, allow_redirects=True,
                             error_message="Error in DELETE Request : "):
        """
        Static method to call requests to get response using GET calls
        :param cookies: cookies for delete call
        :param timeout: timeout for delete call
        :param auth: authorization for delete call
        :param allow_redirects: allow redirects for delete call
        :param close_early: close the connection early (default: False)
        :param headers: Header for delete call
        :param url:  URL for delete call
        :param params:  Parameters for delete call
        :param error_message: Error message to be printed in case of exception
        :return: Response from the requests call
        """
        response = None
        self.log_minimal(f"Url for HTTP DELETE request : {url}")
        logging.debug(f"Parameters for HTTP DELETE request : {params}")
        logging.debug(f"Headers for HTTP DELETE request : {headers}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP DELETE request : {cookies}")
        logging.debug(f"Timeout for HTTP DELETE request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP DELETE request : {allow_redirects}")
        logging.debug(f"Authorization for HTTP DELETE request : {auth}")

        try:
            response = self.client.delete(url, headers=headers, params=params, cookies=cookies,
                                          timeout=timeout, follow_redirects=allow_redirects, auth=auth)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal(f"Status Code : {response.status_code}")
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response

    def call_head_requests(self, url: str, headers=None, params=None, auth=None,
                           close_early=False, cookies=None, timeout=None,
                           allow_redirects=True, error_message="Error in HEAD Request : "):
        """
                method to call requests to get response using HEAD calls
                :param params: parameters for head call
                :param headers: headers for head call
                :param url:  URL for head call
                :param auth: authorization for head call
                :param close_early: close the connection early (default: False)
                :param cookies: cookies for head call
                :param timeout: timeout for head call
                :param allow_redirects: allow redirects for head call
                :param error_message: Error message to be printed in case of exception
                :return: Response from the requests call
                """
        response = None
        self.log_minimal(f"Url for HTTP HEAD request : {url}")
        logging.debug(f"Parameters for HTTP HEAD request : {params}")
        logging.debug(f"Headers for HTTP HEAD request : {headers}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP HEAD request : {cookies}")
        logging.debug(f"Timeout for HTTP HEAD request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP HEAD request : {allow_redirects}")
        logging.debug(f"Authorization for HTTP HEAD request : {auth}")

        try:
            response = self.client.head(url, headers=headers, params=params, auth=auth,
                                        timeout=timeout, cookies=cookies, follow_redirects=True)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal(f"Status Code : {response.status_code}")
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response

    def call_patch_requests(self, url: str, headers=None, params=None, data=None, files=None,
                            close_early=False, cookies=None, timeout=None, auth=None,
                            allow_redirects=True, error_message="Error in PATCH Request : "):
        """
        Static method to call requests to get response using PATCH calls
        :param cookies: cookies for patch call
        :param timeout: timeout for patch call
        :param auth: authorization for patch call
        :param allow_redirects: allow redirects for patch call
        :param close_early: close the connection early (default: False)
        :param files: files for patch call
        :param data: Adding data for patch call
        :param headers: Header for patch call
        :param url:  URL for patch call
        :param params:  Parameters for patch call
        :param error_message: Error message to be printed in case of exception
        :return: Response from the requests call
        """
        response = None
        self.log_minimal(f"Url for HTTP PATCH request : {url}")
        logging.debug(f"Parameters for HTTP PATCH request : {params}")
        logging.debug(f"Headers for HTTP PATCH request : {headers}")
        logging.debug(f"Data for HTTP PATCH request : {data}")
        logging.debug(f"Files for HTTP PATCH request : {files}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP PATCH request : {cookies}")
        logging.debug(f"Timeout for HTTP PATCH  request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP PATCH request : {allow_redirects}")
        logging.debug(f"Authorization for HTTP PATCH request : {auth}")

        try:
            response = self.client.patch(url, headers=headers, params=params, data=data,
                                         files=files, cookies=cookies, timeout=timeout,
                                         follow_redirects=allow_redirects, auth=auth)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal(f"Status Code : {response.status_code}")
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response

    def call_options_requests(self, url: str, headers=None, params=None, auth=None,
                              close_early=False, cookies=None, timeout=None,
                              allow_redirects=True, error_message="Error in OPTIONS Request : "):
        """
        Static method to call requests to get response using PUT calls
        :param params: parameters for options call
        :param headers: headers for options call
        :param url:  URL for options call
        :param auth: authorization for options call
        :param close_early: close the connection early (default: False)
        :param cookies: cookies for options call
        :param timeout: timeout for options call
        :param allow_redirects: allow redirects for options call
        :param error_message: Error message to be printed in case of exception
        :return: Response from the requests call
        """
        response = None
        self.log_minimal(f"Url for HTTP OPTIONS request : {url}")
        logging.debug(f"Parameters for HTTP OPTIONS request : {params}")
        logging.debug(f"Headers for HTTP OPTIONS request : {headers}")
        logging.debug(f"Close Connection Early (True/False)? : {close_early}")
        logging.debug(f"Cookies for HTTP OPTIONS request : {cookies}")
        logging.debug(f"Timeout for HTTP OPTIONS request : {timeout}")
        logging.debug(f"Allow Redirects for HTTP OPTIONS request : {allow_redirects}")
        logging.debug(f"Authorization for HTTP OPTIONS request : {auth}")

        try:

            response = self.client.options(url, headers=headers, params=params, auth=auth,
                                           cookies=cookies, timeout=timeout, follow_redirects=allow_redirects)
            if close_early:
                response.close()
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as error:
            logging.error(error_message + str(error))
        finally:
            if response:
                self.log_minimal(f"Status Code : {response.status_code}")
                logging.debug(f"Total Time taken : {response.elapsed}")
                logging.debug(f"Encoding : {response.encoding}")
                logging.debug(f"Response Headers : {response.headers}")
                logging.debug(f"Request Headers : {response.request.headers}")
                logging.debug(f"Size of response : {len(response.content)}")
                if response.encoding or response.text:
                    logging.debug(f"Response Text : {response.text}")
            else:
                logging.critical(HTTPXRequests.no_response)
        return response


if __name__ == "__main__":
    # LOGGING #
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging_format = "%(asctime)s::%(filename)s::%(funcName)s::%(lineno)d::%(levelname)s:: %(message)s"
    logging.basicConfig(format=logging_format, level=logging.DEBUG, datefmt="%Y/%m/%d %H:%M:%S:%Z(%z)")
    logger = logging.getLogger(__name__)
    HTTPXRequests().call_get_requests("https://www.youtube.com")
