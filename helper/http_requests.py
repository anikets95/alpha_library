#!/usr/bin/python3
# coding= utf-8
"""
This is a helper script for sending HTTP Requests
"""
import logging
import ssl
import time

import requests
from urllib3 import poolmanager


class HTTPRequests(object):
    """
        This method defines http requests call and handle error condition based on that
    """
    # Class Variables
    no_response = "No Response !!"

    def __init__(self, **kwargs):
        self.session = requests.Session()
        self.ssl_verify = True
        self.log_minimally = False
        self.TLSAdapter = False
        self.__dict__.update(kwargs)

        if not self.ssl_verify:
            self.session.verify = self.ssl_verify

        logging.debug(
            "Instance variables for HTTPRequests : %s", self.__dict__)

    def __del__(self):
        """
        Explicitly closed session object
        """
        self.session.close()

    def log_minimal(self, msg, *args, **kwargs):
        if self.log_minimally:
            logging.debug(msg, *args, **kwargs)
        else:
            logging.info(msg, *args, **kwargs)

    def call_get_requests(self, url: str, headers=None, params=None, auth=None,
                          stream=False, close_early=False, cookies=None, timeout=None,
                          allow_redirects=True, error_message="Error in GET Request : "):
        """
        Static method to call requests to get response using GET calls
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
        logging.debug("Parameters for HTTP GET request : %s", params)
        logging.debug("Headers for HTTP GET request : %s", headers)
        logging.debug("Request Stream (True/False)? : %s", stream)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP GET request : %s", cookies)
        logging.debug("Timeout for HTTP GET request : %s", timeout)
        logging.debug("Allow Redirects for HTTP GET request : %s", allow_redirects)
        logging.debug("Authorization for HTTP GET request : %s", auth)

        try:
            if self.TLSAdapter:
                self.session.mount('https://', TLSAdapter())
            response = self.session.get(url, headers=headers, params=params, stream=stream, auth=auth,
                                        cookies=cookies, timeout=timeout, allow_redirects=allow_redirects)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.Timeout as error:
            logging.error("%s %s", error_message, str(error))
            raise Exception(error)
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
        return response

    def call_get_requests_with_retry(self, url: str, headers=None, params=None, auth=None,
                                     stream=False, close_early=False, cookies=None, timeout=None,
                                     allow_redirects=True, error_message="Error in GET Request : ", attempts=1,
                                     sleep_duration=15):
        """Retry for GET failures"""
        for attempt in range(0, attempts):
            resp = self.call_get_requests(url, headers=headers, params=params, auth=auth,
                                          stream=stream, close_early=close_early, cookies=cookies, timeout=timeout,
                                          allow_redirects=allow_redirects, error_message=error_message)
            if isinstance(resp, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", resp.headers.get('X-Request-Id'))
            if resp is not None and resp.status_code < 500:
                return resp
            logging.info("No/error response %s. Retrying..", resp)
            attempt += 1
            time.sleep(sleep_duration)

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
        self.log_minimal("Url for HTTP PUT request : %s", url)
        logging.debug("Parameters for HTTP PUT request : %s", params)
        logging.debug("Headers for HTTP PUT request : %s", headers)
        logging.debug("Data for HTTP PUT request : %s", data)
        logging.debug("Files for HTTP PUT request : %s", files)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP PUT request : %s", cookies)
        logging.debug("Timeout for HTTP PUT request : %s", timeout)
        logging.debug("Allow Redirects for HTTP PUT request : %s", allow_redirects)
        logging.debug("Authorization for HTTP PUT request : %s", auth)
        try:
            response = self.session.put(url, headers=headers, params=params, data=data,
                                        files=files, cookies=cookies, timeout=timeout,
                                        allow_redirects=allow_redirects, auth=auth)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
        return response

    def call_put_requests_with_retry(self, url: str, headers=None, params=None,
                                     data=None, files=None, close_early=False,
                                     cookies=None, timeout=None, auth=None,
                                     allow_redirects=True,
                                     error_message="Error in PUT Request : ",
                                     attempts=1, sleep_duration=15):
        """Retry for PUT failures"""
        for attempt in range(0, attempts):
            resp = self.call_put_requests(url, headers=headers, params=params,
                                          data=data, files=files, auth=auth,
                                          cookies=cookies, timeout=timeout,
                                          close_early=close_early,
                                          allow_redirects=allow_redirects,
                                          error_message=error_message)
            if isinstance(resp, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", resp.headers.get('X-Request-Id'))
            if resp is not None and resp.status_code < 500:
                return resp
            logging.info("No/error response %s. Retrying..", resp)
            attempt += 1
            time.sleep(sleep_duration)

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
        self.log_minimal("Url for HTTP POST request : %s", url)
        logging.debug("Parameters for HTTP POST request : %s", params)
        logging.debug("Headers for HTTP POST request : %s", headers)
        logging.debug("Data for HTTP POST request : %s", data)
        logging.debug("Files for HTTP POST request : %s", files)
        logging.debug("Auth for HTTP POST request : %s", auth)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP POST request : %s", cookies)
        logging.debug("Timeout for HTTP POST request : %s", timeout)
        logging.debug(
            "Allow Redirects for HTTP POST request : %s", allow_redirects)

        try:
            response = self.session.post(url, headers=headers, params=params, data=data,
                                         files=files, cookies=cookies, timeout=timeout,
                                         allow_redirects=allow_redirects, auth=auth)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
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
        self.log_minimal("Url for HTTP DELETE request : %s", url)
        logging.debug("Parameters for HTTP DELETE request : %s", params)
        logging.debug("Headers for HTTP DELETE request : %s", headers)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP DELETE request : %s", cookies)
        logging.debug("Timeout for HTTP DELETE request : %s", timeout)
        logging.debug(
            "Allow Redirects for HTTP DELETE request : %s", allow_redirects)
        logging.debug("Authorization for HTTP DELETE request : %s", auth)

        try:
            response = self.session.delete(url, headers=headers, params=params, cookies=cookies,
                                           timeout=timeout, allow_redirects=allow_redirects, auth=auth)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
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
        self.log_minimal("Url for HTTP HEAD request : %s", url)
        logging.debug("Parameters for HTTP HEAD request : %s", params)
        logging.debug("Headers for HTTP HEAD request : %s", headers)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP HEAD request : %s", cookies)
        logging.debug("Timeout for HTTP HEAD request : %s", timeout)
        logging.debug(
            "Allow Redirects for HTTP HEAD request : %s", allow_redirects)
        logging.debug("Authorization for HTTP HEAD request : %s", auth)

        try:
            if self.TLSAdapter:
                self.session.mount('https://', TLSAdapter())
            response = self.session.head(url, headers=headers, params=params, auth=auth,
                                         cookies=cookies, timeout=timeout, allow_redirects=allow_redirects)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
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
        self.log_minimal("Url for HTTP PATCH request : %s", url)
        logging.debug("Parameters for HTTP PATCH request : %s", params)
        logging.debug("Headers for HTTP PATCH request : %s", headers)
        logging.debug("Data for HTTP PATCH request : %s", data)
        logging.debug("Files for HTTP PATCH request : %s", files)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP PATCH request : %s", cookies)
        logging.debug("Timeout for HTTP PATCH  request : %s", timeout)
        logging.debug(
            "Allow Redirects for HTTP PATCH request : %s", allow_redirects)
        logging.debug("Authorization for HTTP PATCH request : %s", auth)

        try:
            response = self.session.patch(url, headers=headers, params=params, data=data,
                                          files=files, cookies=cookies, timeout=timeout,
                                          allow_redirects=allow_redirects, auth=auth)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
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
        self.log_minimal("Url for HTTP OPTIONS request : %s", url)
        logging.debug("Parameters for HTTP OPTIONS request : %s", params)
        logging.debug("Headers for HTTP OPTIONS request : %s", headers)
        logging.debug("Close Connection Early (True/False)? : %s", close_early)
        logging.debug("Cookies for HTTP OPTIONS request : %s", cookies)
        logging.debug("Timeout for HTTP OPTIONS request : %s", timeout)
        logging.debug(
            "Allow Redirects for HTTP OPTIONS request : %s", allow_redirects)
        logging.debug("Authorization for HTTP OPTIONS request : %s", auth)

        try:
            response = self.session.options(url, headers=headers, params=params, auth=auth,
                                            cookies=cookies, timeout=timeout, allow_redirects=allow_redirects)
            if isinstance(response, requests.Response):
                logging.info("Request-Id for HTTP GET Request: %s", response.headers.get('X-Request-Id'))
            if close_early:
                response.close()
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logging.error("%s %s", error_message, str(error))
        finally:
            if response:
                self.log_minimal("Status Code : %s", response.status_code)
                logging.debug("Total Time taken : %s", response.elapsed)
                logging.debug("Response Headers : %s", response.headers)
                logging.debug("Request Headers : %s", response.request.headers)
            else:
                logging.critical(HTTPRequests.no_response)
        return response


class TLSAdapter(requests.adapters.HTTPAdapter):

    def init_poolmanager(self, connections, maxsize, block=False):
        """Create and initialize the urllib3 PoolManager."""
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLS,
            ssl_context=ctx)


if __name__ == "__main__":
    # LOGGING #
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    LOG_FRMT = "%(asctime)s::%(filename)s::%(funcName)s::%(lineno)d::%(levelname)s:: %(message)s"
    logging.basicConfig(format=LOG_FRMT,
                        level=logging.DEBUG, datefmt="%Y/%m/%d %H:%M:%S:%Z(%z)")
    logger = logging.getLogger(__name__)
    HTTPRequests().call_get_requests("https://www.google.com")
