#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over AWS SES
"""
import logging
import os

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    from alpha_library.boto3_helper.client import Client
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.client import Client

# The character encoding for the email.
CHARSET = "utf-8"


class EmailDeliveryFailure(Exception):
    """Custom exception for delivery failure"""
    # TODO : To be completed


class SesSendEmail(object):
    """
        This Class handles creation of email using SES
    """

    def __init__(self, **kwargs):
        self.aws_details = None

        self.__dict__.update(kwargs)

        # Variables required to run the whole Class
        self.msg = None
        self.recipients = None

        if kwargs.get('region_name'):
            self.email_ses_client_instance = Client(aws_details=self.aws_details).return_client(
                service_name="ses", region_name=kwargs['region_name'])
        else:
            self.email_ses_client_instance = Client(
                aws_details=self.aws_details).return_client(service_name="ses")

        logging.debug(
            "Instance variables for SesSendEmail : %s", self.__dict__)

    def prepare_email(self, **kwargs):
        """
            This method prepare parameters for Email
        :param kwargs: keyword arguments for email
        """
        self.msg = MIMEMultipart()

        # Add subject, from and to lines.
        self.msg["Subject"] = kwargs["subject"] if "subject" in kwargs else ""
        self.msg["From"] = kwargs["sender"] if "sender" in kwargs else ""

        self.recipients = kwargs["to"] if "to" in kwargs else [""]
        self.msg["To"] = ", ".join(self.recipients)

        # Create a multipart/alternative child container.
        msg_body = MIMEMultipart("alternative")

        if "plaintextbody" in kwargs:
            text_part = MIMEText(
                kwargs["plaintextbody"].encode(CHARSET), "plain", CHARSET)
            msg_body.attach(text_part)

        if "htmlbody" in kwargs:
            html_part = MIMEText(
                kwargs["htmlbody"].encode(CHARSET), "html", CHARSET)
            msg_body.attach(html_part)

        self.msg.attach(msg_body)

        if "attachments" in kwargs:
            for attachment in kwargs["attachments"]:
                att = MIMEApplication(open(attachment, "rb").read())
                att.add_header("Content-Disposition", "attachment",
                               filename=os.path.basename(attachment))
                self.msg.attach(att)

    def send_email(self, **kwargs):
        """
        This method sends the actual email using SES service
        :param kwargs: key word arguments
        :return:
        """

        response = None

        self.prepare_email(**kwargs)

        try:
            # Provide the contents of the email.
            response = self.email_ses_client_instance.send_raw_email(
                Source=self.msg["From"],
                Destinations=self.recipients,
                RawMessage={
                    "Data": self.msg.as_string(),
                }
            )
        # Display an error if something goes wrong.
        except Exception as error:
            logging.error("Problem with sending email : %s", error.response['Error']['Message'])
            if kwargs.get("throw_exception"):
                raise EmailDeliveryFailure(error.response['Error']['Message'] or "Unknown Exception") from error
            return error.response['Error']['Message']
        finally:
            if response:
                logging.info("Email sent! Message ID : %s", response['MessageId'])
        return None

