#!/usr/bin/python3
# coding=utf-8
"""
This scripts provides wrapper over AWS Transcribe
"""
import logging
import traceback

try:
    from alpha_library.helper.hash_calculator import calculate_md5sum
    from alpha_library.boto3_helper.client import Client
except ModuleNotFoundError:
    logging.info("Module called internally")
    from boto3_helper.client import Client
    from helper.hash_calculator import calculate_md5sum


class Transcribe(object):
    """
        This Class handles transcription service
    """

    accepted_lang_codes = ["af-ZA", "ar-AE", "ar-SA", "cy-GB", "da-DK", "de-CH", "de-DE", "en-AB", "en-AU",
                           "en-GB", "en-IE", "en-IN", "en-US", "en-WL", "es-ES", "es-US", "fa-IR", "fr-CA",
                           "fr-FR", "ga-IE", "gd-GB", "he-IL", "hi-IN", "id-ID", "it-IT", "ja-JP", "ko-KR",
                           "ms-MY", "nl-NL", "pt-BR", "pt-PT", "ru-RU", "ta-IN", "te-IN", "tr-TR", "zh-CN"]

    def __init__(self, **kwargs):
        self.aws_details = None

        self.__dict__.update(kwargs)

        self.transcribe_client = Client(aws_details=self.aws_details).return_client(service_name="transcribe")

        logging.debug(f"Instance variables for Transcribe : {self.__dict__}")

    def run_transcription(self, **kwargs):
        """
        https://docs.aws.amazon.com/transcribe/latest/dg/API_StartTranscriptionJob.html
        :param kwargs:
        :return:
        """
        response = None
        try:
            job_name = calculate_md5sum(kwargs["media_file_uri"])
            logging.info(f"Job Name : {job_name}")
            if kwargs["language_code"] in Transcribe.accepted_lang_codes:
                response = self.transcribe_client.start_transcription_job(
                    TranscriptionJobName=job_name,
                    LanguageCode=kwargs["language_code"],
                    Media={
                        "MediaFileUri": kwargs["media_file_uri"]
                    },
                    OutputBucketName=kwargs["s3_details"]["bucket_name"],
                    OutputKey=kwargs["s3_details"]["output_key"]
                )
            else:
                response = self.transcribe_client.start_transcription_job(
                    TranscriptionJobName=job_name,
                    IdentifyLanguage=kwargs["identify_language"],
                    Media={
                        "MediaFileUri": kwargs["media_file_uri"]
                    },
                    OutputBucketName=kwargs["s3_details"]["bucket_name"],
                    OutputKey=kwargs["s3_details"]["output_key"],
                    LanguageOptions=Transcribe.accepted_lang_codes
                )
        except BaseException:
            logging.error(f"Uncaught exception in boto3_helper/transcribe.py : {traceback.format_exc()}")

        return response

    def get_job_status(self, job_name):
        response = None
        try:
            response = self.transcribe_client.start_transcription_job(TranscriptionJobName=job_name)
        except BaseException:
            logging.error(f"Uncaught exception in boto3_helper/transcribe.py : {traceback.format_exc()}")

        return response

