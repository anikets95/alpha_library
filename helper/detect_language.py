#!/usr/bin/python3
# coding= utf-8
"""
This script detects language of the data provided
"""
import logging

import fasttext


class LanguageIdentification:
    def __init__(self):
        pretrained_lang_model_path = "/tmp/lid.176.ftz"
        self.model = fasttext.load_model(pretrained_lang_model_path)

    def predict_lang(self, text):
        predictions = self.model.predict(text)
        lang_code = predictions[0][0].split('__')[-1]
        lang_prob = predictions[1][0]
        return lang_code, lang_prob


if __name__ == '__main__':
    # LOGGING #
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging_format = "%(asctime)s::%(filename)s::%(funcName)s::%(lineno)d::%(levelname)s:: %(message)s"
    logging.basicConfig(format=logging_format, level=logging.DEBUG, datefmt="%Y/%m/%d %H:%M:%S:%Z(%z)")
    logger = logging.getLogger(__name__)

    LANGUAGE = LanguageIdentification()
    lang_code, lang_prob = LANGUAGE.predict_lang("O Que Tem Pra Hoje? - Philips Walita")
    print(f"{lang_code}: {lang_prob}")
