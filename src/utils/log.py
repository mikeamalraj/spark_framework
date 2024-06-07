import logging
import sys


class LogUtils:

    def __init__(self, application_name, format_string):
        self.application_name = application_name
        self.formatter = logging.Formatter(format_string)

    def get_logger(self, level=logging.INFO):
        logger = logging.getLogger(self.application_name)
        logger.setLevel(level)

        if not logger.handlers:
            ch = logging.StreamHandler(sys.stderr)
            ch.setLevel(level)
            formatter = self.formatter
            ch.setFormatter(formatter)
            logger.addHandler(ch)

        return logger
