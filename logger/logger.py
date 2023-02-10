"""
Custom logger. Level INFO by default, can be changed to DEBUG if CLD is ran with --debug option.
"""

import logging
from time import gmtime, strftime


class CustomFormatter(logging.Formatter):

    grey = "\x1b[30;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[36;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s    %(name)s    %(levelname)s    %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: blue + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %I:%M:%S')
        return formatter.format(record)


now = strftime("%Y-%m-%d_%H-%M-%S", gmtime())

logger = logging.getLogger("neointerface")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

logger.info("-------------------------------   Loaded neointerface Logger    -------------------------------")
