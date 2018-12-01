# HANDLE LOGGING
import logging
import os


def mylogger(__file__):
    # create a custom logger handler
    logfile = 'logs/' + os.path.splitext(os.path.basename(__file__))[0] + '.logs'
    logger = logging.getLogger(logfile)
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.ERROR)
    l_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(l_format)
    logger.addHandler(handler)
    logger.warning(logfile)

    return logger