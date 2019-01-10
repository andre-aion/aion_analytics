# HANDLE LOGGING
import logging
import os


def mylogger(__file__):
    # create a custom logger
    dir = os.getcwd().split(os.sep)[-1]
    if dir == 'aion-analytics':
        dir = ''
    else:
        dir='aion-analytics/'
    logfile = dir+'logs/' + os.path.splitext(os.path.basename(__file__))[0] + '.logs'
    logger = logging.getLogger(logfile)
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.WARNING)
    l_format = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
    handler.setFormatter(l_format)
    logger.addHandler(handler)
    logger.warning(logfile)

    return logger