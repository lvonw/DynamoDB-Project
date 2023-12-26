import constants
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Clear log file
with open(constants.LOG_FILE_TRANSFER_SERVICE, 'w') as file:
    pass

# Set format
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt=constants.LOG_DATE_FORMAT)

# Handler for writing to log file
log_handler = RotatingFileHandler(constants.LOG_FILE_TRANSFER_SERVICE, maxBytes=1000000, backupCount=1)
log_handler.setFormatter(log_formatter)

# Handler for writing to console/docker logs
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.addHandler(log_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

def log(msg, level=logging.INFO):
    logger.log(level, msg)