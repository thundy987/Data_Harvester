import logging
import os
from datetime import datetime

log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = os.path.join(log_dir, f'app_{timestamp}.log')

logger = logging.getLogger('app_logger')
logger.setLevel(logging.DEBUG)

log_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)

log_file_handler = logging.FileHandler(log_filename)
log_console_handler = logging.StreamHandler()

log_file_handler.setFormatter(log_formatter)
log_console_handler.setFormatter(log_formatter)

logger.addHandler(log_file_handler)
logger.addHandler(log_console_handler)
