import logging.handlers

from config import app_config

log = logging.getLogger('log')
log.setLevel(logging.DEBUG)

file_handler = logging.handlers.RotatingFileHandler(
    app_config.log_file_path,
    maxBytes=app_config.log_maxBytes,
    backupCount=app_config.log_backupCount
)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fmtstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
fmtdate = '%H:%M:%S'
formatter = logging.Formatter(fmtstr, fmtdate)
file_handler.setFormatter(formatter)
ch.setFormatter(formatter)

log.addHandler(file_handler)
log.addHandler(ch)
