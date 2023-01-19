import logging
import logging.config
from logging.handlers import RotatingFileHandler


def init_service_logger():
    logger = logging.getLogger('TRANSFER-LOGGER')
    logging.getLogger('TRANSFER-LOGGER').addHandler(logging.StreamHandler())

    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"logs/transfer.log", encoding="UTF-8")
    # fh = RotatingFileHandler(cfg.LOG_FILE, encoding="UTF-8", maxBytes=100000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.info('TRANSFER-LOGGER started\n---------------------------------')
    return logger


log = init_service_logger()
