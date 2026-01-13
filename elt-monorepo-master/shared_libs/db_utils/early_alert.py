from db_utils.bootstrap_logger import setup_bootstrap_logger

def send_early_email_alert(subject: str, message: str):
    logger = setup_bootstrap_logger()
    logger.critical(message)
