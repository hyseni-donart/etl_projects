import logging


def get_logger(
    log_level=logging.NOTSET, log_format: str = None, filename: str = None
) -> logging.Logger:
    """Configures how logs should be displayed.
    Args:
        log_level (optional): Logging event level. Defaults to INFO.
        log_format (str, optional): Format of the log message. Defaults to None.
        filename (str, optional): Format of the log message. Defaults to None.
    Returns:
        logging.Logger: Logger object with desired configuration.
    """
    if not log_format:
        # noinspection SpellCheckingInspection
        log_format = "%(asctime)s - %(levelname)s ... %(message)s ... %(lineno)s : %(threadName)s"

    if filename:
        # logging.basicConfig(filename=filename, filemode='w', level=log_level, format=log_format, force=True)
        logging.basicConfig(
            handlers=[logging.FileHandler(filename), logging.StreamHandler()],
            level=log_level,
            format=log_format,
            force=True,
        )
    else:
        logging.basicConfig(level=log_level, format=log_format)

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    return logger
