import logging
from logging.handlers import QueueHandler, QueueListener
from os.path import join
from queue import Queue


def get_logger(name: str,
               console: bool = True,
               file_normal: bool = False,
               file_critical: bool = False,
               log_file_path: str = "",
               log_level: int = logging.INFO) -> logging.Logger:
    """
    Gets the logger, sets the base level, removes the already available loggers
    and sets only those required by the arguments given to the function.
    If ```file_normal``` and ```file_critical``` are set, then file logging is set up
    on a separate thread through a :class:`QueueHandler` and :class:`QueueListener`.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    for h in logger.handlers:
        logger.removeHandler(h)

    formatter = logging.Formatter(
        "[%(asctime)s] | [%(name)s > %(funcName)s] | %(levelname)s | %(message)s")

    if console:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)
        # This kind of handler is synchronous
        logger.addHandler(stream_handler)

    # Set up for the asynchronous file loggers
    if file_normal or file_critical:
        folder = log_file_path.strip()
        file_normal_path = f"logs_{name}.log"
        file_critical_path = f"critical_{name}.log"
        if folder != "":
            file_normal_path = join(folder, file_normal_path)
            file_critical_path = join(folder, file_critical_path)
        queue = Queue(-1)
        queue_handler = QueueHandler(queue)
        logger.addHandler(queue_handler)

    if file_normal:
        file_normal_handler = logging.FileHandler(file_normal_path)
        file_normal_handler.setFormatter(formatter)
        file_normal_handler.setLevel(log_level)

    if file_critical:
        file_critical_handler = logging.FileHandler(file_critical_path)
        file_critical_handler.setFormatter(formatter)
        file_critical_handler.setLevel(logging.ERROR)
    
    if file_normal and file_critical:
        queue_listener = QueueListener(queue, file_critical_handler, file_normal_handler, respect_handler_level=True)
    elif file_normal:
        queue_listener = QueueListener(queue, file_normal_handler, respect_handler_level=True)
    elif file_critical:
        queue_listener = QueueListener(queue, file_critical_handler, respect_handler_level=True)

    if file_normal or file_critical:
        queue_listener.start()

    return logger
