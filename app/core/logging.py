import logging

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT)
    logging.getLogger("app").setLevel(level.upper())
    # httpx logs every request at INFO; that is noise next to our own per-batch logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
