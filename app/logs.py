"""Debug output for this project's code: `F1_LOG_LEVEL=DEBUG make stream` (default INFO)."""

import logging
import os

# "__main__" is the module started with `python -m`, e.g. lakehouse.stream.
PROJECT_LOGGERS = ("__main__", "contract", "detector", "fx_producer", "lakehouse")


def setup_logging() -> None:
    """Third-party libraries (py4j, Kafka client) stay at WARNING so DEBUG shows only our lines."""
    logging.basicConfig(format="%(asctime)s %(levelname)-5s %(name)s | %(message)s", level=logging.WARNING)
    level = os.environ.get("F1_LOG_LEVEL", "INFO").upper()
    for name in PROJECT_LOGGERS:
        logging.getLogger(name).setLevel(level)
