"""Общая настройка консольных логов API и consumer."""

import logging
import time
from logging.config import dictConfig


class UTCFormatter(logging.Formatter):
    converter = staticmethod(time.gmtime)


def configure_logging(level: str = "INFO") -> None:
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "()": UTCFormatter,
                    "fmt": "%(asctime)sZ %(levelname)s %(name)s %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "default",
                },
            },
            "root": {"handlers": ["console"], "level": "WARNING"},
            "loggers": {
                "app": {"handlers": ["console"], "level": level, "propagate": False},
                "uvicorn": {"handlers": ["console"], "level": level, "propagate": False},
                "uvicorn.error": {"handlers": [], "level": level, "propagate": True},
                "uvicorn.access": {
                    "handlers": ["console"],
                    "level": level,
                    "propagate": False,
                },
            },
        }
    )
