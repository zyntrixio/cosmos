import io
import json
import logging

from prettyprinter import cpprint


class JSONFormatter(logging.Formatter):
    # noinspection PyMissingConstructor
    def __init__(self) -> None:
        pass

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        return json.dumps(
            {
                "timestamp": record.created,
                "level": record.levelno,
                "levelname": record.levelname,
                "process": record.processName,
                "thread": record.threadName,
                "file": record.pathname,
                "line": record.lineno,
                "module": record.module,
                "function": record.funcName,
                "name": record.name,
                "message": record.getMessage(),
            }
        )


class ConsoleFormatter(logging.Formatter):
    # noinspection PyMissingConstructor
    def __init__(self) -> None:
        pass

    @staticmethod
    def _format_with_colour(values: dict) -> str:
        buf = io.StringIO()
        cpprint(values, stream=buf, width=120, end="")
        return buf.getvalue()

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        return self._format_with_colour(
            {
                "level": f"{record.levelname}",
                "logger": record.name,
                "when": self.formatTime(record),
                "where": f"{record.module}.{record.funcName}, line: {record.lineno}",
                "process": record.processName,
                "thread": record.threadName,
                "message": record.getMessage(),
            }
        )
