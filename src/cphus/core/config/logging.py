import logging

from colorama import Fore, Style, init

init(autoreset=True)


class Formatter(logging.Formatter):
    """Formatter to add colours to logs."""

    COLORS = {
        "DEBUG": Fore.CYAN,
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        # Keep original values (so they aren’t permanently changed)
        levelname = record.levelname
        message = record.getMessage()

        # Add color to levelname
        if levelname in self.COLORS:
            levelname_color = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
        else:
            levelname_color = levelname

        # Highlight IMPORTANT messages
        if "IMPORTANT" in message:
            message = f"{Fore.YELLOW}{message}{Style.RESET_ALL}"

        # Format datetime with milliseconds
        created = self.formatTime(record, self.datefmt)
        msecs = f"{int(record.msecs):03d}"

        log_line = (
            f"{created},{msecs} {levelname_color} {record.filename}:{record.lineno} -- {message}"
        )
        return log_line


def get_logger(name: str, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if called multiple times
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)

        formatter = Formatter(datefmt="%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)

        logger.addHandler(ch)

    return logger
