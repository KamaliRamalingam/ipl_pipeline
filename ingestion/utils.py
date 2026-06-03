import logging
import os
import time
import functools
from dotenv import load_dotenv


def get_logger(name):
    """Return a logger with a console StreamHandler at INFO level."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def load_env():
    """Load .env file and return a dict of all required environment variables.

    Raises:
        EnvironmentError: if any required variable is missing.
    """
    load_dotenv(override=False)

    required_vars = [
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_CONTAINER_NAME",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
        "DATA_FOLDER",
    ]

    config = {}
    missing = []
    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing.append(var)
        else:
            config[var] = value

    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    return config


def retry(max_attempts=3, delay=2):
    """Decorator that retries a function up to max_attempts times on exception.

    Args:
        max_attempts: Maximum number of attempts before raising the last exception.
        delay: Seconds to wait between retries.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    if attempt < max_attempts:
                        logger.warning(
                            "Attempt %d/%d for '%s' failed: %s. Retrying in %ds...",
                            attempt, max_attempts, func.__name__, exc, delay,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "All %d attempts failed for '%s': %s",
                            max_attempts, func.__name__, exc,
                        )
            raise last_exception
        return wrapper
    return decorator
