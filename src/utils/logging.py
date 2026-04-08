import os
import sys
from loguru import logger

working_dir = os.getenv("WORKING_DIR", os.path.dirname(sys.prefix))
LOG_PATH = os.path.join(working_dir, r"info.log")

# Remove any existing handlers to avoid duplicate logs
logger.remove()
logger.add(
    LOG_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} - {message}",
    level="INFO",
)
