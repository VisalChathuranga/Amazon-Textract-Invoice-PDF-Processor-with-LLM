"""
Logging configuration module.
Logs to both console and a log file for debugging and monitoring.
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)
