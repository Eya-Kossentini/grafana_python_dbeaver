# import logging
# import os
# from logging.handlers import RotatingFileHandler

# # Ensure logs directory exists
# os.makedirs(os.environ.get("_PATH"), exist_ok=True)

# # Create rotating file handler
# file_handler = RotatingFileHandler(
#     os.environ.get("_PATH") + "core.log", maxBytes=10 * 1024 * 1024, backupCount=5
# )
# file_handler.setFormatter(
#     logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
# )

# # Create stream handler for console output
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(
#     logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
# )

# # Configure root logger
# logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])

# # Configure library loggers
# for lib in ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy", "sqlalchemy.engine"):
#     logging.getLogger(lib).setLevel(logging.INFO)

# # Module-level logger (optional)
# logger = logging.getLogger(__name__)

# def get_logger(name: str | None = None) -> logging.Logger:
#     """Utility to get a logger with the given name after global config."""
#     return logging.getLogger(name if name else __name__) 