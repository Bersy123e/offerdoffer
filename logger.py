import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional

def setup_logger(
    name: str = "commercial_proposal", 
    level: int = logging.INFO, 
    log_file: str = "app.log",
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> logging.Logger:
    """
    Set up and configure logger.
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Log file path
        max_bytes: Maximum log file size in bytes
        backup_count: Number of backup files to keep
        log_format: Log format string
        
    Returns:
        Configured logger
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Only configure if it hasn't been configured already
    if not logger.handlers:
        logger.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(log_format)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Create file handler with UTF-8 encoding
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=max_bytes, 
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Logger {name} initialized")
    
    return logger

class LoggerManager:
    """Manager for creating and retrieving loggers."""
    
    _loggers: Dict[str, logging.Logger] = {}
    
    @classmethod
    def get_logger(
        cls,
        name: str = "commercial_proposal",
        level: int = logging.INFO,
        log_file: Optional[str] = None
    ) -> logging.Logger:
        """
        Get or create a logger with the given name.
        
        Args:
            name: Logger name
            level: Logging level
            log_file: Log file path (default: logs/{name}.log)
            
        Returns:
            Configured logger
        """
        if name not in cls._loggers:
            log_file = log_file or f"logs/{name}.log"
            cls._loggers[name] = setup_logger(
                name=name,
                level=level,
                log_file=log_file
            )
        
        return cls._loggers[name] 