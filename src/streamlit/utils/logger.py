# File: utils/logger.py
import logging

class AppLogger:
    """Application logging configuration."""
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get configured logger instance."""
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger