import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file=None, level=logging.INFO):
    """Set up and return a logger with appropriate handlers."""
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger  # Logger already set up
    
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        # Ensure logs directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def format_change_summary(changes):
    """Format change summary for commit message and notifications."""
    parts = []
    
    if changes['added']:
        parts.append(f"Added {len(changes['added'])} countries: {', '.join(changes['added'])}")
    
    if changes['removed']:
        parts.append(f"Removed {len(changes['removed'])} countries: {', '.join(changes['removed'])}")
    
    if changes['modified']:
        parts.append(f"Modified {len(changes['modified'])} countries: {', '.join(changes['modified'])}")
    
    if not parts:
        return "No changes detected"
    
    return " | ".join(parts)
