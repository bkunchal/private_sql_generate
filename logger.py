import logging
import os
from datetime import datetime

def setup_logging(yaml_file_path, log_dir):
    # checking log directory exists; if not, create it
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Extract the YAML file name without the extension
    yaml_file_name = os.path.splitext(os.path.basename(yaml_file_path))[0]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    log_file_name = f"{yaml_file_name}_{timestamp}.log"
    
    # Full path for the log file
    log_file_path = os.path.join(log_dir, log_file_name)
    
    # Use the YAML file name as the logger's name
    logger = logging.getLogger(yaml_file_name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a file handler
    file_handler = logging.FileHandler(log_file_path, mode='w')
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    logger.info(f"Logging setup complete. Log file created at {log_file_path}")

    return logger
