import os
import sys
import json
import shutil
import tempfile
import fcntl
from pathlib import Path
from datetime import datetime

from .scraper import fetch_and_save
from .validator import validate_and_compare, load_jsonl
from .utils import setup_logger, format_change_summary

logger = setup_logger("main")

def create_backup(file_path):
    """Create a backup of the given file with timestamp."""
    if not os.path.exists(file_path):
        return None
        
    backup_dir = Path(file_path).parent / "backups"
    backup_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{Path(file_path).name}.{timestamp}.bak"
    
    try:
        shutil.copy2(file_path, backup_path)
        logger.info(f"Created backup at {backup_path}")
        return backup_path
    except Exception as e:
        logger.warning(f"Failed to create backup: {str(e)}")
        return None

def atomic_write(file_path, content):
    """Write content to a file atomically to prevent corruption."""
    # Create a temporary file in the same directory
    temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(file_path))
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write(content)
        # Atomic rename
        os.replace(temp_path, file_path)
        return True
    except Exception as e:
        logger.error(f"Error during atomic write: {str(e)}")
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return False

def atomic_write_jsonl(file_path, items):
    """Write a list of items to a JSONL file atomically."""
    content = "\n".join(json.dumps(item, ensure_ascii=False) for item in items)
    return atomic_write(file_path, content)

def acquire_lock(file_path, block=True):
    """Acquire a lock on the specified file."""
    lock_path = f"{file_path}.lock"
    lock_file = open(lock_path, 'w')
    try:
        if block:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
        else:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except IOError:
        lock_file.close()
        return None

def release_lock(lock_file):
    """Release the lock and close the file."""
    if lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

def main():
    """Main entry point for the GeoNames country scraper."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    output_file = project_root / "countries.jsonl"
    changes_file = project_root / ".changes_summary.txt"
    
    # Acquire a lock to ensure we're the only process updating
    lock_file = acquire_lock(str(output_file))
    if not lock_file:
        logger.error("Another process is already updating the data. Exiting.")
        return 1
    
    try:
        logger.info("Starting country data update process")
        
        # Create a backup before making any changes
        if output_file.exists():
            backup_path = create_backup(str(output_file))
            if backup_path:
                logger.info(f"Backup created at {backup_path}")
        
        # Fetch new data but don't save it yet
        success, countries = fetch_and_save(f"{str(output_file)}.temp")
        
        if not success or not countries:
            logger.error("Failed to fetch country data")
            return 1
        
        # Load existing data for comparison
        old_data = load_jsonl(str(output_file)) if output_file.exists() else []
        
        # Validate the new data
        is_valid, changes, validation_results = validate_and_compare(countries, old_data)
        
        # Handle validation issues with different severity levels
        critical_errors = [e for e in validation_results.get('errors', []) if e.get('severity') == 'critical']
        warnings = [e for e in validation_results.get('errors', []) if e.get('severity') == 'warning']
        
        if critical_errors:
            logger.error("Validation failed with critical errors:")
            for error in critical_errors:
                logger.error(f"  - {error.get('message')}")
            return 1
        
        if warnings:
            logger.warning("Validation generated warnings:")
            for warning in warnings:
                logger.warning(f"  - {warning.get('message')}")
        
        # Check if there are changes
        has_changes = any(len(changes[key]) > 0 for key in changes)
        
        if has_changes:
            # Write the new data atomically
            success = atomic_write_jsonl(str(output_file), countries)
            if not success:
                logger.error("Failed to update the JSONL file")
                return 1
            
            # Save the changes summary for the GitHub Action
            changes_summary = format_change_summary(changes)
            atomic_write(str(changes_file), changes_summary)
            
            logger.info(f"Updated country data with changes: {changes_summary}")
        else:
            logger.info("No changes detected in country data")
            # Clean up the temp file
            if os.path.exists(f"{str(output_file)}.temp"):
                os.unlink(f"{str(output_file)}.temp")
        
        logger.info("Country data update process completed successfully")
        return 0
    finally:
        # Always release the lock
        release_lock(lock_file)
        # Clean up any temporary files
        if os.path.exists(f"{str(output_file)}.temp"):
            try:
                os.unlink(f"{str(output_file)}.temp")
            except:
                pass

if __name__ == "__main__":
    sys.exit(main())
