import os
import sys
import json
from pathlib import Path

from .scraper import fetch_and_save
from .validator import validate_and_compare, load_jsonl
from .utils import setup_logger, format_change_summary

logger = setup_logger("main")

def main():
    """Main entry point for the GeoNames country scraper."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    output_file = project_root / "countries.jsonl"
    changes_file = project_root / ".changes_summary.txt"
    
    # Fetch and save data
    logger.info("Starting country data update process")
    success, countries = fetch_and_save(str(output_file))
    
    if not success or not countries:
        logger.error("Failed to fetch country data")
        sys.exit(1)
    
    # Load the previous data if it exists
    previous_data = load_jsonl(str(output_file)) if output_file.exists() else None
    
    # Validate the new data
    is_valid, changes, errors = validate_and_compare(countries, str(output_file) if output_file.exists() else None)
    
    if not is_valid:
        logger.error("Validation failed with errors:")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    
    # Check if there are changes
    has_changes = any(changes.values())
    
    if has_changes:
        # Update the JSONL file
        with open(output_file, 'w', encoding='utf-8') as f:
            for country in countries:
                f.write(json.dumps(country, ensure_ascii=False) + '\n')
        
        # Save the changes summary for the GitHub Action
        changes_summary = format_change_summary(changes)
        with open(changes_file, 'w', encoding='utf-8') as f:
            f.write(changes_summary)
        
        logger.info(f"Updated country data with changes: {changes_summary}")
    else:
        logger.info("No changes detected in country data")
    
    logger.info("Country data update process completed successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main())
