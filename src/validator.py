import json
import os
from typing import Dict, List, Tuple, Optional, Any, Union
import logging

from .utils import setup_logger

logger = setup_logger("validator")

class ValidationError:
    """Class representing a validation error with a severity level."""
    CRITICAL = 'critical'  # Errors that should stop the process
    WARNING = 'warning'    # Issues that are concerning but not fatal
    INFO = 'info'          # Informational notices
    
    def __init__(self, message: str, severity: str = CRITICAL, code: str = None, context: Dict = None):
        self.message = message
        self.severity = severity
        self.code = code
        self.context = context or {}
    
    def __str__(self):
        return f"[{self.severity.upper()}] {self.message}"
    
    def to_dict(self):
        return {
            'message': self.message,
            'severity': self.severity,
            'code': self.code,
            'context': self.context
        }

class CountryDataValidator:
    """Validator for country data integrity."""
    
    # Required fields for each country entry
    REQUIRED_FIELDS = [
        'iso_alpha2', 'iso_alpha3', 'iso_numeric', 
        'country_name', 'continent'
    ]
    
    # Expected data types for specific fields
    FIELD_TYPES = {
        'iso_alpha2': str,
        'iso_alpha3': str,
        'iso_numeric': str,
        'fips': str,
        'country_name': str,
        'capital': str,
        'area_km2': (int, float),
        'population': (int, float),
        'continent': str
    }
    
    # Valid continent codes
    VALID_CONTINENTS = ['AF', 'AS', 'EU', 'NA', 'SA', 'OC', 'AN']
    
    def __init__(self, countries: List[Dict]):
        self.countries = countries
    
    def validate_structure(self) -> List[ValidationError]:
        """Validate the structure of country data."""
        errors = []
        
        if not self.countries:
            errors.append(ValidationError("No country data found", ValidationError.CRITICAL, "NO_DATA"))
            return errors
        
        for i, country in enumerate(self.countries):
            # Check required fields
            for field in self.REQUIRED_FIELDS:
                if field not in country:
                    errors.append(ValidationError(
                        f"Country at index {i} missing required field: {field}",
                        ValidationError.CRITICAL,
                        "MISSING_REQUIRED_FIELD",
                        {'index': i, 'field': field}
                    ))
            
            # Check field types
            for field, expected_type in self.FIELD_TYPES.items():
                if field in country and country[field] is not None:
                    if isinstance(expected_type, tuple):
                        if not any(isinstance(country[field], t) for t in expected_type):
                            errors.append(ValidationError(
                                f"Country at index {i}, field '{field}' has wrong type: {type(country[field]).__name__}, expected one of {[t.__name__ for t in expected_type]}",
                                ValidationError.WARNING,  # Warning rather than critical
                                "WRONG_FIELD_TYPE",
                                {'index': i, 'field': field, 'found_type': type(country[field]).__name__}
                            ))
                    elif not isinstance(country[field], expected_type):
                        errors.append(ValidationError(
                            f"Country at index {i}, field '{field}' has wrong type: {type(country[field]).__name__}, expected {expected_type.__name__}",
                            ValidationError.WARNING,  # Warning rather than critical
                            "WRONG_FIELD_TYPE",
                            {'index': i, 'field': field, 'found_type': type(country[field]).__name__}
                        ))
            
            # Validate continent codes
            if 'continent' in country and country['continent'] not in self.VALID_CONTINENTS:
                errors.append(ValidationError(
                    f"Country at index {i} has invalid continent code: {country['continent']}",
                    ValidationError.WARNING,  # Warning rather than critical
                    "INVALID_CONTINENT",
                    {'index': i, 'found_continent': country['continent']}
                ))
        
        return errors
    
    def validate_data_quality(self) -> List[ValidationError]:
        """Validate the quality of country data."""
        errors = []
        
        # Check if we have a reasonable number of countries
        if len(self.countries) < 100:
            errors.append(ValidationError(
                f"Suspiciously low number of countries: {len(self.countries)}",
                ValidationError.WARNING,
                "LOW_COUNTRY_COUNT",
                {'count': len(self.countries)}
            ))
        
        # Check ISO code patterns
        for i, country in enumerate(self.countries):
            if 'iso_alpha2' in country and not (isinstance(country['iso_alpha2'], str) and len(country['iso_alpha2']) == 2):
                errors.append(ValidationError(
                    f"Country at index {i} has invalid ISO alpha2 code: {country['iso_alpha2']}",
                    ValidationError.WARNING,
                    "INVALID_ISO_ALPHA2",
                    {'index': i, 'found_code': country['iso_alpha2']}
                ))
            
            if 'iso_alpha3' in country and not (isinstance(country['iso_alpha3'], str) and len(country['iso_alpha3']) == 3):
                errors.append(ValidationError(
                    f"Country at index {i} has invalid ISO alpha3 code: {country['iso_alpha3']}",
                    ValidationError.WARNING,
                    "INVALID_ISO_ALPHA3",
                    {'index': i, 'found_code': country['iso_alpha3']}
                ))
        
        return errors
    
    def validate(self) -> Dict[str, Union[bool, List[Dict]]]:
        """Run all validations on country data."""
        structure_errors = self.validate_structure()
        quality_errors = self.validate_data_quality()
        
        all_errors = structure_errors + quality_errors
        
        # Check if there are any critical errors
        has_critical = any(error.severity == ValidationError.CRITICAL for error in all_errors)
        
        return {
            'is_valid': not has_critical,
            'errors': [error.to_dict() for error in all_errors],
            'warnings_count': sum(1 for error in all_errors if error.severity == ValidationError.WARNING),
            'critical_count': sum(1 for error in all_errors if error.severity == ValidationError.CRITICAL)
        }

def compare_country_data(old_data: List[Dict], new_data: List[Dict]) -> Dict:
    """Compare old and new country data to identify changes."""
    changes = {
        'added': [],
        'removed': [],
        'modified': []
    }
    
    # Determine the key to use for comparison (iso_alpha2 or the first column)
    id_key = 'iso_alpha2'
    
    # Check if iso_alpha2 exists in all records, if not use an alternative
    if not all(id_key in country for country in old_data + new_data):
        # Look for alternative key
        alternative_keys = ['iso_alpha2', 'ISO-3166 alpha2', 'alpha2']
        for key in alternative_keys:
            if all(key in country for country in old_data + new_data):
                id_key = key
                logger.info(f"Using alternative key for comparison: {id_key}")
                break
    
    # Create dictionaries keyed by ISO code for easy lookup
    # Handle cases where the key might be missing
    old_dict = {}
    for country in old_data:
        if id_key in country:
            old_dict[country[id_key]] = country
        else:
            logger.warning(f"Country in old data missing {id_key}: {country}")
    
    new_dict = {}
    for country in new_data:
        if id_key in country:
            new_dict[country[id_key]] = country
        else:
            logger.warning(f"Country in new data missing {id_key}: {country}")
    
    # Find added countries
    for iso, country in new_dict.items():
        if iso not in old_dict:
            changes['added'].append(iso)
    
    # Find removed countries
    for iso, country in old_dict.items():
        if iso not in new_dict:
            changes['removed'].append(iso)
    
    # Find modified countries
    for iso, new_country in new_dict.items():
        if iso in old_dict:
            old_country = old_dict[iso]
            # Compare all fields
            for key in set(new_country.keys()) | set(old_country.keys()):
                if key in new_country and key in old_country:
                    if new_country[key] != old_country[key]:
                        if iso not in changes['modified']:
                            changes['modified'].append(iso)
                else:
                    if iso not in changes['modified']:
                        changes['modified'].append(iso)
    
    return changes

def load_jsonl(file_path: str) -> Optional[List[Dict]]:
    """Load data from a JSONL file."""
    if not os.path.exists(file_path):
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [json.loads(line) for line in f]
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {str(e)}")
        return None

def validate_and_compare(new_data: List[Dict], old_data_or_file: Union[str, List[Dict], None] = None) -> Tuple[bool, Dict, Dict]:
    """Validate new data and compare with previous data if available.
    
    Args:
        new_data: List of country dictionaries to validate
        old_data_or_file: Either a file path (str) or the list of old data dictionaries
    
    Returns:
        Tuple of (is_valid, changes_dict, validation_results)
    """
    # Validate new data
    validator = CountryDataValidator(new_data)
    validation_results = validator.validate()
    is_valid = validation_results['is_valid']
    
    # Compare with previous data if available
    changes = {'added': [], 'removed': [], 'modified': []}
    
    # Handle different types of old_data_or_file
    old_data = []
    if old_data_or_file is not None:
        if isinstance(old_data_or_file, str):
            # It's a file path
            if os.path.exists(old_data_or_file):
                old_data = load_jsonl(old_data_or_file) or []
                logger.info(f"Loaded previous data from {old_data_or_file}: {len(old_data)} countries")
        elif isinstance(old_data_or_file, list):
            # It's already the data
            old_data = old_data_or_file
            logger.info(f"Using provided previous data: {len(old_data)} countries")
    
    # If we have old data, compare it
    if old_data:
        logger.info(f"Comparing with previous data ({len(old_data)} countries)")
        changes = compare_country_data(old_data, new_data)
    else:
        # First run - all countries are "added"
        logger.info("No previous data found, considering all countries as new")
        changes['added'] = [country.get('iso_alpha2', f"country_{i}") 
                          for i, country in enumerate(new_data) 
                          if 'iso_alpha2' in country]
    
    return is_valid, changes, validation_results

if __name__ == "__main__":
    # Example usage
    from .scraper import fetch_and_save
    
    output_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "countries.jsonl")
    success, countries = fetch_and_save(output_file)
    
    if success and countries:
        is_valid, changes, validation_results = validate_and_compare(countries)
        
        if not is_valid:
            print("Validation failed with errors:")
            for error in validation_results['errors']:
                print(f"  - {error['message']}")
        else:
            print("Validation successful!")
            
        print("Changes detected:")
        print(f"  - Added: {len(changes['added'])} countries")
        print(f"  - Removed: {len(changes['removed'])} countries")
        print(f"  - Modified: {len(changes['modified'])} countries")
