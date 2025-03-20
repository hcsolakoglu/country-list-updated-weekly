import json
import os
from typing import Dict, List, Tuple, Optional
import logging

from .utils import setup_logger

logger = setup_logger("validator")

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
    
    def validate_structure(self) -> Tuple[bool, List[str]]:
        """Validate the structure of country data."""
        errors = []
        
        if not self.countries:
            errors.append("No country data found")
            return False, errors
        
        for i, country in enumerate(self.countries):
            # Check required fields
            for field in self.REQUIRED_FIELDS:
                if field not in country:
                    errors.append(f"Country at index {i} missing required field: {field}")
            
            # Check field types
            for field, expected_type in self.FIELD_TYPES.items():
                if field in country and country[field] is not None:
                    if isinstance(expected_type, tuple):
                        if not any(isinstance(country[field], t) for t in expected_type):
                            errors.append(f"Country at index {i}, field '{field}' has wrong type: {type(country[field]).__name__}, expected one of {[t.__name__ for t in expected_type]}")
                    elif not isinstance(country[field], expected_type):
                        errors.append(f"Country at index {i}, field '{field}' has wrong type: {type(country[field]).__name__}, expected {expected_type.__name__}")
            
            # Validate continent codes
            if 'continent' in country and country['continent'] not in self.VALID_CONTINENTS:
                errors.append(f"Country at index {i} has invalid continent code: {country['continent']}")
        
        return len(errors) == 0, errors
    
    def validate_data_quality(self) -> Tuple[bool, List[str]]:
        """Validate the quality of country data."""
        errors = []
        
        # Check if we have a reasonable number of countries
        if len(self.countries) < 100:
            errors.append(f"Suspiciously low number of countries: {len(self.countries)}")
        
        # Check ISO code patterns
        for i, country in enumerate(self.countries):
            if 'iso_alpha2' in country and not (isinstance(country['iso_alpha2'], str) and len(country['iso_alpha2']) == 2):
                errors.append(f"Country at index {i} has invalid ISO alpha2 code: {country['iso_alpha2']}")
            
            if 'iso_alpha3' in country and not (isinstance(country['iso_alpha3'], str) and len(country['iso_alpha3']) == 3):
                errors.append(f"Country at index {i} has invalid ISO alpha3 code: {country['iso_alpha3']}")
        
        return len(errors) == 0, errors
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Run all validations on country data."""
        structure_valid, structure_errors = self.validate_structure()
        quality_valid, quality_errors = self.validate_data_quality()
        
        all_errors = structure_errors + quality_errors
        return structure_valid and quality_valid, all_errors

def compare_country_data(old_data: List[Dict], new_data: List[Dict]) -> Dict:
    """Compare old and new country data to identify changes."""
    changes = {
        'added': [],
        'removed': [],
        'modified': []
    }
    
    # Create dictionaries keyed by ISO code for easy lookup
    old_dict = {country['iso_alpha2']: country for country in old_data}
    new_dict = {country['iso_alpha2']: country for country in new_data}
    
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

def validate_and_compare(new_data: List[Dict], previous_file: str = None) -> Tuple[bool, Dict, List[str]]:
    """Validate new data and compare with previous data if available."""
    # Validate new data
    validator = CountryDataValidator(new_data)
    is_valid, errors = validator.validate()
    
    # Compare with previous data if available
    changes = {'added': [], 'removed': [], 'modified': []}
    if previous_file and os.path.exists(previous_file):
        old_data = load_jsonl(previous_file)
        if old_data:
            changes = compare_country_data(old_data, new_data)
    
    return is_valid, changes, errors

if __name__ == "__main__":
    # Example usage
    from .scraper import fetch_and_save
    
    output_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "countries.jsonl")
    success, countries = fetch_and_save(output_file)
    
    if success and countries:
        is_valid, changes, errors = validate_and_compare(countries)
        
        if not is_valid:
            print("Validation failed with errors:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("Validation successful!")
            
        print("Changes detected:")
        print(f"  - Added: {len(changes['added'])} countries")
        print(f"  - Removed: {len(changes['removed'])} countries")
        print(f"  - Modified: {len(changes['modified'])} countries")
