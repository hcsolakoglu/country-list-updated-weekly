import logging
import json
import time
import random
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import backoff
import os

from .utils import setup_logger

logger = setup_logger("scraper")

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1'
]

class GeoNamesScraper:
    """Scraper for GeoNames country list."""
    
    URL = "https://www.geonames.org/countries/"
    
    def __init__(self, max_retries: int = 5, retry_delay: int = 30):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, 
         requests.exceptions.ConnectionError,
         requests.exceptions.Timeout,
         requests.exceptions.HTTPError),
        max_tries=5, 
        giveup=lambda e: isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in (401, 403, 404)
    )
    def _fetch_page(self) -> str:
        """Fetch the GeoNames country list page with exponential backoff on failure."""
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        logger.info(f"Fetching data from {self.URL}")
        response = requests.get(self.URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Add a random delay to avoid being detected as a bot
        time.sleep(random.uniform(1, 3))
        
        return response.text
    
    def _parse_table(self, html_content: str) -> List[Dict]:
        """Parse the HTML table and extract country data."""
        logger.info("Parsing country data from HTML")
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'id': 'countries'})
        
        if not table:
            raise ValueError("Could not find the countries table in the HTML")
        
        rows = table.find_all('tr')
        
        # Extract headers and normalize them
        headers_row = rows[0]
        headers = []
        for th in headers_row.find_all('th'):
            # Replace <br> with space and get the text
            for br in th.find_all('br'):
                br.replace_with(' ')
            headers.append(th.text.strip())
        
        # Clean up header names for JSON
        header_mapping = {
            'ISO-3166 alpha2': 'iso_alpha2',
            'ISO-3166 alpha3': 'iso_alpha3',
            'ISO-3166 numeric': 'iso_numeric',
            'fips': 'fips',
            'Country': 'country_name',
            'Capital': 'capital',
            'Area in km²': 'area_km2',
            'Population': 'population',
            'Continent': 'continent'
        }
        
        mapped_headers = [header_mapping.get(h, h.lower().replace(' ', '_')) for h in headers]
        
        countries = []
        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) == len(headers):
                country = {}
                for i, cell in enumerate(cells):
                    key = mapped_headers[i]
                    value = cell.text.strip()
                    
                    # Convert numeric values
                    if key in ('area_km2', 'population'):
                        try:
                            value = value.replace(',', '')
                            value = float(value) if '.' in value else int(value)
                        except ValueError:
                            # Keep as string if conversion fails
                            pass
                    
                    country[key] = value
                countries.append(country)
        
        logger.info(f"Successfully parsed {len(countries)} countries")
        return countries
    
    def scrape(self) -> List[Dict]:
        """Scrape the GeoNames country list and return structured data."""
        html_content = self._fetch_page()
        countries = self._parse_table(html_content)
        return countries
    
    def save_to_jsonl(self, data: List[Dict], output_path: str) -> bool:
        """Save the data to a JSONL file with UTF-8 encoding."""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for country in data:
                    f.write(json.dumps(country, ensure_ascii=False) + '\n')
            logger.info(f"Data successfully saved to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to {output_path}: {str(e)}")
            return False

def fetch_and_save(output_path: str) -> Tuple[bool, Optional[List[Dict]]]:
    """Fetch country data and save to JSONL file."""
    scraper = GeoNamesScraper()
    try:
        countries = scraper.scrape()
        success = scraper.save_to_jsonl(countries, output_path)
        return success, countries
    except Exception as e:
        logger.error(f"Error in fetch_and_save: {str(e)}")
        return False, None

if __name__ == "__main__":
    output_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "countries.jsonl")
    success, _ = fetch_and_save(output_file)
    if success:
        print(f"Successfully updated {output_file}")
    else:
        print("Failed to update country data")
