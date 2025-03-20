# GeoNames Country Data Tracker

This project automatically fetches the table from the [GeoNames Country List](https://www.geonames.org/countries/) webpage once a week and updates it in a GitHub repository in JSONL format, but only when changes occur.

## Features

### Data Extraction & Scraping
- Regularly processes the table from the GeoNames webpage
- Saves data in JSONL format with UTF-8 encoding (ensure_ascii=False)
- Uses BeautifulSoup for HTML parsing

### GitHub Automation
- Uses GitHub Actions to fetch data weekly
- Commits only if there are actual changes in the JSONL file
- Specifies what changed in each commit message

### Error Tolerance & Reliability
- Implements error handling for connection issues, broken HTML, and missing data
- Uses an exponential backoff mechanism for retrying failed attempts
- Includes a comprehensive logging system

### Testing & Validation
- Tests connection status
- Validates extracted data for missing or corrupted entries
- Checks JSONL file format and content
- Validates data types and formats
- Compares previous and new JSONL files
- Implements rate limit and bot protection measures
- Uses User-Agent spoofing to bypass bot protections
- Introduces waiting periods to prevent excessive requests

### Notifications & Monitoring
- Creates an issue on GitHub after each update
- Implements a logging mechanism for successful and failed updates

## How It Works

1. A GitHub Action runs weekly (Monday at 00:00 UTC)
2. The script fetches the GeoNames country list
3. The data is validated for structure and quality
4. Changes are detected by comparing with the previous version
5. If changes are found, the JSONL file is updated
6. The changes are committed with a detailed message
7. A GitHub issue is created with a summary of the changes

## Local Development

### Prerequisites
- Python 3.8+
- pip

### Setup
```bash
# Clone the repository
git clone [repository-url]
cd geonames

# Install dependencies
pip install -r requirements.txt

# Run the script manually
python -m src.main
```

## License

This project is open source and available under the [MIT License](LICENSE).
