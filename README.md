# iNaturalist Photo Scraper

[![Supported Python versions](https://img.shields.io/badge/Python-%3E=3.7-blue.svg)](https://www.python.org/downloads/) [![PEP8](https://img.shields.io/badge/Code%20style-PEP%208-orange.svg)](https://www.python.org/dev/peps/pep-0008/) 


## Requirements
- 🐍 [python>=3.7](https://www.python.org/downloads/)


## ⬇️ Installation

```sh
pip install inaturalist
```

## ⌨️ Usage

```
usage: inat [-h] -t TAXON_ID [-o OUTPUT_DIR] [-p RESUME_FROM_PAGE] [-P STOP_AT_PAGE] [-u RESUME_FROM_UUID_INDEX] [--upload-to-s3] [-O]
            [-r RESULTS_PER_PAGE] [-s START_YEAR] [-e END_YEAR] [-Y] [--get-current-progress]

options:
  -h, --help            show this help message and exit
  -t TAXON_ID, --taxon-id TAXON_ID
                        Taxon id
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory
  -p RESUME_FROM_PAGE, --resume-from-page RESUME_FROM_PAGE
                        Page to resume from
  -P STOP_AT_PAGE, --stop-at-page STOP_AT_PAGE
                        Page to stop at
  -u RESUME_FROM_UUID_INDEX, --resume-from-uuid-index RESUME_FROM_UUID_INDEX
                        UUID index to resume from
  --upload-to-s3        Upload to a S3-compatible bucket
  -O, --one-page-only   Terminate after completing a single page
  -r RESULTS_PER_PAGE, --results-per-page RESULTS_PER_PAGE
                        Number of results per page
  -s START_YEAR, --start-year START_YEAR
                        Year to start from (only relevant when number of observations > 10,000)
  -e END_YEAR, --end-year END_YEAR
                        Year to stop at (only relevant when number of observations > 10,000)
  -Y, --one-year-only   Terminate after completing a single year
  --get-current-progress
                        Get current progress
```
