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
usage: inat [-h] -t TAXON_ID [-o OUTPUT_DIR] [-s RESUME_FROM_PAGE]
                  [-e STOP_AT_PAGE] [-u RESUME_FROM_UUID_INDEX]
                  [--upload-to-s3] [--one-page-only]

options:
  -h, --help            show this help message and exit
  -t TAXON_ID, --taxon-id TAXON_ID
                        Taxon id
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory
  -s RESUME_FROM_PAGE, --resume-from-page RESUME_FROM_PAGE
                        Page to resume from
  -e STOP_AT_PAGE, --stop-at-page STOP_AT_PAGE
                        Page to stop at
  -u RESUME_FROM_UUID_INDEX, --resume-from-uuid-index RESUME_FROM_UUID_INDEX
                        UUID index to resume from
  --upload-to-s3        Upload to a S3-compatible bucket
  --one-page-only       Terminate after completing a single page
```
