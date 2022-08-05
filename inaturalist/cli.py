#!/usr/bin/env python
# coding: utf-8

import argparse

from dotenv import load_dotenv

from inaturalist.scraper import InaturalistPhotoScraper


def _opts() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-t',
                        '--taxon-id',
                        help='Taxon id',
                        type=int,
                        required=True)
    parser.add_argument('-o',
                        '--output-dir',
                        help='Output directory',
                        type=str)
    parser.add_argument('-p',
                        '--resume-from-page',
                        help='Page to resume from',
                        type=int,
                        default=0)
    parser.add_argument('-e',
                        '--stop-at-page',
                        help='Page to stop at',
                        type=int)
    parser.add_argument('-u',
                        '--resume-from-uuid-index',
                        help='UUID index to resume from',
                        type=int,
                        default=0)
    parser.add_argument('--upload-to-s3',
                        help='Upload to a S3-compatible bucket',
                        action='store_true')
    parser.add_argument('--one-page-only',
                        help='Terminate after completing a single page',
                        action='store_true')
    return parser.parse_args()


def main() -> None:
    """Main function."""
    load_dotenv()
    args = _opts()
    scraper = InaturalistPhotoScraper(
        taxon_id=args.taxon_id,
        output_dir=args.output_dir,
        resume_from_page=args.resume_from_page,
        stop_at_page=args.stop_at_page,
        resume_from_uuid_index=args.resume_from_uuid_index,
        upload_to_s3=args.upload_to_s3,
        one_page_only=args.one_page_only)
    scraper.run()


if __name__ == '__main__':
    main()