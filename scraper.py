#!/usr/bin/env python
# coding: utf-8

import argparse
import hashlib
import io
import os
import re
import signal
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from loguru import logger
from minio import Minio
from requests.exceptions import HTTPError, JSONDecodeError
from requests.structures import CaseInsensitiveDict


class iNatPhotoScraper:

    def __init__(self,
                 taxon_id: int,
                 output_dir: Optional[str] = None,
                 latest_page: int = 0,
                 latest_uuid_index: int = 0,
                 upload_to_s3: bool = True):
        super(iNatPhotoScraper, self).__init__()
        self.taxon_id = taxon_id
        self.output_dir = output_dir
        self.latest_page = latest_page
        self.latest_uuid_index = latest_uuid_index
        self.upload_to_s3 = upload_to_s3
        self.logger = self._logger()
        self.s3 = self._s3_client()
        self.data = {
            'uuids': [],
            'observations': [],
            'failed_observations': [],
            'failed_downloads': []
        }

    def _s3_client(self):
        s3_client = None
        if self.upload_to_s3:
            s3_endpoint = re.sub(r'https?:\/\/', '', os.environ['S3_ENDPOINT'])
            s3_client = Minio(s3_endpoint,
                              access_key=os.environ['S3_ACCESS_KEY'],
                              secret_key=os.environ['S3_SECRET_KEY'])
        return s3_client

    def _logger(self):
        logger.remove()
        Path('logs').mkdir(exist_ok=True)
        logger.add(
            sys.stderr,
            format='{level.icon} <fg #3bd6c6>{time:HH:mm:ss}</fg #3bd6c6> | '
            '<level>{level: <8}</level> | '
            '<lvl>{message}</lvl>',
            level='DEBUG')
        logger.level('WARNING', color='<yellow><bold>', icon='🚧')
        logger.level('INFO', color='<bold>', icon='🚀')
        logger.add(f'logs/{self.taxon_id}.log')
        return logger

    def _keyboard_interrupt_handler(self, sig: int, _) -> None:
        self.logger.info(f'>>>>>>>>>> Latest page: {self.latest_page}')
        self.logger.info(
            f'>>>>>>>>>> Latest UUID index: {self.latest_uuid_index}')
        self.logger.warning(
            f'Failed observations: {self.data["failed_observations"]}')
        self.logger.warning(
            f'Failed downloads: {self.data["failed_downloads"]}')
        self.logger.warning(
            f'\nKeyboardInterrupt (id: {sig}) has been caught...')
        self.logger.warning('Terminating the session gracefully...')
        sys.exit(1)

    @staticmethod
    def _encode_params(params: dict) -> str:
        return '&'.join(
            [f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])

    def _get_request(self,
                     url: str,
                     params: Optional[dict] = None,
                     as_json: bool = True,
                     **kwargs):
        headers = CaseInsensitiveDict()
        headers['accept'] = 'application/json'

        if params:
            encoded_params = self._encode_params(params)
            url = f'{url}?{encoded_params}'

        try:
            r = requests.get(url, headers=headers, **kwargs)
            r.raise_for_status()
            if as_json:
                return r.json()
            else:
                return r
        except (HTTPError, JSONDecodeError) as e:
            self.logger.error(f'Failed to get {url}! (ERROR: {e})')
            self.logger.exception(e)
            return
        finally:
            time.sleep(1)

    def _get_num_pages(self):
        url = 'https://api.inaturalist.org/v2/observations'
        params = {'taxon_id': self.taxon_id}
        total_results = self._get_request(url, params=params)['total_results']
        return total_results // 200 + 1

    def get_observations_uuids(self, page: int) -> list:
        url = 'https://api.inaturalist.org/v2/observations'
        params = {
            'taxon_id': self.taxon_id,
            'photos': 'true',
            'page': page,
            'per_page': 200,
            'order': 'asc',
            'order_by': 'created_at',
            'fields': 'uuid'
        }

        resp_json = self._get_request(url, params=params)
        if not resp_json:
            return

        uuids = [x['uuid'] for x in resp_json['results']]
        return uuids

    def download_photos(self, _uuid):
        url = f'https://www.inaturalist.org/observations/{_uuid}.json'
        self.logger.debug(f'({_uuid}) Requesting observation')

        observation = self._get_request(url, allow_redirects=True)
        if not observation:
            self.data['failed_observations'].append(_uuid)

        self.data['observations'].append(observation)
        observation_photos = observation['observation_photos']
        if not observation_photos:
            self.logger.debug(f'({_uuid}) No photos... Skipping...')
            return

        for photo in observation_photos:
            photo_url = photo['photo']['large_url']
            photo_uuid = photo['photo']['uuid']
            self.logger.debug(f'({photo_uuid}) Downloading...')

            suffix = Path(photo_url).suffix.lower()
            if not suffix or suffix == '.jpeg':
                suffix = '.jpg'

            r = self._get_request(photo_url, as_json=False)
            if not r:
                self.logger.error(
                    f'Could not download {photo_url}! (ERROR: {e})')
                self.data['failed_downloads'].append(photo_url)
                continue

            fname = hashlib.md5(r.content).hexdigest() + suffix

            if self.upload_to_s3:
                self.s3.put_object(os.environ['S3_BUCKET_NAME'],
                                   fname,
                                   io.BytesIO(r.content),
                                   length=-1,
                                   part_size=10 * 1024 * 1024)
            else:
                with open(Path(f'{self.output_dir}/{fname}'), 'wb') as f:
                    f.write(r.content)
            self.logger.debug(f'({photo_uuid}) ✅ Downloaded')

    def run(self):
        signal.signal(signal.SIGINT, self._keyboard_interrupt_handler)

        if not self.output_dir:
            self.output_dir = f'downloaded_images_{self.taxon_id}'

        if not self.upload_to_s3:
            Path(self.output_dir).mkdir(exist_ok=True, parents=True)

        num_pages = self._get_num_pages()
        pages_range = range(num_pages)
        self.logger.info(f'Number of pages: {num_pages}')
        self.logger.info(
            f'Estimated number of observations: {num_pages * 200}')

        pages_range = pages_range[self.latest_page:]

        for page in pages_range:
            self.logger.info(f'Current page: {page}')
            self.latest_page = page
            uuids = self.get_observations_uuids(page)
            if uuids in self.data['uuids']:
                self.logger.warning(f'Duplicate response in page {page}! '
                                    'Skipping...')
                continue
            self.data['uuids'] += uuids
            uuids = uuids[self.latest_uuid_index:]

            for n, _uuid in enumerate(uuids, start=self.latest_uuid_index):
                self.latest_uuid_index = n
                self.logger.debug(f'Page: {page}, UUID index: {n}')
                self.download_photos(_uuid)


def _opts() -> argparse.Namespace:
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
                        '--latest-page',
                        help='Page to resume from',
                        type=int,
                        default=0)
    parser.add_argument('-u',
                        '--latest-uuid-index',
                        help='UUID index to resume from',
                        type=int,
                        default=0)
    parser.add_argument('-s',
                        '--upload-to-s3',
                        help='Upload to a S3-compatible bucket',
                        action='store_true')
    return parser.parse_args()


if __name__ == '__main__':
    load_dotenv()
    args = _opts()
    scraper = iNatPhotoScraper(taxon_id=args.taxon_id,
                               output_dir=args.output_dir,
                               latest_page=args.latest_page,
                               latest_uuid_index=args.latest_uuid_index,
                               upload_to_s3=args.upload_to_s3)
    scraper.run()
