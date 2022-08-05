#!/usr/bin/env python
# coding: utf-8

import hashlib
import io
import json
import os
import re
import signal
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Union

import requests
from loguru import logger
from minio import Minio
from minio.error import InvalidResponseError, S3Error
from requests import Response
from requests.exceptions import HTTPError, JSONDecodeError
from requests.structures import CaseInsensitiveDict


class InaturalistPhotoScraper:

    def __init__(self,
                 taxon_id: int,
                 output_dir: Optional[str] = None,
                 resume_from_page: int = 0,
                 stop_at_page: Optional[int] = None,
                 resume_from_uuid_index: int = 0,
                 upload_to_s3: bool = True,
                 one_page_only: bool = False):
        super(InaturalistPhotoScraper, self).__init__()
        self.taxon_id = taxon_id
        self.output_dir = output_dir
        self.resume_from_page = resume_from_page
        self.stop_at_page = stop_at_page
        self.resume_from_uuid_index = resume_from_uuid_index
        self.upload_to_s3 = upload_to_s3
        self.one_page_only = one_page_only
        self.logger = self._logger()
        self.s3 = self._s3_client()
        self.data = {
            'uuids': [],
            'observations': [],
            'failed_observations': [],
            'failed_downloads': []
        }

    def _s3_client(self) -> Optional[Minio]:
        """Returns a Minio client instance.

        Returns:
            Minio: Minio client instance.
        """
        s3_client = None
        if self.upload_to_s3:
            s3_endpoint = re.sub(r'https?://', '', os.environ['S3_ENDPOINT'])
            s3_client = Minio(s3_endpoint,
                              access_key=os.environ['S3_ACCESS_KEY'],
                              secret_key=os.environ['S3_SECRET_KEY'])
        return s3_client

    def _logger(self) -> logger:
        """Returns a logger instance.

        Returns:
            logger: Logger instance.
        """
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
        """Handles keyboard interrupt.

        Args:
            sig (int): Signal number.
            _ (): Unused.
        """
        self.logger.info(f'>>>>>>>>>> Latest page: {self.resume_from_page}')
        self.logger.info(
            f'>>>>>>>>>> Latest UUID index: {self.resume_from_uuid_index}')
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
        """Encodes parameters for GET request.

        Args:
            params (dict): Parameters to encode.

        Returns:
            str: Encoded parameters.
        """
        return '&'.join(
            [f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])

    def _get_request(self,
                     url: str,
                     params: Optional[dict] = None,
                     as_json: bool = True,
                     **kwargs) -> Union[Response, dict, None]:
        """Returns a GET request.

        Args:
            url (str): URL to request.
            params (dict, optional): Parameters to send.
            as_json (bool, optional): Whether to return JSON or not.
            **kwargs: Additional arguments to pass to requests.get().

        Returns:
            Union[Response, dict, None]: Response object or JSON object.
        """
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
            self.data['failed_observations'].append(url)
            return
        finally:
            time.sleep(1)

    def get_num_pages(self) -> int:
        """Returns the number of pages.

        Returns:
            int: Number of pages.
        """
        url = 'https://api.inaturalist.org/v2/observations'
        params = {'taxon_id': self.taxon_id}
        r = self._get_request(url, params=params)
        if not r:
            sys.exit('Failed to get number of pages!')
        total_results = r['total_results']
        return total_results // 200 + 1

    def get_observations_uuids(self, page: int) -> Optional[list]:
        """Returns a list of observation UUIDs.

        Args:
            page (int): Page number.

        Returns:
            list: List of observation UUIDs, or `None` if the request failed.
        """
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

    def download_photos(self, observation_uuid) -> Optional[bool]:
        """Downloads photos for a given observation.

        Args:
            observation_uuid (str): Observation UUID.

        Returns:
            bool: Whether the download was successful or not. None if the
                request failed.
        """
        url = f'https://www.inaturalist.org/observations/{observation_uuid}.json'  # noqa E501
        self.logger.debug(f'({observation_uuid}) Requesting observation')

        observation = self._get_request(url, allow_redirects=True)
        if not observation:
            self.data['failed_observations'].append(observation_uuid)
            return

        self.data['observations'].append(observation)
        observation_photos = observation['observation_photos']
        if not observation_photos:
            self.logger.debug(f'({observation_uuid}) No photos... Skipping...')
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
                self.data['failed_downloads'].append(photo_url)
                continue

            fname = hashlib.md5(r.content).hexdigest() + suffix

            if self.upload_to_s3:
                try:
                    s3_object = self.s3.get_object(
                        os.environ['S3_BUCKET_NAME'], fname)
                    object_etag = s3_object.info()['Etag'].strip('"')
                    if Path(fname).stem == object_etag:
                        logger.warning(
                            'File already exists in the bucket! Skipping...')
                        return True
                except S3Error:
                    pass

                try:
                    self.s3.put_object(os.environ['S3_BUCKET_NAME'],
                                       fname,
                                       io.BytesIO(r.content),
                                       length=-1,
                                       part_size=10 * 1024 * 1024)
                except InvalidResponseError:
                    self.data['failed_downloads'].append(photo)
            else:
                with open(Path(f'{self.output_dir}/{fname}'), 'wb') as f:
                    f.write(r.content)
            self.logger.debug(f'({photo_uuid}) ✅ Downloaded')
        return True

    def check_progress(self,
                       page: Union[int, str],
                       mark_as_complete: bool = False) -> Optional[int]:
        """Checks the progress of the download.

        Args:
            page (int): Page number.
            mark_as_complete (bool, optional): Whether to mark the page as
                complete or not.

        Returns:
            int: Returns 1 if the page is complete or in-progress, and None if
            `mark_as_complete` is `True` or page is pending.
        """
        page = str(page)
        progress_fname = f'{self.taxon_id}_progress.json'

        if mark_as_complete:
            progress_raw = self.s3.get_object(
                os.environ['S3_LOGS_BUCKET_NAME'], progress_fname)
            progress = json.loads(progress_raw.read().decode())
            progress[page] = 'complete'
            return

        logs_objects = self.s3.list_objects(os.environ['S3_LOGS_BUCKET_NAME'])
        progress_exists = [
            x for x in logs_objects if x.object_name == progress_fname
        ]
        if not progress_exists:
            logger.info(
                f'Writing progress file for {self.taxon_id} for the first '
                'time...')
            num_pages = self.get_num_pages()
            progress = {str(k): 'pending' for k in range(num_pages)}
            encoded_json = json.dumps(progress).encode()
            self.s3.put_object(os.environ['S3_LOGS_BUCKET_NAME'],
                               progress_fname,
                               io.BytesIO(encoded_json),
                               length=-1,
                               part_size=10 * 1024 * 1024,
                               content_type='application/json')

        progress_raw = self.s3.get_object(os.environ['S3_LOGS_BUCKET_NAME'],
                                          progress_fname)
        progress = json.loads(progress_raw.read().decode())

        if progress[page] == 'complete':
            logger.warning(f'Page {page} is already complete!')
            return 1
        elif progress[page] == 'in-progress':
            logger.warning(f'Page {page} is already in-progress!')
            return 1
        else:
            logger.info(f'Adding in-progress status to page {page}...')
            progress[page] = 'in-progress'

        encoded_json = json.dumps(progress).encode()
        self.s3.put_object(os.environ['S3_LOGS_BUCKET_NAME'],
                           progress_fname,
                           io.BytesIO(encoded_json),
                           length=-1,
                           part_size=10 * 1024 * 1024,
                           content_type='application/json')

    def _dump_logs(self, page) -> None:
        """Dumps the logs to S3.

        Args:
            page (int): Page number.
        """
        if self.one_page_only:
            logs_fname = f'{self.taxon_id}_page{self.resume_from_page}.json'
        else:
            logs_fname = f'{self.taxon_id}_{time.time()}.json'

        if self.data['failed_observations'] or self.data['failed_downloads']:
            logger.warning(
                'Some or all of the downloads failed! Uploading logs...')
            failed = {
                'failed_observations': self.data['failed_observations'],
                'failed_downloads': self.data['failed_downloads']
            }
            failed = json.dumps(failed).encode()
            self.s3.put_object(os.environ['S3_LOGS_BUCKET_NAME'],
                               logs_fname,
                               io.BytesIO(failed),
                               content_type='application/json',
                               length=-1,
                               part_size=10 * 1024 * 1024)
        self.check_progress(page, mark_as_complete=True)

    def run(self) -> None:
        """Runs the scraper."""
        signal.signal(signal.SIGINT, self._keyboard_interrupt_handler)

        if not self.output_dir:
            self.output_dir = f'downloaded_images_{self.taxon_id}'

        if not self.upload_to_s3:
            Path(self.output_dir).mkdir(exist_ok=True, parents=True)

        num_pages = self.get_num_pages()
        pages_range = range(num_pages)
        self.logger.info(f'Number of pages: {num_pages}')
        self.logger.info(
            f'Estimated number of observations: {num_pages * 200}')

        pages_range = pages_range[self.resume_from_page:]

        for page in pages_range:

            if page == self.stop_at_page:
                break

            self.logger.info(f'Current page: {page}')

            if os.getenv('S3_LOGS_BUCKET_NAME'):
                progress_status = self.check_progress(page)
                if progress_status == 1:
                    if self.one_page_only:
                        break
                    continue

            self.resume_from_page = page
            uuids = self.get_observations_uuids(page)

            if not uuids:
                self.data['failed_observations'].append(f'failed page: {page}')
                if self.one_page_only:
                    break
                else:
                    continue

            if uuids in self.data['uuids']:
                self.logger.warning(f'Duplicate response in page {page}! '
                                    'Skipping...')
                continue
            self.data['uuids'] += uuids
            uuids = uuids[self.resume_from_uuid_index:]

            for n, _uuid in enumerate(uuids,
                                      start=self.resume_from_uuid_index):
                self.resume_from_uuid_index = n
                self.logger.debug(f'Page: {page}, UUID index: {n}')
                self.download_photos(_uuid)

            if self.one_page_only:
                break

            if os.getenv('S3_LOGS_BUCKET_NAME'):
                self._dump_logs(page)
