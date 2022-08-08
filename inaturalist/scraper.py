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
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pymongo
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
                 upload_to_s3: bool = False,
                 one_page_only: bool = False,
                 results_per_page: int = 200,
                 start_year: Optional[int] = 2008,
                 end_year: Optional[int] = None,
                 one_year_only: bool = False):
        super(InaturalistPhotoScraper, self).__init__()
        self.taxon_id = taxon_id
        self.output_dir = output_dir
        self.resume_from_page = resume_from_page
        self.stop_at_page = stop_at_page
        self.resume_from_uuid_index = resume_from_uuid_index
        self.upload_to_s3 = upload_to_s3
        self.one_page_only = one_page_only
        self.results_per_page = results_per_page
        self.start_year = start_year
        self.end_year = end_year
        self.one_year_only = one_year_only
        self.s3 = self._s3_client()
        self._logger = self._logger()
        self.data = {
            'uuids': [],
            'observations': [],
            'failed_observations': [],
            'failed_downloads': []
        }
        self.is_large_results = False

    def _s3_client(self) -> Optional[Minio]:
        """Returns a Minio client instance.

        Returns:
            Minio: Minio client instance if `upload_to_s3` is set to `True`.
        """
        s3_client = None
        if self.upload_to_s3:
            s3_endpoint = re.sub(r'https?://', '', os.environ['S3_ENDPOINT'])
            s3_client = Minio(s3_endpoint,
                              access_key=os.environ['S3_ACCESS_KEY'],
                              secret_key=os.environ['S3_SECRET_KEY'])
        return s3_client

    @staticmethod
    def _progress_db():
        client = pymongo.MongoClient(os.environ['MONGODB_CONNECTION_STRING'])
        return client.inat_progress['inat_progress']

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
        logger.info(f'Latest page: {self.resume_from_page}')
        logger.info(f'Latest UUID index: {self.resume_from_uuid_index}')
        if self.start_year:
            logger.info(f' Latest year: {self.start_year}')
        logger.warning(
            f'Failed observations: {self.data["failed_observations"]}')
        logger.warning(f'Failed downloads: {self.data["failed_downloads"]}')
        logger.warning(f'\nKeyboardInterrupt (id: {sig}) has been caught...')
        logger.warning('Terminating the session gracefully...')
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
                     **kwargs) -> Optional[Union[Response, dict]]:
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
            logger.error(f'Failed to get {url}! (ERROR: {e})')
            logger.exception(e)
            self.data['failed_observations'].append(url)
            return
        finally:
            time.sleep(1)

    def get_num_pages(self, on_year: Optional[int] = None) -> tuple:
        """Returns the number of pages.

        Returns:
            tuple: A tuple of (number of pages, number of observations).
        """
        url = 'https://api.inaturalist.org/v2/observations'
        params = {'taxon_id': self.taxon_id}
        if on_year:
            params.update({'year': on_year})
        r = self._get_request(url, params=params)
        if not r:
            sys.exit('Failed to get number of pages!')
        total_results = r['total_results']
        return total_results // self.results_per_page + 1, r['total_results']

    def get_observations(
        self,
        page: int,
        additional_params: Optional[dict] = None
    ) -> Optional[Union[Response, dict]]:
        """Returns a list of observation UUIDs.

        Args:
            page (int): Page number.
            additional_params (dict, optional): Additional parameters to send.

        Returns:
            Union[Response, dict, None]: Response object or observations data.
        """
        url = 'https://api.inaturalist.org/v2/observations'
        params = {
            'taxon_id': self.taxon_id,
            'photos': 'true',
            'page': page,
            'per_page': self.results_per_page,
            'order': 'asc',
            'order_by': 'observed_on',
            'fields': 'uuid,observed_on',
        }
        if additional_params:
            params.update(additional_params)

        observations = self._get_request(url, params=params)
        if not observations:
            logger.error('Could not find observations for this request!')
            return
        return observations

    def _put_object(self,
                    bucket_name: str,
                    object_name: str,
                    data: bytes,
                    content_type: str = 'application/octet-stream'):
        """Uploads an object to S3.

        Args:
            bucket_name (str): Bucket name.
            object_name (str): Object name.
            data (Union[bytes, str]): Data to upload.
            content_type (str, optional): Content type.
        """
        return self.s3.put_object(bucket_name,
                                  object_name,
                                  io.BytesIO(data),
                                  length=-1,
                                  part_size=10 * 1024 * 1024,
                                  content_type=content_type)

    def download_photos(self, observation_uuid) -> Optional[bool]:
        """Downloads photos for a given observation.

        Args:
            observation_uuid (str): Observation UUID.

        Returns:
            bool: Whether the download was successful or not. None if the
                request failed.
        """
        url = \
            f'https://www.inaturalist.org/observations/{observation_uuid}.json'
        logger.debug(f'({observation_uuid}) Requesting observation')

        observation = self._get_request(url, allow_redirects=True)
        if not observation:
            self.data['failed_observations'].append(observation_uuid)
            return

        self.data['observations'].append(observation)
        observation_photos = observation['observation_photos']
        if not observation_photos:
            logger.debug(f'({observation_uuid}) No photos... Skipping...')
            return

        for photo in observation_photos:
            photo_url = photo['photo']['large_url']
            photo_uuid = photo['photo']['uuid']
            logger.debug(f'({photo_uuid}) Downloading...')

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
                    self._put_object(os.environ['S3_BUCKET_NAME'], fname,
                                     json.dumps(self.data).encode())
                except InvalidResponseError:
                    self.data['failed_downloads'].append(photo)
            else:
                with open(Path(f'{self.output_dir}/{fname}'), 'wb') as f:
                    f.write(r.content)
            logger.debug(f'({photo_uuid}) ✅ Downloaded')
        return True

    def _write_progress_collection(self, by_year: bool = False) -> None:
        """Writes the progress collection.

        Args:
            by_year (bool, optional): Whether to write the progress file by
                year.
        """
        logger.info(f'Writing progress for {self.taxon_id} for the first '
                    'time...')
        if by_year:
            progress = {}
            if not self.end_year:
                self.end_year = datetime.now().year
            year_range = range(self.start_year, self.end_year + 1)
            for year in year_range:
                num_pages, _ = self.get_num_pages(on_year=year)
                progress_per_page = {
                    str(k): 'pending'
                    for k in range(num_pages + 1)
                }
                progress[str(year)] = progress_per_page
        else:
            num_pages, _ = self.get_num_pages()
            progress = {str(k): 'pending' for k in range(num_pages)}

        db = self._progress_db()
        db.insert_one({'_id': self.taxon_id, 'progress': progress})

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
        year = str(self.start_year) if self.is_large_results else None

        db = self._progress_db()

        if not db.find_one({'_id': self.taxon_id}):
            if year:
                self._write_progress_collection(by_year=True)
            else:
                self._write_progress_collection()

        progress = db.find_one({'_id': self.taxon_id})['progress']
        if year:
            progress_key = progress[year][page]
        else:
            progress_key = progress[page]

        if mark_as_complete:
            db.update_one({'_id': self.taxon_id},
                          {'$set': {
                              'progress': progress
                          }})
            if year:
                progress[year][page] = 'complete'
            else:
                progress[page] = 'complete'
            logger.info(f'Marking page {page} as complete!')
            return

        if progress_key == 'complete':
            logger.warning(f'Page {page} is already complete!')
            return 1
        elif progress_key == 'in-progress':
            logger.warning(f'Page {page} is already in-progress!')
            return 1
        else:
            logger.info(f'Adding in-progress status to page {page}...')
            if year:
                progress[year][page] = 'in-progress'
            else:
                progress[page] = 'in-progress'

        db.update_one({'_id': self.taxon_id}, {'$set': {'progress': progress}})

    def _get_date(self, sort: str) -> datetime:
        """Gets the datetime object from the current set of observations.

        Args:
            sort (str): Sort.

        Returns:
            datetime: Datetime object.
        """
        url = 'https://api.inaturalist.org/v2/observations'
        resp = self._get_request(url,
                                 params={
                                     'taxon_id': self.taxon_id,
                                     'order_by': 'observed_on',
                                     'order': sort,
                                     'fields': 'uuid,observed_on'
                                 })
        date_str = resp['results'][0]['observed_on']
        date = datetime.strptime(date_str, '%Y-%m-%d')
        return date

    def _parse(self,
               page,
               year: Optional[int] = None) -> Optional[Union[bool, int]]:
        """Parses the page.

        Args:
            page (int): Page number.
            year (int, optional): Year to filter by.

        Returns:
            Returns 1 if the page is complete or in-progress, and None if
                `mark_as_complete` is `True` or page is pending.
        """
        if page == self.stop_at_page:
            logger.warning(f'Stopped at page {page} because '
                           f'`stop_at_page` is set to {self.stop_at_page}.')
            return 2

        logger.info(f'Current page: {page}')

        self.start_year = year
        self.resume_from_page = page

        if os.getenv('MONGODB_CONNECTION_STRING'):
            progress_status = self.check_progress(page)
            if progress_status == 1:
                if self.one_page_only:
                    return 1
                return
        if year:
            observations = self.get_observations(
                page, additional_params={'year': year})
        else:
            observations = self.get_observations(page)
        if not observations:
            return
        uuids = [x['uuid'] for x in observations['results']]

        if not uuids:
            self.data['failed_observations'].append(f'failed page: {page}')
            if self.one_page_only:
                return 1
            else:
                return

        if uuids in self.data['uuids']:
            logger.warning(f'Duplicate response in page {page}! '
                           'Skipping...')
            return
        self.data['uuids'] += uuids
        uuids = uuids[self.resume_from_uuid_index:]

        for n, _uuid in enumerate(uuids, start=self.resume_from_uuid_index):
            self.resume_from_uuid_index = n
            logger.debug(f'Page: {page}, UUID index: {n}')
            self.download_photos(_uuid)

        if self.one_page_only:
            return 1

        if os.getenv('MONGODB_CONNECTION_STRING'):
            self.check_progress(page, mark_as_complete=True)

    def run(self) -> None:
        """Runs the scraper."""
        signal.signal(signal.SIGINT, self._keyboard_interrupt_handler)

        if not self.output_dir:
            self.output_dir = f'downloaded_images_{self.taxon_id}'

        if not self.upload_to_s3:
            Path(self.output_dir).mkdir(exist_ok=True, parents=True)

        num_pages, num_observations = self.get_num_pages()

        pages_range = range(num_pages)
        logger.info(f'Number of pages: {num_pages}')
        logger.info(f'Number of observations: {num_observations}')

        if num_observations > 10000:
            self.is_large_results = True
            if not self.start_year:
                self.start_year = self._get_date('asc').year
            if not self.end_year:
                self.end_year = self._get_date('desc').year

            logger.warning(
                'page * results_per_page > 10,000! '
                f'Will iterate year by year starting from {self.start_year}')

            for year in range(self.start_year, self.end_year + 1):
                num_pages, num_observations = self.get_num_pages(on_year=year)
                logger.info(f'Number of pages for {year}: {num_pages}')
                logger.info(
                    f'Number of observations in {year}: {num_observations}')
                pages_range = range(num_pages)
                pages_range = pages_range[self.resume_from_page:]

                for page in pages_range:
                    resp_code = self._parse(page, year)
                    if resp_code in [1, 2]:
                        if resp_code == 1:
                            logger.warning('Stopped because `one_page_only` '
                                           'is set to `True`.')
                        break
                if self.one_year_only:
                    break

        else:
            pages_range = pages_range[self.resume_from_page:]
            for page in pages_range:
                resp_code = self._parse(page)
                if resp_code in [1, 2]:
                    if resp_code == 1:
                        logger.warning('Stopped because `one_page_only` '
                                       'is set to `True`.')
                    break
