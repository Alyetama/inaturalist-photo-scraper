#!/usr/bin/env python
# coding: utf-8

import textwrap
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from inaturalist.scraper import InaturalistPhotoScraper
from loguru import logger
from tqdm import tqdm


def github_actions_strategy(taxon_id: int,
                            start_year: int = 2008,
                            end_year: Optional[int] = None):
    if not start_year:
        logger.info(f'`start_year` is {start_year}')

    if not end_year:
        end_year = datetime.today().year
        logger.info(f'`end_year` is {end_year}.')

    scraper = InaturalistPhotoScraper(taxon_id=taxon_id)

    data = {str(k): {} for k in range(start_year, end_year + 1)}

    for year in tqdm(range(start_year, end_year + 1)):
        num_pgs, num_obsv = scraper.get_num_pages(on_year=year)
        data[str(year)].update({
            'num_pages': num_pgs,
            'num_observations': num_obsv
        })

    pgs_per_yr = {v['num_pages']: [] for v in data.values()}
    for k, v in data.items():
        pgs_per_yr[v['num_pages']].append(k)

    r = requests.get(
        'https://gist.githubusercontent.com/Alyetama/bfd73b5130da8d3bf2103405cdd16a73/raw/b616f15f3ce62e7926c5ec59fef01669d1d6fc88/styles.css'
    )  # noqa
    if r.status_code == 200:
        styles = textwrap.dedent(r.text)
    else:
        styles = None

    with open(f'{taxon_id}_progress.html', 'w') as f:
        f.write(f'<html><style type="text/css">{styles}</style>'
                '<table class="table table-striped table-bordered">'
                '<thead>\n<tr>\n<th>Finished</th>'
                '<th>Array of years to process</th>'
                '<th>Array of page numbers to process</th>\n</tr>'
                '</thead>\n<tbody>\n')

        f.write('<p><strong>'
                'Any changes will be discarded when this page is closed.'
                '</strong></p>\n')

        strat = {}
        i = 0
        for k, v in pgs_per_yr.items():
            i += 1
            v = list(map(int, v))
            iterator = list(range(1, k + 1))
            if k > 10:
                n = max(1, 10)
                chunks = list(iterator[i:i + n]
                              for i in range(0, len(iterator), n))
                for chunk in chunks:
                    lines = (f'''\
                        <tr>
                        <td><input type="checkbox" id="checkbox{i}">
                        <label for="checkbox{i}">✅</label></td>
                        <td>{v}</td>
                        <td>{chunk}</td>
                        </tr>\n''')
                    f.write(textwrap.dedent(lines))
            else:
                lines = (f'''\
                    <tr>
                    <td><input type="checkbox" id="checkbox{i}">
                    <label for="checkbox{i}">✅</label></td>
                    <td>{v}</td>
                    <td>{iterator}</td>
                    </tr>\n''')
                f.write(textwrap.dedent(lines))

        f.write('''</tbody>\n</table>\n</body></html>\n''')

    p = 'file://' + str(Path(f'{taxon_id}_progress.html').absolute())
    webbrowser.open(p)
