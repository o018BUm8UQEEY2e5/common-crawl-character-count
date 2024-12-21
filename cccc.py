#!/usr/bin/python3

from argparse import ArgumentParser
from charset_normalizer import from_bytes
from collections import Counter
from contextlib import contextmanager
from gzip import GzipFile
from json import dump, load
from lxml.html import parse
from os import remove, rename
from pathlib import Path
from random import shuffle
from urllib3 import request
from urllib3.exceptions import HTTPError
from urllib3.util import Retry

@contextmanager
def get(url):
    response = request('GET', url, preload_content=False, retries=Retry(total=None,
                                                                        backoff_factor=0.1,
                                                                        status_forcelist=frozenset((429, 503, 504))))
    if response.status != 200:
        raise HTTPError
    try:
        yield response
    finally:
        response.release_conn()

def order(iterable, random_order=False):
    if random_order:
        l = list(iterable)
        shuffle(l)
        yield from l
    else:
        yield from iterable

parser = ArgumentParser(description='Count up characters in the common crawl.')
parser.add_argument('-r', '--random', action='store_true', default=False, help='process files in random order (default: false)')
parser.add_argument('-v', '--verbose', action='store_true', default=False, help='print out filenames as they are processed (default: false)')
args = parser.parse_args()
prefix = 'https://data.commoncrawl.org/'
counts_subdirectory = Path('counts/')
total = Counter()
with get(prefix + 'crawl-data/index.html') as index:
    for name in order(parse(index).xpath('.//table/tbody/tr/td[1]/a/text()'), random_order=args.random):
        with get(prefix + 'crawl-data/' + name + '/wet.paths.gz') as paths_gz:
            with GzipFile(fileobj=paths_gz) as paths:
                for path in order(paths.read().split(b'\n'), random_order=args.random):
                    path = path.decode('utf-8')
                    filename = Path(path).name
                    if not filename.endswith('.warc.wet.gz'):
                        raise ValueError(f'path filename doesn\'t end with ".warc.wet.gz", got: {filename}')
                    if args.verbose:
                        print(filename)
                    filename = filename.removesuffix('warc.wet.gz') + 'json'
                    filepath = counts_subdirectory / filename
                    if filepath.is_file():
                        with open(filepath, 'r') as json_file:
                            count = load(json_file)
                        for k, v in count.items():
                            total[k] += v
                    else:
                        with get(prefix + path) as segment_gz:
                            with GzipFile(fileobj=segment_gz) as segment:
                                count = Counter()
                                while True:
                                    try:
                                        line = next(segment)
                                    except StopIteration:
                                        break
                                    try:
                                        if line != b'WARC/1.0\r\n':
                                            raise ValueError(f'expected: {b'WARC/1.0\r\n'}, got: {line}')
                                        line = next(segment)
                                        header = {}
                                        while line != b'\r\n':
                                            if not line.endswith(b'\r\n'):
                                                raise ValueError(f'header field doesn\'t end with "\r\n", got: {line}')
                                            line = line.removesuffix(b'\r\n')
                                            match line.split(b': '): # probably shouldn't expect exactly 1 space here
                                                case (b'WARC-Type', value):
                                                    header[b'WARC-Type'] = value
                                                case (b'Content-Length', value):
                                                    header[b'Content-Length'] = value
                                                # add additional fields you care about here
                                                case (_, _):
                                                    pass
                                            line = next(segment)
                                        record = segment.read(int(header[b'Content-Length']))
                                        if header[b'WARC-Type'] == b'conversion':
                                            count.update(str(from_bytes(record).best()))
                                        match (next(segment), next(segment)):
                                            case (b'\r\n', b'\r\n'):
                                                pass
                                            case (a, b):
                                                raise ValueError(f'expected: {b'\r\n\r\n'}, got: {a+b}') 
                                    except StopIteration:
                                        raise ValueError('unexpected end of record')
                        filepath_partial = counts_subdirectory / (filename + '.partial')
                        with open(filepath_partial, 'w') as json_file:
                            dump(count, json_file)
                        rename(filepath_partial, filepath)
                        total += count
with open('grand_total.json.partial', 'w') as json_file:
    dump(count, json_file)
rename('grand_total.json.partial', 'grand_total.json')
