import requests
import argparse
import json
import gzip
import csv
import codecs

from bs4 import BeautifulSoup
from io import BytesIO

# parse the command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-d", "--domain", required=True, help="The domain to target")
args = vars(ap.parse_args())

domain = args['domain']

# list of available indices
index_list = ["2018-09", "2018-05", ] #"2017-51", "2017-47", "2017-43", "2017-39", "2017-34", "2017-30", "2017-26", "2017-22", "2017-17", "2017-13", "2017-09", "2017-04", "2016-50", "2016-44", "2016-40", "2016-36", "2016-30", "2016-26", "2016-22", "2016-18", "2016-07", "2015-48", "2015-40", "2015-35", "2015-32", "2015-27", "2015-22", "2015-18", "2015-14", "2015-11", "2015-06", "2014-52", "2014-49", "2014-42", "2014-41", "2014-35", "2014-23", "2014-15", "2014-10", "2013-48", "2013-20"]


def search_domain(domain):
    """
    Searches the Common Crawl Index for a domain.
    """

    record_list = []

    print("[*] Trying target domain: %s" % domain)

    for index in index_list:

        print("[*] Trying index %s" % index)

        cc_url = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        cc_url += "url=%s&matchType=domain&output=json" % domain

        response = requests.get(cc_url)

        if response.status_code == 200:

            records = response.content.splitlines()

            for record in records:
                record_list.append(json.loads(record))

            print("[*] Added %d results." % len(records))

    print("[*] Found a total of %d hits." % len(record_list))

    return record_list


def download_page(record):
    """
    Downloads a page from Common Crawl.
    Adapted graciously from @Smerity -
    https://gist.github.com/Smerity/56bc6f21a8adec920ebf
    """

    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1

    prefix = 'https://aws-publicdatasets.s3.amazonaws.com/'

    # We can then use the Range header to ask for just this set of bytes
    resp = requests.get(
        prefix + record['filename'],
        headers={'Range': 'bytes={}-{}'.format(offset, offset_end)}
    )

    # The page is stored compressed (gzip) to save space
    # We can extract it using the GZIP library
    raw_data = BytesIO(resp.content)
    print(raw_data.getvalue())

    f = gzip.GzipFile(fileobj=raw_data)

    # What we have now is just the WARC response, formatted:
    data = f.read()

    response = ""

    if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
        except:
            pass

    return response


record_list = search_domain(domain)

for record in record_list:
    print(record)
    html_content = download_page(record)
    print("[*] Retrieved %d bytes for %s" % (len(html_content), record['url']))
