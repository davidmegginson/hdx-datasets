"""Crawl HDX to produce a list of public datasets, with some metadata.
CKAN API documentation: http://docs.ckan.org/en/latest/api/
Python CKAN library: https://github.com/ckan/ckanapi

Started by David Megginson, 2017-12-20
"""

import ckanapi, datetime, time, sys, csv, logging

DELAY = 5
"""Time delay in seconds between requests, to give HDX a break."""

CHUNK_SIZE=100
"""Number of datasets to read at once"""

CKAN_URL = 'https://data.humdata.org'
"""Base URL for the CKAN instance."""

# Open a connection to HDX
ckan = ckanapi.RemoteCKAN(CKAN_URL)

# Open a CSV output stream
output = csv.writer(sys.stdout)

# Set up a logger
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# Iterate through all the datasets ("packages") and resources on HDX
start = 0
result_count = 999999 # just a big, big number; will reset on first search result
output.writerow([
    'Dataset stub',
    'Dataset title',
    'Date created',
    'Date modified',
    'Organization stub',
    'Organization title',
    'Tags',
    'Total resource downloads',
    'Pageviews (last 14 days)'
])
output.writerow([
    '#x_dataset +id',
    '#x_dataset +name',
    '#date +created',
    '#date +modified',
    '#org +id',
    '#org +name',
    '#x_tags',
    '#meta +downloads',
    '#meta +pageviews'
])
while start < result_count:
    result = ckan.action.package_search(start=start, rows=CHUNK_SIZE)
    result_count = result['count']
    logging.info("Read {} package(s)...".format(len(result['results'])))
    for package in result['results']:
        tags = ",".join([tag['name'] for tag in package['tags']])
        output.writerow([
            package['name'],
            package['title'],
            package['metadata_created'][:10],
            package['metadata_modified'][:10],
            package['organization']['name'],
            package['organization']['title'],
            tags,
            package['total_res_downloads'],
            package['pageviews_last_14_days']
        ])
    start += CHUNK_SIZE # next chunk, but first ...
    time.sleep(DELAY) # give HDX a short rest

# end
