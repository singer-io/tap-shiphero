#!/usr/bin/env python3

import json
import sys

import singer
from singer import metadata

from tap_shiphero.client import ShipHeroClient
from tap_shiphero.discover import discover
from tap_shiphero.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'token'
]

def do_discover():
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with ShipHeroClient(parsed_args.config['token'],
                        parsed_args.config.get('user_agent')) as client:

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client,
                 parsed_args.catalog,
                 parsed_args.state,
                 parsed_args.config['start_date'],
                 parsed_args.config.get('end_date'))
