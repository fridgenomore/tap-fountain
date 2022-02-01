#!/usr/bin/env python3

import singer
from singer import utils
from tap_fountain.discover import discover
from tap_fountain.sync import do_sync
from tap_fountain.client import Client

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = {
    "api_token",
    "applicants_start_date",
    "user_activity_start_date"
}


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    client = Client(config.get("api_token"))
    state = {}
    if args.state:
        state = args.state

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = discover()
        catalog.dump()
        LOGGER.info("Finished discovery mode")
    # Otherwise run in sync mode
    elif args.catalog:
        do_sync(config, state, args.catalog, client)


if __name__ == '__main__':
    main()
