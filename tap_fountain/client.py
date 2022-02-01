import json
import sys
import time
from collections import OrderedDict

import backoff
import requests
import singer
from singer import utils

LOGGER = singer.get_logger()

class Server429Error(Exception):
    pass


class Client:
    def __init__(self, api_token, user_agent=None):
        self.__api_token = api_token
        self.__user_agent = user_agent
        self.__session = requests.Session()

    def post_request(self, url, data):
        return self.request(url=url, method="POST", data=data)

    def get_request(self, url, params=None):
        return self.request(url=url, method="GET", params=params)

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException, Server429Error),
                          max_tries=10,
                          factor=3)
    @utils.ratelimit(150, 60)
    def request(self, url, method, params=None, data=None):
        params = params or {}

        headers = {
            "X-ACCESS-TOKEN": self.__api_token
        }
        if self.__user_agent:
            headers["User-Agent"] = self.__user_agent

        body = data
        if isinstance(data, dict):
            body = json.dumps(data)

        req = requests.Request(method, url, headers=headers, data=body, params=params).prepare()
        LOGGER.info("%s %s", method, req.url)

        with singer.metrics.http_request_timer(url) as timer:
            response = self.__session.send(req)
            timer.tags[singer.metrics.Tag.http_status_code] = response.status_code

        # Use retry functionality in backoff to wait and retry if
        # response code equals 429 because rate limit has been exceeded
        # https://developer.fountain.com/docs/rate-limitations
        if response.status_code == 429:
            raise Server429Error(response.json().get("error", {})
                                 .get("msg", "Too many API requests. See documentation for more information"))

        if response.status_code >= 400:
            LOGGER.error("%s %s [%s - %s]", method, req.url, response.status_code, response.content)
            sys.exit(1)

        # Ensure keys and rows are ordered as received from API
        # return response.json(object_pairs_hook=OrderedDict)
        return response.json()

    def request_pages(self, url, params):
        data = self.get_request(url=url, params=params)
        pages = [data]
        current_cursor, next_cursor = self._extract_cursor(data)
        if current_cursor and next_cursor:
            while current_cursor != next_cursor:
                params.update({"cursor": next_cursor})
                data = self.get_request(url=url, params=params)
                current_cursor, next_cursor = self._extract_cursor(data)
                pages.append(data)
        return pages

    def _extract_cursor(self, response):
        current_cursor = response.get('pagination', {}).get('current_cursor')
        next_cursor = response.get('pagination', {}).get('next_cursor')
        return current_cursor, next_cursor
