from datetime import datetime, timedelta

import backoff
import requests
import singer
from requests import exceptions
from singer import metrics
from ratelimit import limits, RateLimitException, sleep_and_retry
import simplejson
LOGGER = singer.get_logger()

class Server5xxError(Exception):
    pass

class ShipHeroClient(object):
    BASE_URL = 'https://api-gateway.shiphero.com/v1.2/general-api'

    def __init__(self, token, user_agent):
        self.__token = token
        self.__user_agent = user_agent
        self.__session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          (Server5xxError,
                           RateLimitException,
                           exceptions.Timeout,
                           exceptions.ConnectionError,
                           exceptions.ChunkedEncodingError,
                           simplejson.scanner.JSONDecodeError),
                          max_tries=5,
                          factor=2)
    @sleep_and_retry
    @limits(calls=2, period=1)
    def request(self, method, path, **kwargs):
        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['x-api-key'] = self.__token
        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if 'params' not in kwargs:
            kwargs['params'] = {}
        kwargs['params']['token'] = self.__token

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method,
                                              self.BASE_URL + path,
                                              **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()
        elif response.status_code == 429:
            raise RateLimitException('Rate limit exceeded', 1)


        response.raise_for_status()

        try:
            return response.json()
        except:
            LOGGER.info('response.headers["content-type"]: %s', response.headers['content-type'])

            # This raise lets the backoff retry to kick in
            raise



    def get(self, path, **kwargs):
        return self.request('GET', path, **kwargs)
