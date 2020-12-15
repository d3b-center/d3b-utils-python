import logging
import os

import requests
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class Session(requests.Session):
    """
    requests.Session that retries on recoverable errors.

    :param session: an existing requests.Session to use (optional)
    :param total: max total retry attempts
        set MAX_RETRIES_ON_CONN_ERROR environment variable to globally override
    :param read: max retries on read errors
    :param connect: max retries on connection errors
    :param status: max retries on bad status codes in status_forcelist
    :param backoff_factor: sleep up to Retry.BACKOFF_MAX between retries for
        backoff_factor * (2 ** (number_of_retries_so_far - 1))
    :param status_forcelist: set of integer HTTP status codes to retry on
    :param allowed_methods: set of uppercased HTTP method verbs to retry on,
        or False means all
    :param \\*\\*kwargs: other urllib3.util.retry.Retry kwargs

    references:
    https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.retry.Retry
    https://2.python-requests.org/en/latest/user/advanced/#transport-adapters
    https://2.python-requests.org/en/master/api/#requests.adapters.HTTPAdapter
    """

    def __init__(
        self,
        total=10,
        read=10,
        connect=10,
        status=10,
        backoff_factor=5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=False,
        method_whitelist=False,  # preserved for backwards compatibility
        **kwargs,
    ):
        self.status_forcelist = status_forcelist
        self.logger = logging.getLogger(type(self).__name__)
        super().__init__()

        # we'll force raise_on_status to False so we can get the server response,
        # because otherwise urllib3 hides the error body after retry exhaustion
        kwargs.pop("raise_on_status", None)

        # environmental retries override (useful for testing)
        total = int(os.environ.get("MAX_RETRIES_ON_CONN_ERROR", total))

        allowed_methods = allowed_methods or method_whitelist
        try:
            retry = Retry(
                total=total,
                read=read,
                connect=connect,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
                allowed_methods=allowed_methods,
                raise_on_status=False,
                **kwargs,
            )
        except TypeError:
            retry = Retry(
                total=total,
                read=read,
                connect=connect,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
                method_whitelist=allowed_methods,
                raise_on_status=False,
                **kwargs,
            )
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

    def send(self, req, **kwargs):
        self.logger.debug(f"Sending request: {vars(req)}")
        resp = super().send(req, **kwargs)
        self.logger.debug(f"Got response:  {vars(resp)}")
        try:
            if resp.status_code in self.status_forcelist:
                resp.raise_for_status()
        except HTTPError:
            self.logger.debug(f"MAX RETRIES EXCEEDED. Error body:  {resp.text}")
            raise
        return resp
