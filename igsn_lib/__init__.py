"""
Utility methods for working with IGSNs and dates.
"""
import logging
import urllib.parse
import requests

__version__ = "0.1.0"

IGSN_RESOLVER_URL = "http://igsn.org/"
"""Resolver URL. 

Note that this seems to always delegate to hdl.handle.net
"""

N2T_RESOLVER_URL = "https://n2t.net/"
"""N2T Resolver URL. 

N2T will resolve many types of identifiers, including IGSNs
"""

DEFAULT_RESOLVE_HEADERS = {
    "User-Agent": f"igsn_lib/{__version__}",
    "Accept": "application/json, application/ld+json;q=0.9, text/json;q=0.8; text/xml;q=0.7, text/html;q=0.5",
    "Accept-Language": "en-US, en;q=0.9",
}
"""Default headers for talking to the resolver
"""


def normalize(igsn_str):
    """
    Return the value part of an IGSN.

    Note this method does not verify the prefix. The returned
    value may or may not be an actual IGSN.

    Args:
        igsn_str: IGSN string

    Returns:
        string, value part of IGSN.

    Examples:

        .. jupyter-execute::

           import igsn_lib

           # Variants of the fictitious IGSN "ABCD123"
           print(igsn_lib.normalize("10273/AbCd123"))
           print(igsn_lib.normalize("http://hdl.handle.net/10273/ABCD123"))
           print(igsn_lib.normalize("IGSN: abcd123"))
           print(igsn_lib.normalize("info:hdl/10273/ABCD123"))
           print(igsn_lib.normalize("http://some.url/with/perhaps/igsn/ABCD123"))
           print(igsn_lib.normalize("http://igsn.org/ABCD123"))
           # Not an IGSN:
           print(igsn_lib.normalize("info:hdl/20.1000/ABCD123"))
    """
    igsn_str = igsn_str.strip().upper()
    # url or path form
    parts = igsn_str.split("/")
    if len(parts) > 1:
        label = parts[-2].strip()
        candidate = parts[-1].strip()
        if label == "10273":
            return candidate
        if label == "IGSN.ORG":
            return candidate
        if label == "IGSN":
            return candidate
        if label == "IGSN:10273":
            return candidate
        return None
    # igsn:XXX form
    parts = igsn_str.split(":")
    if len(parts) > 1:
        label = parts[-2].strip()
        candidate = parts[-1].strip()
        if label == "IGSN":
            return candidate
        if label == "10273":
            return candidate
        return None
    return igsn_str


def _doResolveStep(url, include_body=False, headers=None, timeout=5):
    L = logging.getLogger("igsn_lib")
    if include_body:
        return requests.get(
            url, headers=headers, allow_redirects=False, timeout=timeout
        )
    return requests.head(url, headers=headers, allow_redirects=False, timeout=timeout)


class FakeResponse(object):
    def __init__(self, d):
        self.__dict__ = d


def _doResolve(url, include_body=False, headers=None, timeout=5, callback=None):
    L = logging.getLogger("igsn_lib")
    responses = []
    c_url = url
    while True:
        cheaders = headers.copy()
        if c_url.startswith(N2T_RESOLVER_URL):
            cheaders["Accept"] = "*/*"
        try:
            do_continue = True
            if callback is not None:
                do_continue = callback(c_url)
            if do_continue:
                response = _doResolveStep(
                    c_url, include_body=include_body, headers=cheaders
                )
            else:
                return responses
        except Exception as e:
            L.error(e)
            fake_response = FakeResponse(
                {
                    "status_code": 0,
                    "url": c_url,
                    "headers": {},
                    "encoding": "",
                    "text": "",
                    "request": {"url": c_url, "headers": cheaders},
                }
            )
            responses.append(fake_response)
            return responses
        responses.append(response)
        if response.status_code >= 400:
            L.warning("Aborting _doResolve on error status %s", response.status_code)
            return responses
        elif response.status_code >= 300:
            c_url = response.headers.get("Location", None)
            if c_url is None:
                L.warning("redirect code but no location! %s", response.status_code)
                return responses
        elif response.status_code >= 200:
            return responses


def resolveN2T(identifier, include_body=False, headers=None, callback=None):
    _L = logging.getLogger("igsn_lib")
    url = f"{N2T_RESOLVER_URL}{urllib.parse.quote(identifier)}"
    n2theaders = DEFAULT_RESOLVE_HEADERS.copy()
    if headers is not None:
        n2theaders.update(headers)
    return _doResolve(
        url, include_body=include_body, headers=n2theaders, callback=callback
    )


def resolve(igsn_value, include_body=False, headers=None):
    """
    Resolve an IGSN value

    #TODO: add support for link headers

    Args:
        igsn_value: pre-normalized IGSN string

    Returns:
        requests.Response

    Examples:

        .. jupyter-execute::

           import json
           import igsn_lib

           res = igsn_lib.resolve('PRR047915')
           print(json.dumps(res.json(), indent=2))
    """
    _L = logging.getLogger("igsn_lib")
    rheaders = DEFAULT_RESOLVE_HEADERS.copy()
    if headers is not None:
        rheaders.update(headers)
    url = f"{IGSN_RESOLVER_URL}{urllib.parse.quote(igsn_value)}"
    _L.debug("Resolve URL = %s", url)
    return _doResolve(url, include_body=include_body, headers=rheaders)
