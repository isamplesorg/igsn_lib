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


def resolveN2T(identifier, headers=None):
    _L = logging.getLogger("igsn_lib")
    rheaders = DEFAULT_RESOLVE_HEADERS.copy()
    if headers is not None:
        rheaders.update(headers)
    url = f"{N2T_RESOLVER_URL}{urllib.parse.quote(identifier)}"
    _L.debug("Resolve URL = %s", url)
    return requests.get(url, headers=rheaders)



def resolve(igsn_value, headers=None):
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
    return requests.get(url, headers=rheaders)
