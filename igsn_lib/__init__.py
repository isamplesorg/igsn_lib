import logging
import urllib.parse
import requests

__version__ = "0.1.0"

# Resolver URL. Note that this seems to always delegate to hdl.handle.net
IGSN_RESOLVER_URL="http://igsn.org/"

# Default headers for talking to the resolver
DEFAULT_RESOLVE_HEADERS = {
    'User-Agent': f"igsn_lib/{__version__}",
    'Accept':'application/ld+json, application/json, text/xml, text/html'
}



def normalize(igsn_str):
    '''
    Return the value part of an IGSN.

    e.g.:
      "10273/ABCD" -> "ABCD"
      "http://hdl.handle.net/10273/ABCD" -> "ABCD"
      "IGSN: ABCD" -> "ABCD"

    Args:
        igsn_str: IGSN string

    Returns: string, value part of IGSN.
    '''
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
    parts = igsn_str.split(':')
    if len(parts) > 1:
        label = parts[-2].strip()
        candidate = parts[-1].strip()
        if label == "IGSN":
            return candidate
        if label == "10273":
            return candidate
        return None
    return igsn_str


def resolve(igsn_value, headers=DEFAULT_RESOLVE_HEADERS):
    '''
    Resolve an IGSN value

    Args:
        igsn_value: pre-normalized IGSN string

    Returns:
        requests.Response
    '''
    url = f"{IGSN_RESOLVER_URL}{urllib.parse.quote(igsn_value)}"
    logging.debug(url)
    return requests.get(url, headers=headers)
