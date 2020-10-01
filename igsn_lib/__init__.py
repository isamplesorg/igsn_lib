'''
Utility methods for working with IGSNs
'''
import logging
import urllib.parse
import requests
import datetime
import dateparser
import astropy.time
import astropy.utils.exceptions

__version__ = "0.1.0"

# Resolver URL. Note that this seems to always delegate to hdl.handle.net
IGSN_RESOLVER_URL="http://igsn.org/"

# Default headers for talking to the resolver
DEFAULT_RESOLVE_HEADERS = {
    'User-Agent': f"igsn_lib/{__version__}",
    'Accept':'application/ld+json, application/json, text/xml, text/html'
}

JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
OAI_TIME_FORMAT  = '%Y-%m-%dT%H:%M:%SZ'

def _getLogger():
    return logging.getLogger('igsn_lib')

def dateTimetoJD(dt):
    '''
    Convert a python datetime to Julian date.

    Naive datetime is assumed to be UTC.

    Args:
        dt: datetime.datetime

    Returns:
        float, Julian date
    '''
    # force UTC if no timezone information is provided with dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return astropy.time.Time(dt).jd

def dtnow():
    return datetime.datetime.now(datetime.timezone.utc)

def jdnow():
    '''
    Current Julian date.

    Returns:
        float, Julian date
    '''
    return dateTimetoJD( dtnow())


def jdFromString(tstr):
    '''
    Julian date from string representation of time.

    Uses dateparser.parse to get date time from the string.

    See https://dateparser.readthedocs.io/en/latest/

    Args:
        tstr: string, time in text.

    Returns:
        float, Julian date
    '''
    dt = dateparser.parse(tstr, settings={'TIMEZONE': '+0000'})
    return dateTimetoJD(dt)

def jdToDateTime(jd):
    '''
    Convert Julian date to datetime.

    Note that Julian date values can exceed the range supported by
    python datetime, in which case astropy or datetime will raise
    an exception, and this method will return None.

    Args:
        jd: float, Julian date/

    Returns:
        datetime with UTC timezone or None
    '''
    atime = astropy.time.Time(jd, format='jd')
    try:
        return atime.to_datetime(datetime.timezone.utc)
    except ValueError as e:
        _L = _getLogger()
        _L.error(e)
    return None


def jdToString(jd, format_str):
    dt = jdToDateTime(jd)
    if not dt is None:
        return dt.strftime(format_str)
    return None


def jdToJsonString(jd):
    '''
    Convert Julian date to a JSON datetime string.

    Args:
        jd: float, Julian date

    Returns:
        string or None on error
    '''
    return jdToString(jd, JSON_TIME_FORMAT)


def jdToOAIPMHString(jd):
    '''
    Convert Julian date to an OAI-PMH acceptable datetime string.

    Args:
        jd: float, Julian date

    Returns:
        string or None on error
    '''
    return jdToString(jd, OAI_TIME_FORMAT)



def normalize(igsn_str):
    '''
    Return the value part of an IGSN.

    Example:
      "10273/ABCD" -> "ABCD"
      "http://hdl.handle.net/10273/ABCD" -> "ABCD"
      "IGSN: ABCD" -> "ABCD"

    Args:
        igsn_str: IGSN string

    Returns:
        string, value part of IGSN.
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


def resolve(igsn_value, headers=None):
    '''
    Resolve an IGSN value

    Args:
        igsn_value: pre-normalized IGSN string

    Returns:
        requests.Response
    '''
    #TODO: add support for link headers
    rheaders = DEFAULT_RESOLVE_HEADERS.copy()
    if headers is not None:
        rheaders.update(headers)
    url = f"{IGSN_RESOLVER_URL}{urllib.parse.quote(igsn_value)}"
    logging.debug(url)
    return requests.get(url, headers=rheaders)
