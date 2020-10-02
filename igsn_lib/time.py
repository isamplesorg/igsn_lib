"""
Time handling routines for igsn_lib.
"""
import logging
import datetime
import dateparser
import astropy.time
import astropy.utils.exceptions

JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
"""datetime format string for generating JSON content
"""

OAI_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
"""datetime format string for generating OAI-PMH requests
"""


def _getLogger():
    return logging.getLogger("igsn_lib.time")


def datetimeToJD(dt):
    """
    Convert a python datetime to Julian date.

    Naive datetime is assumed to be UTC.

    Args:
        dt: datetime.datetime

    Returns:
        float, Julian date

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           import datetime

           dt = datetime.datetime.now(datetime.timezone.utc)
           print(dt)
           print(igsn_lib.time.datetimeToJD(dt))
    """
    # force UTC if no timezone information is provided with dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return astropy.time.Time(dt).jd


def dtnow():
    """
    Get datetime for now in UTC timezone.

    Returns:
        datetime.datetime with UTC timezone

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           print(igsn_lib.time.dtnow())
    """
    return datetime.datetime.now(datetime.timezone.utc)


def jdnow():
    """
    Current Julian date.

    Returns:
        float, Julian date

    Example:

       .. jupyter-execute::

          import igsn_lib.time
          print(igsn_lib.time.jdnow())
    """
    return datetimeToJD(dtnow())


def jdFromString(tstr):
    """
    Julian date from string representation of time.

    Uses dateparser.parse to get date time from the string.

    See https://dateparser.readthedocs.io/en/latest/

    Args:
        tstr: string, time in text.

    Returns:
        float, Julian date

    Example:

        .. jupyter-execute::

           import igsn_lib.time

           print(igsn_lib.time.jdFromString("now"))
           print(igsn_lib.time.jdFromString("100 years ago"))
           print(igsn_lib.time.jdFromString("2020-08-15 16:30:00 ET"))
    """
    dt = dateparser.parse(tstr, settings={"TIMEZONE": "+0000"})
    return datetimeToJD(dt)


def jdToDateTime(jd):
    """
    Convert Julian date to datetime.

    Note that Julian date values can exceed the range supported by
    python datetime, in which case astropy or datetime will raise
    an exception, and this method will return None.

    Args:
        jd: float, Julian date/

    Returns:
        datetime with UTC timezone or None

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           print(igsn_lib.time.jdToDateTime(2459125.5))
    """
    atime = astropy.time.Time(jd, format="jd")
    try:
        return atime.to_datetime(datetime.timezone.utc)
    except ValueError as e:
        _L = _getLogger()
        _L.error(e)
    return None


def jdToString(jd, format_str):
    """
    Convert Julian date to a string representation

    Args:
        jd: float, Julian date
        format_str: python strftime format string

    Returns:
        string

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           print(igsn_lib.time.jdToString(2459125.5, "%Y%m%d"))
    """
    dt = jdToDateTime(jd)
    if not dt is None:
        return dt.strftime(format_str)
    return None


def jdToJsonString(jd):
    """
    Convert Julian date to a JSON datetime string.

    Args:
        jd: float, Julian date

    Returns:
        string or None on error

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           print(igsn_lib.time.jdToJsonString(2459125.5))
    """
    return jdToString(jd, JSON_TIME_FORMAT)


def jdToOAIPMHString(jd):
    """
    Convert Julian date to an OAI-PMH acceptable datetime string.

    Args:
        jd: float, Julian date

    Returns:
        string or None on error

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           print(igsn_lib.time.jdToOAIPMHString(2459125.5))
    """
    return jdToString(jd, OAI_TIME_FORMAT)
