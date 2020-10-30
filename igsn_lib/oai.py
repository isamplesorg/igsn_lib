"""
Methods in support of OAI-PMH harvesting of IGSN records.
"""

import logging
import concurrent.futures
import datetime
import dateparser
import sickle
import sickle.oaiexceptions
import json
import xmltodict
import igsn_lib
import igsn_lib.time

IGSN_OAI_NAMESPACES = {
    "http://www.w3.org/2001/XMLSchema-instance": "xsi",
    "http://purl.org/dc/elements/1.1/": "dc",
    "http://www.openarchives.org/OAI/2.0/": "oai",
    "http://www.openarchives.org/OAI/2.0/oai_dc/": "oai_dc",
    "http://www.openarchives.org/OAI/2.0/oai-identifier": "oai_identifier",
    "http://igsn.org/schema/kernel-v.1.0": "igsn",
    "http://schema.igsn.org/description/1.0": "igsn_desc",
}
IGSN_OAI_NAMESPACES_INV = {v: k for k, v in IGSN_OAI_NAMESPACES.items()}

"""Metadata namespaces commonly seen in IGSN OAI-PMH responses
"""

DEFAULT_METADATA_PREFIX = "igsn"
DEFAULT_THREAD_COUNT = 10
DEFAULT_ENCODING = "utf-8"
DEFAULT_IGSN_OAIPMH_PROVIDER = "https://doidb.wdc-terra.org/igsnoaip/oai"


def _getLogger():
    return logging.getLogger("igsn_lib.oai")


def getSickle(url):
    """
    Create a Sickle instance

    Args:
        url: OAI-PMH service URL

    Returns:
        sickle.Sickle instance
    """
    return sickle.Sickle(url, encoding=DEFAULT_ENCODING)


def identify(url):
    """
    Call the OAI-PMH Identify operation on the provided service URL.

    Args:
        url (str): OAI-PMH service URL

    Returns:
        sickle.Identify object

    Examples:
        .. jupyter-execute::

           import igsn_lib.oai
           import xmltodict
           import json

           url = "https://doidb.wdc-terra.org/igsnoaip/oai"
           res = igsn_lib.oai.identify(url)
           res_dict = xmltodict.parse(
             res.raw,
             process_namespaces=True,
             namespaces=igsn_lib.oai.IGSN_OAI_NAMESPACES
           )
           print(json.dumps(res_dict, indent=2))
    """
    svc = getSickle(url)
    response = svc.Identify()
    return response


def recordCount(
    svc,
    metadata_prefix=DEFAULT_METADATA_PREFIX,
    ignore_deleted=False,
    set_spec=None,
    tfrom=None,
    tuntil=None,
):
    """
    Determine the number of records that match an OAI-PMH ListRecords request.

    Args:
        svc (Sickle): Initialized instance of Sickle
        metadata_prefix (str): Metadata prefix to use (igsn)
        ignore_deleted (bool): Ignore deleted records in the request
        setSpec (str): Optional set name to limit records
        tfrom (str or datetime): Optional representation of time for the earliest record (inclusive)
        tuntil (str or datetime): Optional representation of time for the latest record (inclusive)

    Returns:
        int: Number of records matching the specified subset.

    Examples:

        .. jupyter-execute::

           import igsn_lib.oai

           svc_url = "https://doidb.wdc-terra.org/igsnoaip/oai"
           svc = igsn_lib.oai.getSickle(svc_url)
           count = igsn_lib.oai.recordCount(
             svc,
             set_spec='IEDA',
             tfrom='2020-01-01',
             tuntil='2020-02-01'
           )
           print(f"Matching records = {count}")

    """
    L = _getLogger()
    kwargs = {
        "metadataPrefix": metadata_prefix,
        "set": set_spec,
        "from": None,
        "until": None,
    }
    try:
        kwargs["from"] = igsn_lib.time.datetimeFromSomething(tfrom).strftime(
            igsn_lib.time.OAI_TIME_FORMAT
        )
    except:
        pass
    try:
        kwargs["until"] = igsn_lib.time.datetimeFromSomething(tuntil).strftime(
            igsn_lib.time.OAI_TIME_FORMAT
        )
    except:
        pass
    count = 0
    try:
        response = svc.ListRecords(ignore_deleted=ignore_deleted, **kwargs)
        count = int(response.resumption_token.complete_list_size)
    except sickle.oaiexceptions.NoRecordsMatch as e:
        L.info("No records for set %s @ %s - %s", set_spec, tfrom, tuntil)
    return count


def listSets(svc, get_counts=False):
    """
    List the sets reported by an OAI-PMH service.

    Args:
        svc (Sickle): Initialized instance of Sickle
        get_counts (boolean): Return record counts with set

    Returns:
        list: list of ``{setSpec:, setName:, count:}``

    Examples:
        .. jupyter-execute::

           import igsn_lib.oai

           svc_url = "https://doidb.wdc-terra.org/igsnoaip/oai"
           svc = igsn_lib.oai.getSickle(svc_url)
           sets = igsn_lib.oai.listSets(svc, get_counts=True)
           for s in sets:
             print(f"{s['count']:10} {s['setSpec']}: {s['setName']}")
    """

    def _doCount(_svc, _entry):
        _entry["count"] = recordCount(_svc, set_spec=_entry["setSpec"])
        return _entry

    L = _getLogger()
    result = []
    response = svc.ListSets()
    for s in response:
        entry = {"setSpec": s.setSpec, "setName": s.setName, "count": None}
        result.append(entry)
    if not get_counts:
        return result
    count_result = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=DEFAULT_THREAD_COUNT
    ) as executor:
        futures = []
        for entry in result:
            futures.append(executor.submit(_doCount, svc, entry))
        for future in concurrent.futures.as_completed(futures):
            count_result.append(future.result())
    return count_result

def listRecords(
    svc,
    metadata_prefix=DEFAULT_METADATA_PREFIX,
    ignore_deleted=False,
    set_spec=None,
    tfrom=None,
    tuntil=None):
    L = _getLogger()
    kwargs = {
        "metadataPrefix": metadata_prefix,
        "set": set_spec,
        "from": None,
        "until": None,
    }
    try:
        kwargs["from"] = igsn_lib.time.datetimeFromSomething(tfrom).strftime(
            igsn_lib.time.OAI_TIME_FORMAT
        )
    except:
        pass
    try:
        kwargs["until"] = igsn_lib.time.datetimeFromSomething(tuntil).strftime(
            igsn_lib.time.OAI_TIME_FORMAT
        )
    except:
        pass
    return svc.ListRecords(ignore_deleted=ignore_deleted, **kwargs)


def oaiRecordToDict(xml_string):
    '''
    Converts an OAI-PMH IGSN metadata record to a dict

    The IGSN schema is at https://doidb.wdc-terra.org/igsn/schemas/igsn.org/schema/1.0/igsn.xsd

    Times are returned as timezone aware python datetime, TZ=UTC.

    Args:
        raw_record: OAI-PMH record XML in IGSN format

    Returns:
        dict or None on failure

    Example:

        .. jupyter-execute::

           import pprint
           import igsn_lib.oai

           xml = """<?xml version="1.0"?>
            <record xmlns="http://www.openarchives.org/OAI/2.0/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <header>
                <identifier>oai:registry.igsn.org:6940929</identifier>
                <datestamp>2019-10-15T06:00:10Z</datestamp>
                <setSpec>IEDA</setSpec>
                <setSpec>IEDA.SESAR</setSpec>
              </header>
              <metadata>
                <sample xmlns="http://igsn.org/schema/kernel-v.1.0"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xsi:schemaLocation="http://igsn.org/schema/kernel-v.1.0 http://doidb.wdc-terra.org/igsn/schemas/igsn.org/schema/1.0/igsn.xsd">
                  <sampleNumber identifierType="igsn">10273/BSU0005JF</sampleNumber>
                  <registrant>
                    <registrantName>IEDA</registrantName>
                  </registrant>
                  <log>
                    <logElement event="submitted" timeStamp="2019-10-15T04:00:09Z"/>
                  </log>
                </sample>
              </metadata>
            </record>
           """
           data = igsn_lib.oai.oaiRecordToDict(xml)
           pprint.pprint(data, indent=2)
    '''
    _L = _getLogger()
    data = {
        "igsn_id": None,  # Value of the IGSN identifier
        "oai_id": None,  # Internal OAI-PMH identifier of this record
        "registrant": None,  # registrant name
        "oai_time": None,  # time stamp on the OAI record
        "igsn_time": None,  # submitted or registered time in the log
        "set_spec": [],  # list of setSpec entries for record
        "log": [],  # list of log entries
        "related": [],  # list of related identifiers
        "_source": {},
    }
    try:
        data["_source"] = xmltodict.parse(
            xml_string, process_namespaces=True, namespaces=IGSN_OAI_NAMESPACES
        )
    except Exception as e:
        _L.error(e)
        return None
    # _L.debug(json.dumps(data["_source"], indent=2))
    data["oai_id"] = data["_source"]["oai:record"]["oai:header"]["oai:identifier"]
    # Always store time in UTC
    data["oai_time"] = dateparser.parse(
        data["_source"]["oai:record"]["oai:header"]["oai:datestamp"],
        settings={"TIMEZONE": "+0000"},
    )
    _sample = data["_source"]["oai:record"]["oai:metadata"]["igsn:sample"]
    igsn_id = _sample["igsn:sampleNumber"]["#text"]
    data["igsn_id"] = igsn_lib.normalize(igsn_id)
    data["registrant"] = _sample["igsn:registrant"]["igsn:registrantName"]
    data["set_spec"] = data["_source"]["oai:record"]["oai:header"]["oai:setSpec"]
    # log 'events':
    #   https://doidb.wdc-terra.org//igsn/schemas/igsn.org/schema/1.0/include/igsn-eventType-v1.0.xsd
    igsn_log = _sample["igsn:log"]["igsn:logElement"]
    if isinstance(igsn_log, dict):
        igsn_log = [
            igsn_log,
        ]
    data["log"] = []
    igsn_time = None
    for _log in igsn_log:
        _event = _log["@event"].lower().strip()
        _time = dateparser.parse(
            _log["@timeStamp"],
            settings={"TIMEZONE": "+0000", "RETURN_AS_TIMEZONE_AWARE": True},
        )
        data["log"].append(
            {"event": _event, "time": _time.strftime(igsn_lib.time.JSON_TIME_FORMAT)}
        )
        if _event == "submitted":
            igsn_time = _time
        if _event == "registered":
            # Use registered time if submitted not available
            if igsn_time is None:
                igsn_time = _time
        if _event == "updated":
            # Fall back to updated time
            if igsn_time is None:
                igsn_time = _time
    data["igsn_time"] = igsn_time
    _related_ids = []
    try:
        _related_ids = _sample["igsn:relatedResourceIdentifiers"][
            "igsn:relatedIdentifier"
        ]
        if isinstance(_related_ids, dict):
            _related_ids = [
                _related_ids,
            ]
    except KeyError:
        _L.debug("No related identifiers in record")
    for related_id in _related_ids:
        entry = {}
        entry["id"] = related_id.get("#text", "")
        entry["id_type"] = related_id.get("@relatedIdentifierType", "")
        entry["rel_type"] = related_id.get("@relationType", "")
        data["related"].append(entry)
    return data


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            if isinstance(obj,datetime.datetime):
                return igsn_lib.time.datetimeToJsonStr(obj)
            return str(obj)

def oaiDictRecordToJson(record, indent=2, include_source=False):
    result = record.copy()
    if not include_source:
        result.pop('_source',None)
    return json.dumps(result, cls=DatetimeEncoder, indent=indent)

