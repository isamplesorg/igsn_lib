"""
Methods in support of OAI-PMH harvesting of IGSN records.
"""

import logging
import dateparser
import sickle
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
'''Metadata namespaces commonly seen in IGSN OAI-PMH responses
'''


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
    return sickle.Sickle(url, encoding="utf-8")


def identify(url):
    """
    Call the OAI-PMH Identify operation on the provided service URL.

    Args:
        url: OAI-PMH service URL

    Returns:
        sickle.Identify object
    """
    svc = getSickle(url)
    response = svc.Identify()
    return response


def oaiRecordToDict(xml_string):
    '''
    Converts an OAI-PMH IGSN metadata record to a dict

    The IGSN schema is at https://doidb.wdc-terra.org//igsn/schemas/igsn.org/schema/1.0/igsn.xsd

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
        logging.debug("No related identifiers in record")
    for related_id in _related_ids:
        entry = {}
        entry["id"] = related_id.get("#text", "")
        entry["id_type"] = related_id.get("@relatedIdentifierType", "")
        entry["rel_type"] = related_id.get("@relationType", "")
        data["related"].append(entry)
    return data
