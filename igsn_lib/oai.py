'''

'''

import dateparser
import sickle
import xmltodict
import igsn_lib
import json

# Metadata namespaces commonly seen in IGSN OAI-PMH responses
IGSN_OAI_NAMESPACES = {
    'http://www.w3.org/2001/XMLSchema-instance':'xsi',
    'http://purl.org/dc/elements/1.1/':'dc',
    'http://www.openarchives.org/OAI/2.0/':'oai',
    'http://www.openarchives.org/OAI/2.0/oai_dc/':'oai_dc',
    'http://www.openarchives.org/OAI/2.0/oai-identifier':'oai_identifier',
    'http://igsn.org/schema/kernel-v.1.0':'igsn',
    'http://schema.igsn.org/description/1.0':'igsn_desc',
}

def getSickle(url):
    return sickle.Sickle(url, encoding='utf-8')

def identify(url):
    svc = getSickle(url)
    response = svc.Identify()
    return response

def oaiRecordToDict(raw_record):
    data = {
        'igsn_id': None,
        'creator': None,
        'tstamp': None
    }
    data['source'] = xmltodict.parse(
        raw_record,
        process_namespaces=True,
        namespaces = IGSN_OAI_NAMESPACES
    )
    data['igsn_id'] = None
    data['tstamp'] = dateparser.parse(
        data['source']['oai:record']['oai:header']['oai:datestamp'],
        settings={'TIMEZONE': '+0000'}
    )
    creators = data['source']['oai:record']['oai:metadata']['oai_dc:dc'].get('dc:creator',None)
    if isinstance(creators, str):
        data['creator'] = creators
    else:
        data['creator'] = json.dumps(creators)
    # multiple identifier expressions are common, though they should all be the same IGSN
    igsn_values = data['source']['oai:record']['oai:metadata']['oai_dc:dc']['dc:identifier']
    if isinstance(igsn_values, str):
        data['igsn_id'] = igsn_lib.normalize(igsn_values)
    else:
        #TODO: should probably confirm that all values are the same igsn...
        data['igsn_id'] = igsn_lib.normalize(igsn_values[0])
    return data
