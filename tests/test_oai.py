import pytest
import igsn_lib
import igsn_lib.oai

igsn_record_values = [
    ("""<record xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<header>
    <identifier>oai:registry.igsn.org:18209</identifier>
    <datestamp>2013-06-19T17:28:22Z</datestamp>
    <setSpec>IEDA</setSpec>
    <setSpec>IEDA.SESAR</setSpec>
</header>
<metadata>
    <sample xmlns="http://igsn.org/schema/kernel-v.1.0" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://igsn.org/schema/kernel-v.1.0 http://doidb.wdc-terra.org/igsn/schemas/igsn.org/schema/1.0/igsn.xsd">
        <sampleNumber identifierType="igsn">10273/847000106</sampleNumber>
        <registrant>
            <registrantName>IEDA</registrantName>
        </registrant>
        <log>
            <logElement event="submitted" timeStamp="2013-06-19T03:28:20Z"/>
        </log>
    </sample>
</metadata>
</record>""",
     {'igsn_id':'847000106',
      'registrant':'IEDA',
      'oai_time':'2013-06-19T17:28:22+00:00',
      'igsn_time':'2013-06-19T03:28:20+00:00',
      'related': [],
      }),

    ("""<record xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<header>
    <identifier>oai:registry.igsn.org:18209</identifier>
    <datestamp>2013-06-19T17:28:22Z</datestamp>
    <setSpec>IEDA</setSpec>
    <setSpec>IEDA.SESAR</setSpec>
</header>
<metadata>
    <sample xmlns="http://igsn.org/schema/kernel-v.1.0" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://igsn.org/schema/kernel-v.1.0 http://doidb.wdc-terra.org/igsn/schemas/igsn.org/schema/1.0/igsn.xsd">
        <sampleNumber identifierType="igsn">10273/847000106</sampleNumber>
        <registrant>
            <registrantName>IEDA</registrantName>
        </registrant>
        <log>
            <logElement event="deprecated" timeStamp="2013-06-19T03:28:20Z"/>
            <logElement event="submitted" timeStamp="2013-06-19T03:28:20Z"/>
        </log>
        <relatedResourceIdentifiers>
            <relatedIdentifier relatedIdentifierType="IGSN" relationType="IsPartOf">AU1234</relatedIdentifier>
        </relatedResourceIdentifiers>
    </sample>
</metadata>
</record>""",
     {'igsn_id':'847000106',
      'registrant':'IEDA',
      'oai_time':'2013-06-19T17:28:22+00:00',
      'igsn_time':'2013-06-19T03:28:20+00:00',
      'related':[
          {'id':'AU1234', 'id_type':'IGSN', 'rel_type':'IsPartOf'}
      ]
      })

]

@pytest.mark.parametrize('record,expected', igsn_record_values)
def test_oaiRecordToDict(record, expected):
    data = igsn_lib.oai.oaiRecordToDict(record)
    assert data['oai_time'].isoformat() == expected['oai_time']
    assert data['igsn_time'].isoformat() == expected['igsn_time']
    assert data['registrant'] == expected['registrant']
    assert data['igsn_id'] == expected['igsn_id']
    assert len(data['related']) == len(expected['related'])
    for dr in data['related']:
        match = None
        for er in expected['related']:
            if er['id'] == dr['id'] and er['rel_type'] == dr['rel_type']:
                match = er
                break
        if match is None:
            raise(f"Related record {dr['id']} not in expected")
        assert match['id_type'] == dr['id_type']
