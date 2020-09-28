import pytest
import igsn_lib
import igsn_lib.oai

igsn_testcases = [
    ["IGSN:A1234", "A1234"],
    ["IGSN:a1234", "A1234"],
    ["IGSN: A1234", "A1234"],
    ["igsn:A1234", "A1234"],
    [" igsn: A1234 ", "A1234"],
    ["DOI:1234", None],
    ["http://hdl.handle.net/10273/ABCD", "ABCD"],
    ["10273/abcd", "ABCD"],
    ["http://some.url/with/perhaps/igsn/1234", "1234"],
    ["info:hdl/10273/ABCD", "ABCD"],
    ["info:hdl/20.1000/ABCD", None],
    ["igsn:10273/ABCD", "ABCD"],
    ["http://igsn.org/BGRB5054RX05201","BGRB5054RX05201"],
    ["http://igsn.org/AU1101","AU1101"],
]

igsn_resolve_values = [
    ('1234', 404),
    ('AU1234IAMFAKE', 404),
    ('000fake-id', 404),
    ('ICDP5054EEW1001',200)
]

igsn_record_values = [
    ("""<record xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <header>
    <identifier>oai:registry.igsn.org:18209</identifier>
    <datestamp>2013-06-19T17:28:22Z</datestamp>
    <setSpec>IEDA</setSpec>
    <setSpec>IEDA.SESAR</setSpec>
  </header>
  <metadata>
    <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
      <dc:creator>IEDA</dc:creator>
      <dc:identifier>http://hdl.handle.net/10273/847000106</dc:identifier>
      <dc:identifier>igsn:10273/847000106</dc:identifier>
    </oai_dc:dc>
  </metadata>
</record>""",
     {'igsn_id':'847000106',
      'creator':'IEDA',
      'tstamp':'2013-06-19T17:28:22+00:00'
      })
]

@pytest.mark.parametrize("igsn_str,expected", igsn_testcases)
def test_normalize(igsn_str, expected):
    result = igsn_lib.normalize(igsn_str)
    assert result == expected

@pytest.mark.parametrize("igsn_val,status_code",igsn_resolve_values)
def test_resolve(igsn_val, status_code):
    response = igsn_lib.resolve(igsn_val)
    assert response.status_code == status_code

@pytest.mark.parametrize('record,expected', igsn_record_values)
def test_oaiRecordToDict(record, expected):
    data = igsn_lib.oai.oaiRecordToDict(record)
    assert data['tstamp'].isoformat() == expected['tstamp']
    assert data['creator'] == expected['creator']
    assert data['igsn_id'] == expected['igsn_id']