import pytest
import igsn_lib
import igsn_lib.oai
import dateparser

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

jd_time_values = [
    (2400000.5, '1858-11-17T00:00:00Z'),
    (2459123.0, '2020-09-30T12:00:00Z'),
    (0, None),
]

@pytest.mark.parametrize("igsn_str,expected", igsn_testcases)
def test_normalize(igsn_str, expected):
    result = igsn_lib.normalize(igsn_str)
    assert result == expected

@pytest.mark.parametrize("igsn_val,status_code",igsn_resolve_values)
def test_resolve(igsn_val, status_code):
    response = igsn_lib.resolve(igsn_val)
    assert response.status_code == status_code


@pytest.mark.parametrize('jd,tstring',jd_time_values)
def test_jdToDateTime(jd, tstring):
    assert tstring == igsn_lib.jdToJsonString(jd)

@pytest.mark.parametrize('jd,tstring',jd_time_values)
def test_dateTimeToJD(jd, tstring):
    if tstring is not None:
        dt = dateparser.parse(tstring)
        assert jd == igsn_lib.dateTimetoJD(dt)