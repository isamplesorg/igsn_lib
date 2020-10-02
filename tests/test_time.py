import pytest
import igsn_lib.time
import dateparser

jd_time_values = [
    (2400000.5, "1858-11-17T00:00:00+0000"),
    (2459123.0, "2020-09-30T12:00:00+0000"),
    (0, None),
]


@pytest.mark.parametrize("jd,tstring", jd_time_values)
def test_jdToDateTime(jd, tstring):
    assert tstring == igsn_lib.time.jdToJsonString(jd)


@pytest.mark.parametrize("jd,tstring", jd_time_values)
def test_dateTimeToJD(jd, tstring):
    if tstring is not None:
        dt = dateparser.parse(tstring)
        assert jd == igsn_lib.time.datetimeToJD(dt)
