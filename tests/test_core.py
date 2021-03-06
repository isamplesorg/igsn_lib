import pytest
import igsn_lib

'''
pytest test

Can run like:
pytest
pytest test_core.py
pytest test_core.py::test_parseLinkHeader

Add -s to output stdout
'''

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
    ["http://igsn.org/BGRB5054RX05201", "BGRB5054RX05201"],
    ["http://igsn.org/AU1101", "AU1101"],
]

igsn_resolve_values = [
    ("1234", 404),
    ("AU1234IAMFAKE", 404),
    ("000fake-id", 404),
    ("ICDP5054EEW1001", 200),
]


link_header_values = [
    (
        '<meta.rdf>;rel="meta"',
        [
            {
                'href': "meta.rdf",
                'rel': 'meta',
            },
        ]
    ),
    (
      '<http://example.com/TheBook,chapter2;a>; rel="prev;ious"; title="previous chapter"',
      [
          {
              'href':'http://example.com/TheBook,chapter2;a',
              'rel': 'prev;ious',
              'title': 'previous chapter'
          }
      ]
    ),
    (
        '<https://one.example.com>; rel="preconnect", <https://two.example.com>; rel="preconnect", <https://three.example.com>; rel="preconnect"',
        [
            {
                'href':'https://one.example.com',
                'rel': 'preconnect',
            },
            {
                'href': 'https://two.example.com',
                'rel': 'preconnect',
            },
            {
                'href': 'https://three.example.com',
                'rel': 'preconnect',
            },
        ]
    )
]


@pytest.mark.parametrize("igsn_str,expected", igsn_testcases)
def test_normalize(igsn_str, expected):
    result = igsn_lib.normalize(igsn_str)
    assert result == expected


@pytest.mark.parametrize("igsn_val,status_code", igsn_resolve_values)
def test_resolve(igsn_val, status_code):
    responses = igsn_lib.resolve(igsn_val)
    last_response = responses[-1]
    assert last_response.status_code == status_code

@pytest.mark.parametrize("hv, expected", link_header_values)
def test_parseLinkHeader(hv, expected):
    parsed = igsn_lib.parseLinkHeader(hv)
    print(parsed)
    assert parsed == expected
