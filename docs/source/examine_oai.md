---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Examine an IGSN OAI-PMH Endpoint

A list of known IGSN OAI-PMH endpoints gathered from IGSN documentation [^igsndoc]:

```{code-cell} python3
---
tags: [hide-input]
---
import igsn_oai_scan as iscan
endpoints = iscan.Endpoints()
cnt = 1
for endpoint in endpoints:
    print(f"{cnt:02} {endpoint['name']:>10}: {endpoint['url']}")
    cnt += 1
```

Sickle [^sickle] is a library for interacting with OAI-PMH [^oaipmh] end points. It is used in the examples below. The
xml responses are converted to python dictionaries using `xmltodict`.

```{code-cell} python3
import lxml.etree as ET
import sickle
import json
import xmltodict

OAI_NAMESPACES = {
    'http://www.w3.org/2001/XMLSchema-instance':'xsi',
    'http://purl.org/dc/elements/1.1/':'dc',
    'http://www.openarchives.org/OAI/2.0/':'oai',
    'http://www.openarchives.org/OAI/2.0/oai_dc/':'oai_dc',
    'http://www.openarchives.org/OAI/2.0/oai-identifier':'oai_identifier',
    'http://igsn.org/schema/kernel-v.1.0':'igsn',
    'http://schema.igsn.org/description/1.0':'igsn_desc',
}
oai_endpoint_url = endpoints.get(name='igsnoaip').get('url')
# Create a sickle instance for interacting with the endpoint
oai_svc = sickle.Sickle(oai_endpoint_url, encoding='utf-8')
```

[^igsndoc]: IGSN documentation: [https://igsn.github.io/oai/](https://igsn.github.io/oai/)
[^sickle]: Sickle: [https://sickle.readthedocs.io/en/latest/](https://sickle.readthedocs.io/en/latest/)
[^oaipmh]: OAI-PMH: [http://www.openarchives.org/OAI/openarchivesprotocol.html](http://www.openarchives.org/OAI/openarchivesprotocol.html)

## Identify

The OAI-PMH Identify verb requests metadata about the OAI-PMH service. For example, the XML response

```{code-cell} python3
response = oai_svc.Identify()
xml = ET.fromstring(response.raw)
print(ET.tostring(xml, pretty_print=True).decode())
```

and rendered as a json structure after conversion using `xmltodict`:

```{code-cell} python3
response_dict = xmltodict.parse(
        response.raw, 
        process_namespaces=True,
        namespaces=OAI_NAMESPACES
    )
print(json.dumps(response_dict, indent=2))
```


## Metadata formats

List metadata formats available on provider. All OAI-PMH providers support at least `oai_dc`
which contains Dublin Core elements.

```{code-cell} python3
metadata_list = []
items = oai_svc.ListMetadataFormats()
for item in items:
    item_dict = xmltodict.parse(
        item.raw, 
        process_namespaces=True,
        namespaces=OAI_NAMESPACES
    )
    metadata_list.append({
        'metadataPrefix':item_dict['oai:metadataFormat']['oai:metadataPrefix'],
        'schema':item_dict['oai:metadataFormat']['oai:schema'],
        'metadataNamespace':item_dict['oai:metadataFormat']['oai:metadataNamespace'],
    })
cnt = 0
for item in metadata_list:
    print(f"{cnt:03}{item['metadataPrefix']:>8}: {item['metadataNamespace']}")
    print(f"{' ':>13}{item['schema']}")
    cnt += 1
```

## Date ranges

The OAI-PMH spec identifies that records can be retrieved from a specific time range. This is 
helpful when keeping up to date with an OAI-PMH endpoint, since a request can be constructed
that returns the records since the last time the endpoint was examined.

The earliest date stamp of the endpoint can be determined by examining the service metadata 
returned by the `Identify` verb. Note however, that this date will be the date of the oldest
record which may have a deleted status.

Test to see if the provider supports date range requests:

```{code-cell} python3
import dateparser
earliest_record = dateparser.parse(response_dict['oai:Identify']['oai:earliestDatestamp'])
print(earliest_record)

```