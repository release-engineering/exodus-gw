import io
from xml.etree.ElementTree import ElementTree, Element, SubElement

from fastapi import Response
from defusedxml.ElementTree import fromstring


def extract_mpu_parts(
    body: str, xmlns: str = "http://s3.amazonaws.com/doc/2006-03-01/"
):
    """Extract part data from an XML-formatted CompleteMultipartUpload request.

    This function parses the request body used by this operation:
    https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html

    Arguments:
        body (str)
            Body of incoming request; expected to be a valid XML document.
        xmlns (str)
            Namespace used by the XML document.

    Returns:
        list[dict]
            A list of dicts in the format used for ``Parts`` in the boto s3 client's
            ``complete_multipart_upload`` method, e.g.

                [{"PartNumber": 1, "ETag": "abc123..."},
                 {"PartNumber": 2, "ETag": "xxxyyy..."},
                 ...]
    """
    namespaces = {"s3": xmlns}

    etree = fromstring(body)
    tags = etree.findall(".//s3:ETag", namespaces)
    partnums = etree.findall(".//s3:PartNumber", namespaces)

    return [
        {"ETag": tag.text, "PartNumber": int(partnum.text)}
        for (tag, partnum) in zip(tags, partnums)
    ]


def xml_response(operation: str, **kwargs) -> Response:
    """Get an XML response of the style used by S3 APIs.

    Arguments:
        operation (str)
            The name of the top-level element
            (e.g. "CompleteMultipartUploadOutput")
        kwargs (dict)
            keys/values to include in the document.
            Each item will result in a tag within the XML document.
    """
    root = Element(operation)

    for (key, value) in kwargs.items():
        child = SubElement(root, key)
        child.text = str(value)

    xml = io.BytesIO()
    ElementTree(root).write(xml, encoding="UTF-8", xml_declaration=True)
    return Response(content=xml.getvalue(), media_type="application/xml")
