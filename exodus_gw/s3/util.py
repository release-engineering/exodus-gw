import io
from xml.etree.ElementTree import Element, ElementTree, SubElement

from defusedxml.ElementTree import fromstring
from fastapi import Response


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

    status_code = kwargs.get("Code", 200)

    for (key, value) in kwargs.items():
        child = SubElement(root, key)
        child.text = str(value)

    xml = io.BytesIO()
    ElementTree(root).write(xml, encoding="UTF-8", xml_declaration=True)
    return Response(
        content=xml.getvalue(),
        status_code=status_code,
        media_type="application/xml",
    )


class RequestReader:
    """Tiny wrapper to help pass streaming requests into aiobotocore.

    This class is a bit of a trick to work around one point where botocore
    and aiobotocore are not working well together:

    - aiobotocore uses aiohttp and it fully supports accepting an async iterable
      for a request body, so request.stream() should work there.

    - but, botocore performs schema validation on incoming arguments and expects
      Body to be a str, bytes or file-like object, so it refuses to accept request.stream(),
      even though the underlying layer can cope with it just fine.

    This wrapper makes the request stream look like a file-like object so
    that boto will accept it (though note that actually *using it* as a file
    would raise an error).
    """

    def __init__(self, request):
        self._req = request

    def __aiter__(self):
        return self._req.stream().__aiter__()

    def read(self, *_, **__):
        raise NotImplementedError()

    @classmethod
    def get_reader(cls, request):
        # a helper to make tests easier to write.
        # tests can patch over this to effectively disable streaming.
        return cls(request)
