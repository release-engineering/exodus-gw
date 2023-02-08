import io
import logging
import re
from typing import AnyStr, Dict
from xml.etree.ElementTree import Element, ElementTree, SubElement

from defusedxml.ElementTree import fromstring
from fastapi import HTTPException, Request, Response

from ..settings import Settings

LOG = logging.getLogger("exodus-gw")


def extract_request_metadata(request: Request, settings: Settings):
    # Any headers prefixed with "x-amz-meta-" will be picked out as
    # metadata for s3 upload.
    #
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
    metadata = {}
    for k, v in request.headers.items():
        if k.startswith("x-amz-meta-"):
            metadata[k.replace("x-amz-meta-", "", 1)] = v

    validate_metadata(metadata, settings)

    return metadata


def validate_metadata(metadata: Dict[str, str], settings: Settings):
    valid_meta_fields = settings.upload_meta_fields
    for k, v in metadata.items():
        if k not in valid_meta_fields.keys():
            raise HTTPException(400, detail="Invalid metadata field, '%s'" % k)

        pattern = re.compile(valid_meta_fields[k])

        if not re.match(pattern, v):
            raise HTTPException(
                400,
                detail="Invalid value for metadata field '%s', '%s'" % (k, v),
            )


def validate_object_key(key: str):
    pattern = re.compile(r"[0-9a-f]{64}")

    if not re.match(pattern, key):
        raise HTTPException(400, detail="Invalid object key: '%s'" % key)


def content_md5(request):
    """Produce ContentMD5 value expected by S3 APIs.

    When uploading empty files, the Content-MD5 header may not be
    included in the request when content length is 0. In such cases,
    return the appropriate base64 encoded md5.
    """

    if int(request.headers["Content-Length"]) == 0:
        return "1B2M2Y8AsgTpgAmY7PhCfg=="

    return request.headers["Content-MD5"]


def extract_mpu_parts(
    body: AnyStr, xmlns: str = "http://s3.amazonaws.com/doc/2006-03-01/"
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

    for key, value in kwargs.items():
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


def uri_alias(uri, aliases):
    # Resolve every alias between paths within the uri (e.g.
    # allow RHUI paths to be aliased to non-RHUI).
    #
    # Aliases are expected to come from cdn-definitions.

    new_uri = ""
    remaining = aliases

    # We do multiple passes here to ensure that nested aliases
    # are resolved correctly, regardless of the order in which
    # they're provided.
    while remaining:
        processed = []

        for alias in remaining:
            if uri.startswith(alias["src"] + "/") or uri == alias["src"]:
                new_uri = uri.replace(alias["src"], alias["dest"], 1)
                LOG.debug(
                    "Resolved alias:\n\tsrc: %s\n\tdest: %s", uri, new_uri
                )
                uri = new_uri
                processed.append(alias)

        if not processed:
            # We didn't resolve any alias, then we're done processing.
            break

        # We resolved at least one alias, so we need another round
        # in case others apply now. But take out anything we've already
        # processed, so it is not possible to recurse.
        remaining = [r for r in remaining if r not in processed]

    return uri
