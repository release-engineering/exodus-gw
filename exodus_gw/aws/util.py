import io
import logging
import re
from collections.abc import Iterable
from typing import AnyStr
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


def validate_metadata(metadata: dict[str, str], settings: Settings):
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

    status_code = kwargs.pop("status_code", 200)

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


def uri_alias(
    uri: str, aliases: list[tuple[str, str, list[str]]]
) -> list[str]:
    # Resolve every alias between paths within the uri (e.g.
    # allow RHUI paths to be aliased to non-RHUI).
    #
    # Aliases are expected to come from cdn-definitions.
    #
    # Returns an ordered list of URIs, guaranteed to always contain
    # at least one element (the original URI).
    # URIs are returned for each intermediate step in alias resolution,
    # and are ordered by depth, from deepest to most shallow.
    #
    # For example:
    #  - if there are no applicable aliases, returns [uri]
    #  - if there is one matching alias, returns [uri_beyond_alias, uri]
    #  - if one alias matches, and then beyond that another alias matches
    #    (meaning two levels deep of alias resolution), returns:
    #    [uri_beyond_both_aliases, uri_beyond_first_alias, uri]
    #
    out: list[str] = [uri]
    uri_alias_recurse(out, uri, aliases)
    return out


def uri_alias_recurse(
    accum: list[str],
    uri: str,
    aliases: list[tuple[str, str, list[str]]],
    depth=0,
    maxdepth=4,
):
    if depth > maxdepth:
        # Runaway recursion breaker.
        #
        # There is no known path to get here, and if we ever do it's
        # probably a bug.
        #
        # The point of this check is to avoid such a bug
        # causing publish to completely break or hang.
        LOG.warning(
            "Aliases too deeply nested, bailing out at %s (URIs so far: %s)",
            uri,
            accum,
        )
        return

    def add_out(new_uri: str) -> bool:
        # Prepends a new URI to the output list
        # (or shifts the existing URI to the front if it was already there)
        # Return True if the URI was newly added.
        out = True
        if new_uri in accum:
            accum.remove(new_uri)
            out = False
        accum.insert(0, new_uri)
        return out

    for src, dest, exclude_paths in aliases:
        if uri.startswith(src + "/") or uri == src:
            # Used to replicate old NetStorage-compatible behaviour. This will
            # typically match non-rpm paths, such as /images/ or /isos/
            if any([re.search(exclusion, uri) for exclusion in exclude_paths]):
                LOG.debug(
                    "Aliasing for %s was not applied as it matches one "
                    "of the following exclusion paths: %s.",
                    uri,
                    ",".join(exclude_paths),
                    extra={"event": "publish", "success": True},
                )
                continue

            new_uri = uri.replace(src, dest, 1)
            LOG.debug(
                "Resolved alias:\n\tsrc: %s\n\tdest: %s",
                uri,
                new_uri,
                extra={"event": "publish", "success": True},
            )
            if add_out(new_uri):
                # We've calculated a new URI and it's the first
                # time we've seen it. We have to recursively apply
                # the aliases again to this new URI.
                #
                # Before doing that, we'll subtract *this* alias from the set,
                # meaning that it won't be considered recursively.
                #
                # We do this because otherwise RHUI aliases are infinitely
                # recursive. For example, since:
                #
                #   /content/dist/rhel8/rhui
                #     => /content/dist/rhel8
                #
                # That means this also works:
                #   /content/dist/rhel8/rhui/rhui
                #     => /content/dist/rhel8/rhui
                #       => /content/dist/rhel8
                #
                # In fact you could insert as many "rhui" components into the path
                # as you want and still ultimately resolve to the same path.
                # That's the way the symlinks actually worked on the legacy CDN.
                #
                # But this is not desired behavior, we know that every alias is intended
                # to be resolved in the URL a maximum of once, hence this adjustment.
                sub_aliases = [
                    (subsrc, subdest, exclusions)
                    for (subsrc, subdest, exclusions) in aliases
                    if (subsrc, subdest) != (src, dest)
                ]

                uri_alias_recurse(
                    accum,
                    new_uri,
                    sub_aliases,
                    depth=depth + 1,
                    maxdepth=maxdepth,
                )


def uris_with_aliases(
    uris: Iterable[str], aliases: list[tuple[str, str, list[str]]]
) -> list[str]:
    # Given a collection of uris and aliases, returns a new collection of uris
    # post alias resolution, including *both* sides of each alias when applicable.
    out: set[str] = set()

    for uri in uris:
        # We accept inputs both with and without leading '/', normalize.
        uri = "/" + uri.removeprefix("/")

        for resolved in uri_alias(uri, aliases):
            out.add(resolved)

    return sorted(out)
