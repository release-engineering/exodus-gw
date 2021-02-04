"""An API for uploading binary data.

This API provides endpoints for uploading files into the data store
used by the Exodus CDN. Uploading files does not immediately expose
them to clients of the CDN, but is a prerequisite of publishing files,
which is achieved via the [publish](#tag/publish) APIs.

The upload API is a partially compatible subset of the S3 API.
It supports at least enough functionality such that the boto S3
client and resource may be used.

Differences from the AWS S3 API include:

- Most optional arguments are not supported.

- All `x-amz-*` headers are omitted from responses.

- The usual AWS authentication mechanism is unused; request signatures are ignored.
  Authentication is expected to be performed by other means.

- Object keys should always be SHA256 checksums of the objects being stored,
  in lowercase hex digest form. This allows the object store to be used
  as content-addressable storage.

- When uploading content, the Content-MD5 and Content-Length headers are mandatory;
  chunked encoding is not supported.

- The API may enforce stricter limits or policies on uploads than those imposed
  by the AWS API.

## Using boto3 with the upload API

As the upload API is partially compatible with S3, it is possible to use
existing S3 clients such as the AWS SDK to perform uploads. This is the
recommended method of using the API.

Use `endpoint_url` when creating a boto resource or client to point at exodus-gw.
Region and credentials will be ignored.

Note that, as the upload API provides only a subset of the S3 API, many boto methods
will not work. Uploading objects and querying the existence of an object are
supported.

```python
import boto3
from botocore.config import Config

# Prepare S3 resource pointing at exodus-gw
s3 = boto3.resource('s3',
                    endpoint_url='https://exodus-gw.example.com/upload',
                    # If SSL needs to be configured:
                    verify='/path/to/bundle.pem',
                    config=Config(client_cert=('client.crt', 'client.key')))

# Bucket name must match one of the section names in exodus-gw.ini without 'env.' prefix
bucket = s3.Bucket('dev')

# Basic APIs such as upload_file now work as usual
bucket.upload_file('/tmp/hello.txt',
                   'aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f')
```
"""
import logging
import textwrap
from typing import Optional

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Path, Query, Request, Response

from .. import deps
from ..aws.client import S3ClientWrapper as s3_client
from ..aws.util import (
    RequestReader,
    content_md5,
    extract_mpu_parts,
    xml_response,
)
from ..settings import Environment

LOG = logging.getLogger("s3")

openapi_tag = {"name": "upload", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


# A partial TODO list for this API:
# - format of 'key' should be enforced (sha256sum)
# - a way to check if object is already uploaded, e.g. HEAD
# - limits on chunk sizes during multipart upload should be decided and enforced
# - requests should be authenticated


@router.post(
    "/upload/{env}/{key}",
    summary="Create/complete multipart upload",
    response_class=Response,
)
async def multipart_upload(
    request: Request,
    env: Environment = deps.env,
    key: str = Path(..., description="S3 object key"),
    uploadId: Optional[str] = Query(
        None,
        description=textwrap.dedent(
            """
            ID of an existing multi-part upload.

            If this argument is provided, it must be the ID of a multi-part upload
            created previously. The upload will be validated and completed.

            Must not be passed together with ``uploads``."""
        ),
    ),
    uploads: Optional[str] = Query(
        None,
        description=textwrap.dedent(
            """
            If this argument is provided, a new multi-part upload will be created
            and its ID returned. The provided value should be an empty string.

            Must not be passed together with ``uploadId``."""
        ),
    ),
):
    """Create or complete a multi-part upload.

    To create a multi-part upload:
    - include ``uploads`` in query string, with no value (e.g. ``POST /upload/{env}/{key}?uploads``)
    - see also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html

    To complete a multi-part upload:
    - include ``uploadId`` in query string
    - include parts with ETags in request body
    - see also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    """

    if uploads == "":
        # Means a new upload is requested
        return await create_multipart_upload(env, key)

    if uploads is None and uploadId:
        # Given an existing upload to complete
        return await complete_multipart_upload(env, key, uploadId, request)

    # Caller did something wrong
    raise HTTPException(
        status_code=400,
        detail="Invalid uploadId=%s, uploads=%s"
        % (repr(uploadId), repr(uploads)),
    )


@router.put(
    "/upload/{env}/{key}",
    summary="Upload bytes",
    response_class=Response,
)
async def upload(
    request: Request,
    env: Environment = deps.env,
    key: str = Path(..., description="S3 object key"),
    uploadId: Optional[str] = Query(
        None, description="ID of an existing multi-part upload."
    ),
    partNumber: Optional[int] = Query(
        None, description="Part number, where multi-part upload is used."
    ),
):
    """Write to an object, either as a standalone operation or within a multi-part upload.

    To upload an entire object:
    - include all object bytes in request body
    - see also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html

    To upload part of an object:
    - provide multipart upload ID in ``uploadId``
    - provide part number from 1 to 10,000 in ``partNumber``
    - include part of an object in request body (must be at least 5MB in size, except last part)
    - retain the `ETag` from the response, as it will be required to complete the upload
    - see also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    """

    if uploadId is None and partNumber is None:
        # Single-part upload
        return await object_put(env, key, request)

    # Multipart upload
    return await multipart_put(env, key, uploadId, partNumber, request)


async def object_put(env: Environment, key: str, request: Request):
    # Single-part upload handler: entire object is written via one PUT.
    reader = RequestReader.get_reader(request)

    async with s3_client(profile=env.aws_profile) as s3:
        response = await s3.put_object(
            Bucket=env.bucket,
            Key=key,
            Body=reader,
            ContentMD5=content_md5(request),
            ContentLength=int(request.headers["Content-Length"]),
        )

    return Response(headers={"ETag": response["ETag"]})


async def complete_multipart_upload(
    env: Environment, key: str, uploadId: str, request: Request
):
    body = await request.body()
    parts = extract_mpu_parts(body)

    LOG.debug("completing mpu for parts %s", parts)

    async with s3_client(profile=env.aws_profile) as s3:
        response = await s3.complete_multipart_upload(
            Bucket=env.bucket,
            Key=key,
            UploadId=uploadId,
            MultipartUpload={"Parts": parts},
        )

    LOG.debug("Completed mpu: %s", response)
    return xml_response(
        "CompleteMultipartUploadOutput",
        Location=response["Location"],
        Bucket=response["Bucket"],
        Key=response["Key"],
        ETag=response["ETag"],
    )


async def create_multipart_upload(env: Environment, key: str):
    async with s3_client(profile=env.aws_profile) as s3:
        response = await s3.create_multipart_upload(Bucket=env.bucket, Key=key)

    return xml_response(
        "CreateMultipartUploadOutput",
        Bucket=response["Bucket"],
        Key=response["Key"],
        UploadId=response["UploadId"],
    )


async def multipart_put(
    env: Environment,
    key: str,
    uploadId: str,
    partNumber: int,
    request: Request,
):
    reader = RequestReader.get_reader(request)

    async with s3_client(profile=env.aws_profile) as s3:
        response = await s3.upload_part(
            Body=reader,
            Bucket=env.bucket,
            Key=key,
            PartNumber=partNumber,
            UploadId=uploadId,
            ContentMD5=content_md5(request),
            ContentLength=int(request.headers["Content-Length"]),
        )

    return Response(headers={"ETag": response["ETag"]})


@router.delete(
    "/upload/{env}/{key}",
    summary="Abort multipart upload",
    response_description="Empty response",
    response_class=Response,
)
async def abort_multipart_upload(
    env: Environment = deps.env,
    key: str = Path(..., description="S3 object key"),
    uploadId: str = Query(..., description="ID of a multipart upload"),
):
    """Abort a multipart upload.

    If an upload cannot be completed, explicitly aborting it is recommended in order
    to free up resources as early as possible, although this is not mandatory.

    See also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
    """
    LOG.debug("Abort %s", uploadId)

    async with s3_client(profile=env.aws_profile) as s3:
        await s3.abort_multipart_upload(
            Bucket=env.bucket, Key=key, UploadId=uploadId
        )

    return Response()


@router.head(
    "/upload/{env}/{key}",
    summary="Request head object",
    response_class=Response,
)
async def head(
    env: Environment = deps.env,
    key: str = Path(..., description="S3 object key"),
):
    """Retrieve metadata from an S3 object."""

    try:
        async with s3_client(profile=env.aws_profile) as s3:
            response = await s3.head_object(Bucket=env.bucket, Key=key)
    except ClientError as exc_info:
        # According to botocore documentation, it is safe to rely on
        # the API to throw exceptions for any non-2xx response.
        LOG.exception("HEAD to S3 failed")
        return Response(status_code=int(exc_info.response["Error"]["Code"]))

    return Response(headers={"ETag": response["ETag"]})
