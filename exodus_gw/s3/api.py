"""An API for uploading binary data.

This API provides endpoints for uploading files into the data store
used by the Exodus CDN. Uploading files does not immediately expose
them to clients of the CDN, but is a prerequisite of publishing files,
which is achieved via other APIs.

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

- The API may enforce stricter limits or policies on uploads than those imposed
  by the AWS API.
"""
from typing import Optional
import textwrap
import logging

import aioboto3
from fastapi import Request, Response, Path, Query, HTTPException

from ..app import app

from .util import extract_mpu_parts, xml_response


LOG = logging.getLogger("s3")

# A partial TODO list for this API:
# - format of 'key' should be enforced (sha256sum)
# - a way to check if object is already uploaded, e.g. HEAD
# - limits on chunk sizes during multipart upload should be decided and enforced
# - should support configuration of the target S3 environment rather than using
#   whatever's the boto default
# - {bucket} shouldn't be a parameter, as the target bucket for each exodus env
#   is predefined and not controlled by the caller.
# - requests should be authenticated


@app.post(
    "/upload/{bucket}/{key}",
    tags=["upload"],
    summary="Create/complete multipart upload",
    response_class=Response,
)
async def multipart_upload(
    request: Request,
    bucket: str = Path(..., description="S3 bucket name"),
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
    - include ``uploads`` in query string, with no value (e.g. ``POST /upload/{bucket}/{key}?uploads``)
    - see also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html

    To complete a multi-part upload:
    - include ``uploadId`` in query string
    - include parts with ETags in request body
    - see also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    """

    if uploads == "":
        # Means a new upload is requested
        return await create_multipart_upload(bucket, key)

    elif uploads is None and uploadId:
        # Given an existing upload to complete
        return await complete_multipart_upload(bucket, key, uploadId, request)

    # Caller did something wrong
    raise HTTPException(
        status_code=400,
        detail="Invalid uploadId=%s, uploads=%s"
        % (repr(uploadId), repr(uploads)),
    )


@app.put(
    "/upload/{bucket}/{key}",
    tags=["upload"],
    summary="Upload bytes",
    response_class=Response,
)
async def upload(
    request: Request,
    bucket: str = Path(..., description="S3 bucket name"),
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
        return await object_put(bucket, key, request)

    # Multipart upload
    return await multipart_put(bucket, key, uploadId, partNumber, request)


async def object_put(bucket: str, key: str, request: Request):
    # Single-part upload handler: entire object is written via one PUT.
    body = await request.body()

    async with aioboto3.client("s3") as s3:
        response = await s3.put_object(Bucket=bucket, Key=key, Body=body)

    return Response(headers={"ETag": response["ETag"]})


async def complete_multipart_upload(
    bucket: str, key: str, uploadId: str, request: Request
):
    body = await request.body()

    parts = extract_mpu_parts(body)

    LOG.debug("completing mpu for parts %s", parts)

    async with aioboto3.client("s3") as s3:
        response = await s3.complete_multipart_upload(
            Bucket=bucket,
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


async def create_multipart_upload(bucket: str, key: str):
    async with aioboto3.client("s3") as s3:
        response = await s3.create_multipart_upload(Bucket=bucket, Key=key)

    return xml_response(
        "CreateMultipartUploadOutput",
        Bucket=response["Bucket"],
        Key=response["Key"],
        UploadId=response["UploadId"],
    )


async def multipart_put(
    bucket: str, key: str, uploadId: str, partNumber: int, request: Request
):
    body = await request.body()

    async with aioboto3.client("s3") as s3:
        response = await s3.upload_part(
            Body=body,
            Bucket=bucket,
            Key=key,
            PartNumber=partNumber,
            UploadId=uploadId,
        )

    return Response(headers={"ETag": response["ETag"]})


@app.delete(
    "/upload/{bucket}/{key}",
    tags=["upload"],
    summary="Abort multipart upload",
    response_description="Empty response",
    response_class=Response,
)
async def abort_multipart_upload(
    bucket: str = Path(..., description="S3 bucket name"),
    key: str = Path(..., description="S3 object key"),
    uploadId: str = Query(..., description="ID of a multipart upload"),
):
    """Abort a multipart upload.

    If an upload cannot be completed, explicitly aborting it is recommended in order
    to free up resources as early as possible, although this is not mandatory.

    See also: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
    """
    LOG.debug("Abort %s", uploadId)

    async with aioboto3.client("s3") as s3:
        await s3.abort_multipart_upload(
            Bucket=bucket, Key=key, UploadId=uploadId
        )

    return Response()
