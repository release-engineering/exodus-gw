.. _api-guide:

API User Guide
==============

This document provides an overview of the APIs available in exodus-gw
along with general usage information.

For a full reference on the available endpoints, see:
:ref:`api-reference`.

.. warning::

    exodus-gw is in early stages of development. All APIs are subject
    to backwards-incompatible changes without warning.


Authentication
--------------

Authentication is currently unimplemented in exodus-gw; any clients with
access to the service can perform all operations.


Uploading blobs
---------------

.. seealso:: `Upload API reference <api.html#tag/upload>`_

.. automodule:: exodus_gw.s3.api

Using boto3 with the upload API
...............................

As the upload API is partially compatible with S3, it is possible to use
the AWS SDK to perform uploads. This is the recommended method of using the API.

Use ``endpoint_url`` when creating a boto resource or client to point at exodus-gw.
Region and credentials will be ignored.

Note that, as the upload API provides only a subset of the S3 API, many boto methods
will not work. Uploading files is supported.

Usage:

.. code-block:: python

    import boto3
    from botocore.config import Config

    # Prepare S3 resource pointing at exodus-gw
    s3 = boto3.resource('s3',
                        endpoint_url='https://exodus-gw.example.com/upload',
                        # If SSL needs to be configured:
                        verify='/path/to/bundle.pem',
                        config=Config(client_cert=('client.crt', 'client.key')))

    # Basic APIs such as upload_file now work as usual
    bucket = s3.Bucket('exodus-cdn-dev')
    bucket.upload_file('/tmp/hello.txt',
                       'aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f')



Publishing content
------------------

exodus-gw is expected to provide an API for making uploaded content
externally accessible on the exodus CDN. This API is not yet implemented.
