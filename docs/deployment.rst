.. _deploy-guide:

Deployment Guide
================


Target platform
---------------

exodus-gw is an ASGI application which may be deployed using any ASGI-compliant
web server. The development team's recommended setup is summarized as:

- Use OpenShift >= 4.x to deploy the service.

- Use the exodus-gw images at https://quay.io/repository/exodus/exodus-gw to run
  the service. These images run the service using gunicorn & uvicorn on RHEL8.

  In general, the
  `uvicorn deployment advice <https://www.uvicorn.org/deployment/>`_
  applies.

- Deploy the service's primary container behind a reverse-proxy implementing
  authentication according to your organization's needs (see next section).


Authentication & Authorization
------------------------------

The exodus-gw service does not implement any authentication mechanism. It is instead
designed to integrate with a reverse-proxy implementing any desired mechanism.

.. warning::
    If exodus-gw is deployed without an authenticating reverse-proxy, the service must
    be considered completely unsecured - all users will be able to perform all operations.

This reverse-proxy must add an ``X-RhApiPlatform-CallContext`` header onto all incoming
requests. This header must contain a base64-encoded form of the following JSON object:

.. code-block:: json

  {
    "client": {
      "roles": ["someRole", "anotherRole"],
      "authenticated": true,
      "serviceAccountId": "clientappname"
    },
    "user": {
      "roles": ["viewer"],
      "authenticated": true,
      "internalUsername": "someuser"
    }
  }

The ``roles`` and ``authenticated`` fields influence whether an exodus-gw request will be
permitted - the necessary roles are documented on relevant exodus-gw API endpoints.
Other fields are unused or used only for logging.

The separate ``client`` and ``user`` fields can be used to separate service accounts
(machine users) from human users, but this does not affect exodus-gw.

Within Red Hat, a container known as "platform-sidecar" is used as the reverse proxy - consult
internal documentation for information on this component. In other contexts, any reverse
proxy may be used as long as it produces headers according to the scheme documented above.


Database Migrations
-------------------

The exodus-gw service uses a postgres database.

On startup, the service will run database migrations to ensure the DB implements the
required schema.

It is a goal that migrations can be performed online with minimal disruption to the
service, even with old and new versions of the service running simultaneously
(for example, during an OpenShift rolling deployment).

Downgrading to an earlier version of the schema is not directly supported by the
service. However, as exodus-gw is designed not to store any permanent state, dropping
and recreating the exodus-gw database is a viable option if needed.


Settings
--------

.. autoclass:: exodus_gw.settings.Settings()
    :members:

    exodus-gw may be configured by the following settings.

    Each settings value may be overridden using an environment variable of the
    same name, prefixed with ``EXODUS_GW_`` (example: ``EXODUS_GW_CALL_CONTEXT_HEADER``).

To enable per-environment configuration of exodus-gw, exodus-gw.ini is available to point the
application at specific AWS resources and declare the AWS profile to use when interacting with
those resources. Each environment must appear in its own section with the prefix "env.".

.. code-block:: ini

  [env.prod]
  aws_profile = production
  bucket = cdn-prod-s3
  table = cdn-prod-db

Logger levels may also be configured via exodus-gw.ini. Under a section named "loglevels",
users may specify a logger name and the level at which to set said logger.

.. code-block:: ini

  [loglevels]
  root = NOTSET
  exodus-gw = INFO
  s3 = DEBUG
  ...

CDN cache flush
...............

exodus-gw supports flushing the cache of an Akamai CDN edge via
the `Fast Purge API <https://techdocs.akamai.com/purge-cache/reference/api>`_.

This feature is optional. If configuration is not provided, related APIs in
exodus-gw will continue to function but will skip cache flush operations.

Enabling the feature requires the deployment of two sets of configuration.

Firstly, in the ``exodus-gw.ini`` section for the relevant environment,
set ``cache_flush_urls`` to enable cache flush by URL and/or
``cache_flush_arl_templates`` to enable cache flushing by ARL. Both options
can be used together as needed.

.. code-block:: ini

  [env.live]
  # Root URL(s) of CDN properties for which to flush cache.
  # Several can be provided.
  cache_flush_urls =
    https://cdn1.example.com
    https://cdn2.example.com

  # Templates of ARL(s) for which to flush cache.
  # Templates can use placeholders:
  # - path: path of a file under CDN root
  # - ttl (optional): a TTL value will be substituted
  cache_flush_arl_templates =
    S/=/123/22334455/{ttl}/cdn1.example.com/{path}
    S/=/123/22334455/{ttl}/cdn2.example.com/{path}

Secondly, use environment variables to deploy credentials for the
Fast Purge API, according to the below table. The fields here correspond
to those used by the `.edgerc file <https://techdocs.akamai.com/developer/docs/set-up-authentication-credentials>`_
as found in Akamai's documentation.

Note that "<env>" should be replaced with the specific corresponding
environment name, e.g. ``EXODUS_GW_FASTPURGE_HOST_LIVE`` for a ``live``
environment.

.. list-table:: Fast Purge credentials

   * - Variable
     - ``.edgerc`` field
     - Example
   * - ``EXODUS_GW_FASTPURGE_CLIENT_SECRET_<env>``
     - ``client_secret``
     - ``abcdEcSnaAt123FNkBxy456z25qx9Yp5CPUxlEfQeTDkfh4QA=I``
   * - ``EXODUS_GW_FASTPURGE_HOST_<env>``
     - ``host``
     - ``akab-lmn789n2k53w7qrs10cxy-nfkxaa4lfk3kd6ym.luna.akamaiapis.net``
   * - ``EXODUS_GW_FASTPURGE_ACCESS_TOKEN_<env>``
     - ``access_token``
     - ``akab-zyx987xa6osbli4k-e7jf5ikib5jknes3``
   * - ``EXODUS_GW_FASTPURGE_CLIENT_TOKEN_<env>``
     - ``client_token``
     - ``akab-nomoflavjuc4422-fa2xznerxrm3teg7``
