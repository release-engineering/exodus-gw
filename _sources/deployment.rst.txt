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


Settings
--------

.. autoclass:: exodus_gw.settings.Settings()
    :members:

    exodus-gw may be configured by the following settings.

    Each settings value may be overridden using an environment variable of the
    same name, prefixed with ``EXODUS_GW_`` (example: ``EXODUS_GW_CALL_CONTEXT_HEADER``).
