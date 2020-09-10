.. _dev-guide:

Development Guide
=================

This document contains useful info for developers contributing
to the exodus-gw project.


Spoofing authentication
-----------------------

The exodus-gw service parses an ``X-RhApiPlatform-CallContext`` header for information
relating to authentication & authorization; see :ref:`deploy-guide` for more info on
this scheme.

During development, arbitrary values for this header may be used to test the
behavior of endpoints with various roles. However, due to the format of this header,
generating these values by hand can be cumbersome.

To assist in this, a helper script is provided in the exodus-gw repo at
``scripts/call-context``. This script accepts any number of role names as arguments
and produces a header value which will produce an authenticated & authorized request
using those roles.

For example, if we want to use ``curl`` to make a request to an endpoint needing
``qa-uploader`` role, we can use the following command:

.. code-block:: shell

    curl \
      -H "X-RhApiPlatform-CallContext: $(scripts/call-context qa-uploader)" \
      http://localhost:8080/some/qa/endpoint

