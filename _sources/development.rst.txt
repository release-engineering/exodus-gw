.. _dev-guide:

Development Guide
=================

This document contains useful info for developers contributing
to the exodus-gw project.


Running exodus-gw with tox
--------------------------

A development instance of the exodus-gw service may be run locally
using tox:

.. code-block:: shell

  tox -e dev-server

This will run the service using uvicorn with hot reload enabled.

The service will use ``exodus-gw.ini`` from the source directory for
configuration, along with any ``EXODUS_GW_*`` environment variables,
as described in :ref:`deploy-guide`.

Note that tox will not start or manage any dependencies of exodus-gw.
A more complete development environment can be provided using systemd
units, as described below.


Systemd-based development environment
-------------------------------------

A systemd-based development environment is offered which allows running
an instance of exodus-gw along with its dependencies, via systemd
user units. This may be used as a lightweight alternative to running
a complete instance of the service in Kubernetes/OpenShift.

This development environment includes:

- exodus-gw uvicorn server (http)
- sidecar proxy container (https)
- postgres container
- localstack container
- helpers for managing development certs

Note: the sidecar proxy is only enabled when you instantiate the environment
from within Red Hat's network. Otherwise, the service will only be available
via http.


Prerequisites
.............

- The dev env is designed for use on currently supported versions of Fedora Workstation.
- Your login sessions must make use of a systemd user manager.
- You may need to install some packages. If so, the install script will list the needed
  packages for you.


Installation
............

In the exodus-gw repo, run:

.. code-block:: shell

  scripts/systemd/install

If you're missing any needed packages, the script may suggest some ``dnf install``
commands for you to run.

If installation succeeds, various systemd user units will be installed and
set as dependencies of a new ``exodus-gw`` target. The script will also output a few
example commands to get you started with using the dev env.


Uninstallation
..............

If you want to remove the dev env, run:

.. code-block:: shell

  scripts/systemd/uninstall

This will stop any running services and remove the installed systemd user units.

If you also want to erase any persistent state used by the dev env (such as any
changes written to the DB and localstack), run:

.. code-block:: shell

  scripts/systemd/clean


Cheat sheet
...........

Various example commands are listed here which may be useful when working with
the development environment.

.. list-table::
   :header-rows: 1

   * - Command
     - Notes

   * - ``systemctl --user start exodus-gw.target``
     - Start all development services

   * - ``journalctl --user '--unit=exodus-gw-*' -f``
     - Watch logs of all services

   * - ``sudo cp ~/.config/exodus-gw-dev/ca.crt /etc/pki/ca-trust/source/anchors/exodus-gw-dev.crt``

       ``sudo update-ca-trust``

     - Trust development CA certificate.

       It is strongly recommended to ensure that HTTPS is used during development rather than HTTP,
       and without disabling SSL verification. There are significant changes to behavior in boto
       libraries when using HTTPS vs HTTP.

   * - ``curl http://localhost:8000/healthcheck``
     - Sanity check for exodus-gw (http)

   * - ``curl https://localhost:8010/healthcheck``
     - Sanity check for exodus-gw (https).

       This should not require ``--insecure`` or other means of disabling SSL verification.

   * - ``curl --cert my.crt --key my.key https://localhost:8010/whoami``
     - Sanity check of an exodus-gw endpoint using authentication.

       If using the sidecar proxy provided on Red Hat's internal network, this requires
       you to have a valid certificate and key produced by RHCS.
       The method of obtaining these is beyond the scope of this documentation.

   * - ``curl https://localhost:3377``
     - Sanity check for localstack

   * - ``aws configure --profile test``
     - Create a ``test`` profile for AWS CLI.

       In order for exodus-gw to work, an AWS profile must exist in ``~/.aws/credentials``
       matching the ``aws_profile`` in ``exodus-gw.ini``.  This command will help to create
       a profile if you don't already have one.

       It'll prompt for a few parameters:

       - For region, use ``us-east-1``.

       - For access keys, use any dummy value (e.g. ``"x"``) - these won't be used,
         but must be present.

       - Other parameters can be omitted.

   * - ``aws --endpoint-url=https://localhost:3377 s3api create-bucket --bucket my-bucket``
     - Create a bucket in localstack.

       The localstack environment is initially empty, which will make it impossible to
       upload any objects. For upload to work with exodus-gw, you'll want to create one
       or more buckets matching the info in ``exodus-gw.ini``.

   * - ``examples/s3-upload --endpoint-url https://localhost:8010/upload --env test some-file``
     - Upload an object via exodus-gw.

       This will write to the localstack service.
       If you're not sure whether anything really happened, check the logs of
       exodus-gw-localstack.service.

   * - ``psql -h localhost -p 3355 -U exodus-gw``
     - Connect to the postgres database.

       The database will be empty until exodus-gw has started successfully at least once.

   * - ``systemctl --user stop exodus-gw-db``

       ``rm -rf ~/.config/exodus-gw-dev/postgresql/``

       ``systemctl --user start exodus-gw.target``
     - Clean database while leaving other data untouched.

   * - ``systemctl --user stop exodus-gw-localstack``

       ``rm -rf ~/.config/exodus-gw-dev/localstack/``

       ``systemctl --user start exodus-gw.target``
     - Clean localstack while leaving other data untouched.

       Don't forget to recreate any deleted buckets.


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
      http://localhost:8000/some/qa/endpoint

This approach is only necessary if you are accessing the service via http
(for example, if you don't access to the sidecar container).
If you are accessing the service using https, the same certificates and keys as
used for production may be used in your local environment.
