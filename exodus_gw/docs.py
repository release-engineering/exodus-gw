import os

DEFAULT_OVERVIEW = ""

DEFAULT_AUTHENTICATION = """
The exodus-gw API does not include any direct support for authentication and is
instead expected to be deployed behind a reverse-proxy implementing any desired
authentication mechanism.

If you are deploying an instance of exodus-gw, see
[the deployment guide](https://release-engineering.github.io/exodus-gw/deployment.html)
for information on how to integrate an authentication mechanism.

If you are a client looking to make use of exodus-gw, consult your organization's
internal documentation for advice on how to authenticate with exodus-gw.
"""

DEFAULT_ENVIRONMENTS = """
The set of environments is configured when exodus-gw is deployed.
A typical scenario is to deploy a "pre" environment for pre-release content and a
"live" environment for live content.

Different environments will also require the user to hold different roles. For example,
a client might be permitted only to write to one of the configured environments, or all
of them, depending on the configuration of the server.

If you are deploying an instance of exodus-gw, see
[the deployment guide](https://release-engineering.github.io/exodus-gw/deployment.html)
for information on how to configure environments.

If you are a client looking to make use of exodus-gw, consult your organization's
internal documentation for advice on which environment(s) you should be using.
"""


def format_docs(docstring: str) -> str:
    return docstring.format(
        OVERVIEW=os.getenv("EXODUS_GW_DOCS_OVERVIEW") or DEFAULT_OVERVIEW,
        AUTHENTICATION=os.getenv("EXODUS_GW_DOCS_AUTHENTICATION")
        or DEFAULT_AUTHENTICATION,
        ENVIRONMENTS=os.getenv("EXODUS_GW_DOCS_ENVIRONMENTS")
        or DEFAULT_ENVIRONMENTS,
    )
