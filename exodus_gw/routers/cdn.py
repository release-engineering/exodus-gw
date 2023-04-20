"""Utilities for accessing the Exodus CDN."""

import base64
import json
import logging
import os
import urllib.parse
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from botocore.utils import datetime2timestamp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.responses import Response

from exodus_gw import auth, schemas

from .. import deps
from ..settings import Environment, Settings

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "cdn", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


def build_policy(url: str, expiration: datetime):
    datelessthan = int(datetime2timestamp(expiration))
    condition = {"DateLessThan": {"AWS:EpochTime": datelessthan}}
    payload = [("Resource", url), ("Condition", condition)]
    policy = {"Statement": [OrderedDict(payload)]}
    return json.dumps(policy, separators=(",", ":")).encode("utf-8")


def rsa_signer(private_key: str, policy: bytes):
    bytes_key = bytes(private_key, "utf-8")
    loaded_key = serialization.load_pem_private_key(
        bytes_key, password=None, backend=default_backend()
    )
    return loaded_key.sign(policy, padding.PKCS1v15(), hashes.SHA1())  # type: ignore # nosec


def cf_b64(data: bytes):
    return (
        base64.b64encode(data)
        .replace(b"+", b"-")
        .replace(b"=", b"_")
        .replace(b"/", b"~")
    )


def sign_url(url: str, timeout: int, env: Environment):
    if not env.cdn_url:
        LOG.error("Missing cdn_url in exodus-gw environment settings")
        raise HTTPException(
            status_code=500,
            detail="Missing cdn_url, nowhere to redirect request",
        )
    if not env.cdn_key_id:
        LOG.error("Missing cdn_key_id in exodus-gw environment settings")
        raise HTTPException(
            status_code=500, detail="Missing key ID for CDN access"
        )
    if not env.cdn_private_key:
        LOG.error("CDN_PRIVATE_KEY_%s is unset", env.name.upper())
        raise HTTPException(
            status_code=500, detail="Missing private key for CDN access"
        )

    dest_url = os.path.join(env.cdn_url, url)
    expiration = datetime.now(timezone.utc) + timedelta(seconds=timeout)

    LOG.info("redirecting %s to %s. . .", url, dest_url)

    policy = build_policy(dest_url, expiration)
    signature = rsa_signer(env.cdn_private_key, policy)
    params = [
        "Expires=%s" % int(datetime2timestamp(expiration)),
        "Signature=%s" % cf_b64(signature).decode("utf8"),
        "Key-Pair-Id=%s" % env.cdn_key_id,
    ]
    separator = "&" if "?" in url else "?"
    return dest_url + separator + "&".join(params)


Url = Path(
    ...,
    title="URL",
    description="URL of a piece of content relative to CDN root",
    example="content/dist/rhel8/8/x86_64/baseos/os/repodata/repomd.xml",
)


redirect_common = dict(
    status_code=302,
    response_model=schemas.EmptyResponse,
    responses={
        302: {
            "description": "Redirect",
            "headers": {
                "location": {
                    "description": "An absolute, signed, temporary URL of CDN content"
                }
            },
        }
    },
)


@router.head(
    "/{env}/cdn/{url:path}",
    summary="Redirect (HEAD)",
    # overriding description here avoids repeating the main doc text
    # under both GET and HEAD methods.
    description="Identical to GET redirect, but for HEAD method.",
    **redirect_common,  # type: ignore
)
@router.get(
    "/{env}/cdn/{url:path}", summary="Redirect (GET)", **redirect_common  # type: ignore
)
def cdn_redirect(
    url: str = Url,
    settings: Settings = deps.settings,
    env: Environment = deps.env,
):
    """Redirects to a requested URL on the CDN.

    The CDN requires a signature from an authorized signer in order to permit
    requests. When using this endpoint, exodus-gw acts as an authorized signer
    on the caller's behalf, thus allowing any exodus-gw client to access CDN
    content without holding the signing keys.

    The URL used in the redirect will become invalid after a server-defined
    timeout, typically less than one hour.
    """
    signed_url = sign_url(url, settings.cdn_signature_timeout, env)
    return Response(
        content=None, headers={"location": signed_url}, status_code=302
    )


@router.get(
    "/{env}/cdn-access",
    summary="Access",
    status_code=200,
    dependencies=[auth.needs_role("cdn-consumer")],
    response_model=schemas.AccessResponse,
)
def cdn_access(
    expire_days: int = Query(
        # The following default is invalid and is set just to share a single
        # validation path below for unset, too small and too large values.
        default=-1,
        description=(
            "Desired expiration time, in days, for generated signatures.\n\n"
            "It is mandatory to provide a value.\n\n"
            "Cannot exceed a maximum value configured by the "
            "server (typically 365 days)."
        ),
        example=30,
    ),
    settings: Settings = deps.settings,
    env: Environment = deps.env,
    call_context: auth.CallContext = deps.call_context,
):
    """Obtain signed cookies and other information needed for accessing
    a specific CDN environment.

    This endpoint may be used to look up the CDN origin server belonging
    to a particular environment and to obtain long-term signed cookies
    authorizing requests to that environment. The cookies returned by
    this endpoint should be treated as a secret.

    **Required roles**: `{env}-cdn-consumer`
    """

    if expire_days < 1 or expire_days > settings.cdn_max_expire_days:
        raise HTTPException(
            400,
            detail=(
                "An expire_days option from 1 "
                f"to {settings.cdn_max_expire_days} must be provided"
            ),
        )

    parsed_url = urllib.parse.urlparse(env.cdn_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    # Note: it would be nice to generate separate cookies for /origin/*
    # and /content/* resources, like the browser-oriented /_/cookie
    # endpoint on exodus-lambda does, to help ensure all CDN access is
    # locked down to those paths only.
    #
    # The problem with it is that CloudFront doesn't seem to accept
    # multiple cookies provided by the client at once (seems undefined
    # which one of the cookies is used). That means the client has to
    # only provide the cookie which is relevant for the path being
    # accessed.
    #
    # This is not an issue for a proper cookie engine as found
    # in a browser or curl, which will implement that as a standard
    # feature. But it is significantly onerous for some expected usage
    # of these cookies: CDN edge servers cannot simply add
    # a hardcoded Cookie header when contacting cloudfront but would
    # instead have to branch based on which subtree is accessed.
    #
    # It seems as though it'd be unreasonable to require that complexity,
    # so we're just going to generate a single cookie for "/*" and let
    # the client provide one cookie for all requests.
    resource = f"{base_url}/*"
    expires = datetime.utcnow() + timedelta(days=expire_days)

    policy = build_policy(resource, expires)
    signature = rsa_signer(env.cdn_private_key, policy)
    policy_encoded = cf_b64(policy).decode("utf-8")

    components = {
        "CloudFront-Key-Pair-Id": env.cdn_key_id,
        "CloudFront-Policy": policy_encoded,
        "CloudFront-Signature": cf_b64(signature).decode("utf8"),
    }
    cookie = "; ".join(f"{key}={value}" for (key, value) in components.items())

    # Log info with sufficient detail so that all cookies in use can be traced
    # back to the creating user.
    username = (
        call_context.client.serviceAccountId
        or call_context.user.internalUsername
        or "<unknown user>"
    )
    LOG.info(
        "Generated cookie for: user=%s, key=%s, resource=%s, expires=%s, policy=%s",
        username,
        env.cdn_key_id,
        resource,
        expires,
        policy_encoded,
    )

    return {
        "url": base_url,
        "expires": expires.isoformat(timespec="minutes") + "Z",
        "cookie": cookie,
    }
