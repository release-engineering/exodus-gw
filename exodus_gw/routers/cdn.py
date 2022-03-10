"""Utilities for accessing the Exodus CDN."""

import base64
import json
import logging
import os
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from botocore.utils import datetime2timestamp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from fastapi import APIRouter, HTTPException, Path
from fastapi.responses import Response

from exodus_gw import schemas

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
    return loaded_key.sign(policy, padding.PKCS1v15(), hashes.SHA1())  # type: ignore


def encode_signature(data: bytes):
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
        "Signature=%s" % encode_signature(signature).decode("utf8"),
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
    **redirect_common  # type: ignore
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
