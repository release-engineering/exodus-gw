"""APIs for accessing the Exodus CDN via URL.

## CDN Redirect

The cdn_redirect API signs and redirects the given URL to the given
environment's AWS CloudFront distribution.
"""

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
from fastapi import APIRouter, HTTPException
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


@router.head("/{env}/cdn/{url:path}", response_model=schemas.BaseModel)
@router.get("/{env}/cdn/{url:path}", response_model=schemas.BaseModel)
def cdn_redirect(
    url: str, settings: Settings = deps.settings, env: Environment = deps.env
):
    """Constructs a new URL from the given URL and the environment's CDN root
    URL, signs it using private key and key ID pair, and returns a redirect
    response to the signed URL.
    """
    signed_url = sign_url(url, settings.cdn_signature_timeout, env)
    return Response(
        content=None, headers={"location": signed_url}, status_code=302
    )
