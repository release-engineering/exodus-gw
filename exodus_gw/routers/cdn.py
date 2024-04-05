"""Utilities for accessing the Exodus CDN."""

import base64
import json
import logging
import os
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse

from botocore.utils import datetime2timestamp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from fastapi import APIRouter, Body, HTTPException, Path, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from exodus_gw import auth, models, schemas, worker

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


def cf_cookie(url: str, env: Environment, expires: datetime, username: str):
    policy = build_policy(url, expires)
    signature = rsa_signer(env.cdn_private_key, policy)
    policy_encoded = cf_b64(policy).decode("utf-8")
    signature_encoded = cf_b64(signature).decode("utf-8")

    LOG.info(
        "Generated cookie for: user=%s, key=%s, resource=%s, expires=%s, policy=%s",
        username,
        env.cdn_key_id,
        url,
        expires,
        policy_encoded,
        extra={"event": "cdn", "success": True},
    )

    return {
        "CloudFront-Key-Pair-Id": env.cdn_key_id,
        "CloudFront-Policy": policy_encoded,
        "CloudFront-Signature": signature_encoded,
    }


def sign_url(url: str, settings: Settings, env: Environment, username: str):
    if not env.cdn_url:
        LOG.error(
            "Missing cdn_url in exodus-gw environment settings",
            extra={"event": "cdn", "success": False},
        )
        raise HTTPException(
            status_code=500,
            detail="Missing cdn_url, nowhere to redirect request",
        )
    if not env.cdn_key_id:
        LOG.error(
            "Missing cdn_key_id in exodus-gw environment settings",
            extra={"event": "cdn", "success": False},
        )
        raise HTTPException(
            status_code=500, detail="Missing key ID for CDN access"
        )
    if not env.cdn_private_key:
        LOG.error(
            "CDN_PRIVATE_KEY_%s is unset",
            env.name.upper(),
            extra={"event": "cdn", "success": False},
        )
        raise HTTPException(
            status_code=500, detail="Missing private key for CDN access"
        )

    dest_url = os.path.join(env.cdn_url, url)
    signature_expires = datetime.now(timezone.utc) + timedelta(
        seconds=settings.cdn_signature_timeout
    )
    cookie_expires = datetime.now(timezone.utc) + timedelta(
        seconds=settings.cdn_cookie_ttl
    )

    LOG.info(
        "redirecting %s to %s. . .",
        url,
        dest_url,
        extra={"event": "cdn", "success": True},
    )

    cookies = []
    for resource in ("/content/", "/origin/"):
        parsed_url = urlparse(env.cdn_url)
        policy_url = f"{parsed_url.scheme}://{parsed_url.netloc}{resource}*"
        cookie = cf_cookie(policy_url, env, cookie_expires, username)
        append = (
            f"; Secure; HttpOnly; SameSite=lax; Domain={parsed_url.netloc}; "
            f"Path={resource}; Max-Age={settings.cdn_cookie_ttl}"
        )
        cookies.extend([f"{k}={v}{append}" for k, v in cookie.items()])

    cookies_bytes = bytes(json.dumps(cookies), "utf-8")
    cookies_encoded = cf_b64(cookies_bytes).decode("utf-8")

    dest_url = f"{dest_url}?CloudFront-Cookies={cookies_encoded}"
    policy = build_policy(dest_url, signature_expires)
    signature = rsa_signer(env.cdn_private_key, policy)

    params = [
        f"Expires={int(datetime2timestamp(signature_expires))}",
        f"Signature={cf_b64(signature).decode('utf8')}",
        f"Key-Pair-Id={env.cdn_key_id}",
    ]
    return f"{dest_url}&{'&'.join(params)}"


Url = Path(
    ...,
    title="URL",
    description="URL of a piece of content relative to CDN root",
    examples=["content/dist/rhel8/8/x86_64/baseos/os/repodata/repomd.xml"],
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
    call_context: auth.CallContext = deps.call_context,
):
    """Redirects to a requested URL on the CDN.

    The CDN requires a signature from an authorized signer in order to permit
    requests. When using this endpoint, exodus-gw acts as an authorized signer
    on the caller's behalf, thus allowing any exodus-gw client to access CDN
    content without holding the signing keys.

    The URL used in the redirect will become invalid after a server-defined
    timeout, typically less than one hour.
    """
    username = (
        call_context.client.serviceAccountId
        or call_context.user.internalUsername
        or "<unknown user>"
    )
    url = quote(url)
    signed_url = sign_url(url, settings, env, username)
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
        examples=[30],
    ),
    resource: str = Query(
        default="/*",
        description=(
            "Desired resource to access. If included, must begin with '/'. Defaults to '/*', providing access to the entire CloudFront distribution."
        ),
        examples=["/content/dist/rhel8/8.2/x86_64/baseos/iso/PULP_MANIFEST"],
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

    if not resource.startswith("/"):
        raise HTTPException(
            400, detail="A resource URL option must begin with '/'"
        )

    if expire_days < 1 or expire_days > settings.cdn_max_expire_days:
        raise HTTPException(
            400,
            detail=(
                "An expire_days option from 1 "
                f"to {settings.cdn_max_expire_days} must be provided"
            ),
        )

    username = (
        call_context.client.serviceAccountId
        or call_context.user.internalUsername
        or "<unknown user>"
    )

    parsed_url = urlparse(env.cdn_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    policy_url = f"{base_url}{resource}"
    expires = datetime.utcnow() + timedelta(days=expire_days)

    cookie = cf_cookie(policy_url, env, expires, username)
    cookie_str = "; ".join(f"{key}={value}" for (key, value) in cookie.items())

    return {
        "url": base_url,
        "expires": expires.isoformat(timespec="minutes") + "Z",
        "cookie": cookie_str,
    }


@router.post(
    "/{env}/cdn-flush",
    summary="Flush cache",
    status_code=200,
    dependencies=[auth.needs_role("cdn-flusher")],
    response_model=schemas.Task,
)
def flush_cdn_cache(
    items: list[schemas.FlushItem] = Body(
        ...,
        examples=[
            [
                {
                    "web_uri": "/some/path/i/want/to/flush",
                },
                {
                    "web_uri": "/another/path/i/want/to/flush",
                },
            ]
        ],
    ),
    deadline: datetime = deps.deadline,
    env: Environment = deps.env,
    db: Session = deps.db,
) -> models.Task:
    """Flush given paths from CDN cache(s) corresponding to this environment.

    This API may be used to request CDN edge servers downstream from exodus-gw
    and exodus-cdn to discard cached versions of content, ensuring that
    subsequent requests will receive up-to-date content.

    The API is provided for troubleshooting and for scenarios where it's
    known that explicit cache flushes are needed. It's not necessary to use
    this API during a typical upload and publish workflow.

    Returns a task. Successful completion of the task indicates that CDN
    caches have been flushed.

    **Required roles**: `{env}-cdn-flusher`
    """
    paths = sorted(set([item.web_uri for item in items]))

    msg = worker.flush_cdn_cache.send(
        env=env.name,
        paths=paths,
    )

    LOG.info(
        "Enqueued cache flush for %s path(s) (%s, ...)",
        len(paths),
        paths[0] if paths else "<empty>",
    )

    task = models.Task(
        id=msg.message_id,
        state="NOT_STARTED",
        deadline=deadline,
    )
    db.add(task)

    return task
