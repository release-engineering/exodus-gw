from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import schemas
from ..auth import CallContext, call_context
from ..crud import create_publish
from ..database import SessionLocal

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/healthcheck", tags=["service"])
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}


@router.post(
    "/{env}/publish", response_model=schemas.Publish, tags=["service"]
)
async def publish(env: str, db: Session = Depends(get_db)):
    """Returns a new, empty publish object"""
    if env not in ["dev", "qa", "stage", "prod"]:
        return {"error": "environment {0} not found".format(env)}

    return create_publish(db)


@router.get(
    "/whoami",
    response_model=CallContext,
    tags=["service"],
    responses={
        200: {
            "description": "returns caller's auth context",
            "content": {
                "application/json": {
                    "example": {
                        "client": {
                            "roles": ["someRole", "anotherRole"],
                            "authenticated": True,
                            "serviceAccountId": "clientappname",
                        },
                        "user": {
                            "roles": ["viewer"],
                            "authenticated": True,
                            "internalUsername": "someuser",
                        },
                    }
                }
            },
        }
    },
)
def whoami(context: CallContext = Depends(call_context)):
    """Return basic information on the caller's authentication & authorization context.

    This endpoint may be used to determine whether the caller is authenticated to
    the exodus-gw service, and if so, which set of role(s) are held by the caller.

    It is a read-only endpoint intended for diagnosing authentication issues.
    """
    return context
