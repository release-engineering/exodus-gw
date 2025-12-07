from datetime import datetime

import pytest
from sqlalchemy.exc import StatementError

from exodus_gw.models import Item, Publish


def test_utcdatetime(db, fake_publish):
    """
    Ensure UTCDateTime raises a TypeError when a native datetime is passed in. This
    error only occurs when the invalid UTCDateTime is commited to the DB.
    """
    with pytest.raises(StatementError) as exc_info:
        fake_publish.items.append(
            Item(
                web_uri="/content/testproduct/1/repo/.__exodus_autoindex",
                object_key="5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
                publish_id="123e4567-e89b-12d3-a456-426614174000",
                updated=datetime(2023, 10, 4, 3, 52, 2),
            )
        )
        db.add(fake_publish)
        db.commit()

    assert "(builtins.TypeError) tzinfo is required" in str(exc_info)
