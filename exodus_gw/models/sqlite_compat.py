"""Make some postgres dialect compatible with sqlite, for use within tests."""

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

# If you're confused how the below can make sqlite "support" types like JSONB,
# it doesn't. sqlite just stores everything as a string no matter
# what and it doesn't matter what we return here, it's just needed to let
# queries compile.


@compiles(JSONB, "sqlite")
def sqlite_jsonb(*_args, **_kwargs):
    return "JSONB"
