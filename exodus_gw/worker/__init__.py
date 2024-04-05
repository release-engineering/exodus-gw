import dramatiq

from exodus_gw.dramatiq import Broker

dramatiq.set_broker(Broker())

# Imports must occur after the broker is set.
# flake8 and pylint complain about that, hence the noqa
# and following disabled error.
#
# pylint: disable=wrong-import-position

from .autoindex import autoindex_partial  # noqa
from .cache import flush_cdn_cache  # noqa
from .deploy import deploy_config  # noqa
from .publish import commit  # noqa
from .scheduled import cleanup  # noqa
