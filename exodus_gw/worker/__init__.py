import dramatiq

from exodus_gw.dramatiq import Broker

dramatiq.set_broker(Broker())

# Imports must occur after the broker is set.
# flake8 complains about that, hence the noqa.

from .deploy import deploy_config  # noqa
from .publish import commit  # noqa
from .scheduled import cleanup  # noqa
