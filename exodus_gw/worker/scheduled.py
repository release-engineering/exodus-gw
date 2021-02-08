import logging

import dramatiq

LOG = logging.getLogger("exodus-gw")


@dramatiq.actor(scheduled=True)
def cleanup():
    # TODO: implement me.
    #
    # We must first implement:
    # - timestamps on task and publish objects
    # - state on publish objects
    #
    LOG.warning("Would do cleanup now")
