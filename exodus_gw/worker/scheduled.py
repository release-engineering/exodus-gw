import logging
from datetime import datetime, timedelta, timezone

import dramatiq
from sqlalchemy.orm import Session

from exodus_gw.database import db_engine
from exodus_gw.models import Publish, Task
from exodus_gw.schemas import PublishStates, TaskStates
from exodus_gw.settings import Settings

LOG = logging.getLogger("exodus-gw")


class Janitor:
    def __init__(self):
        self.settings = Settings()
        self.db = Session(bind=db_engine(self.settings))
        self.now = datetime.now(timezone.utc)

    def run(self):
        self.fix_timestamps()
        self.fix_abandoned()
        self.clean_old_data()

        self.db.commit()

        LOG.info("Scheduled cleanup has completed")

    def fix_timestamps(self):
        # Fill in missing timestamps on any data.
        #
        # Timestamps are nullable. If we aren't sure the real
        # updated timestamp on a particular object, we'll just
        # pretend it was updated right now.
        for klass in [Task, Publish]:
            for instance in self.db.query(klass).filter(klass.updated == None):
                LOG.warning(
                    "%s %s: setting updated",
                    klass.__name__,
                    instance.id,
                )
                instance.updated = self.now

    def fix_abandoned(self):
        # Find any publishes and tasks which appear to be abandoned (i.e.
        # they did not complete and have not been updated for a long time)
        # and mark them as failed.
        #
        # This covers scenarios:
        #
        # - a client created a publish, then crashed before committing it.
        #
        # - an internal error in exodus-gw somehow prevented a task from being
        #   executed and also prevented marking the task as failed, such as
        #   an extended outage from the DB.
        #
        hours = self.settings.publish_timeout
        threshold = self.now - timedelta(hours=hours)

        for klass, states in [(Task, TaskStates), (Publish, PublishStates)]:
            for instance in self.db.query(klass).filter(
                # Anything old enough...
                klass.updated < threshold,
                # And also not in a terminal state...
                ~klass.state.in_(states.terminal()),
            ):
                LOG.warning(
                    "%s %s: marking as failed (last updated: %s)",
                    klass.__name__,
                    instance.id,
                    instance.updated,
                )
                instance.state = states.failed

    def clean_old_data(self):
        # Find any objects of transient types in terminal states which have not
        # been updated for the configured period of time and delete them.
        #
        # This helps enforce the design that exodus-gw contains no persistent
        # state.
        hours = self.settings.history_timeout
        threshold = self.now - timedelta(hours=hours)

        for klass, states in [(Task, TaskStates), (Publish, PublishStates)]:
            for instance in self.db.query(klass).filter(
                # Anything old enough...
                klass.updated < threshold,
                # And also in a terminal state so there will be no further updates...
                klass.state.in_(states.terminal()),
            ):
                LOG.info(
                    "%s %s: cleaning old data (last updated: %s)",
                    klass.__name__,
                    instance.id,
                    instance.updated,
                )
                self.db.delete(instance)


@dramatiq.actor(scheduled=True)
def cleanup():
    janitor = Janitor()
    janitor.run()
