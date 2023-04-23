import logging
from threading import Lock
from time import monotonic

LOG = logging.getLogger("exodus-gw")


class ProgressLogger:
    """A helper to generate progress logs during long-running processes."""

    def __init__(self, message: str, items_total: int, interval: float = 5.0):
        """Construct a progress logger.

        Arguments:
            message
                A prefix for each generated log message.
                Should indicate the operation taking place, e.g. "Writing items"

            items_total
                Total number of items expected to be processed.

            interval
                Minimum time in seconds between log messages.
                The logger will not produce messages more frequently than
                this.
        """
        self.message = message
        self.lock = Lock()
        self.items_total = items_total
        self.items_processed = 0
        self.start_time = monotonic()
        self.last_write = 0.0
        self.interval = interval

    def adjust_total(self, increment: int):
        """Add or subtract from the configured items_total.

        This can be used to adjust the item total on the fly if
        the earlier total was estimated and the estimate has
        been updated.
        """
        with self.lock:
            self.items_total += increment

    def update(self, increment: int):
        """Add given value to the count of processed items, possibly
        triggering a log message.

        This should be called repeatedly during a long-running operation
        to produce log messages.
        """

        total = self.items_total
        now = monotonic()

        with self.lock:
            self.items_processed += increment
            processed = self.items_processed

            # Prevents log messages from writing too frequently.
            # But if we've just passed the expected total, we should always log.
            if (
                processed < self.items_total
                and now - self.last_write < self.interval
            ):
                return

            self.last_write = now

        percent = processed / total * 100
        runtime = now - self.start_time
        items_per_second = processed / runtime if runtime > 0.01 else 0

        # Example message:
        # Writing phase 1 items: 775 (of 10000) [ 8% ] [6.9 p/sec]
        LOG.info(
            "%s: %s (of %s) [%2.0f%% ] [%2.1f p/sec]",
            self.message,
            processed,
            total,
            percent,
            items_per_second,
        )
