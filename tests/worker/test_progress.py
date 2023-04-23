import logging

from freezegun import freeze_time

from exodus_gw.worker.progress import ProgressLogger


def test_progress_logs_typical(caplog):
    caplog.set_level(logging.INFO, logger="exodus-gw")

    with freeze_time() as time:
        logger = ProgressLogger(message="testing", items_total=100)

        # Initial log
        logger.update(5)

        # More logs - should not generate anything because not enough
        # time has passed
        logger.update(5)
        logger.update(5)

        # Let time pass
        time.tick(10)

        # Now an update should do something
        logger.update(5)

        # Let time pass again
        time.tick(10)

        # Let's say we found some more items (total now 110)
        logger.adjust_total(10)
        logger.update(5)

        # OK, let's finish up
        logger.update(110 - 5 * 5)

    # This is what it should have logged...
    assert caplog.messages == [
        "testing: 5 (of 100) [ 5% ] [0.0 p/sec]",
        "testing: 20 (of 100) [20% ] [2.0 p/sec]",
        "testing: 25 (of 110) [23% ] [1.2 p/sec]",
        "testing: 110 (of 110) [100% ] [5.5 p/sec]",
    ]
