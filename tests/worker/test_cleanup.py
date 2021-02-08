from exodus_gw.worker import cleanup

# cleanup is a scheduled actor, we want to test the actor itself
# and not the scheduling mechanism, so unwrap it to get at the
# implementation.

cleanup = cleanup.options["unscheduled_fn"]


def test_cleanup_noop():
    """Calling cleanup doesn't crash."""

    # Actual implementation doesn't exist yet so we don't have
    # any more meaningful test right now.
    cleanup()
