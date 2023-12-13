from collections.abc import Iterable
from hashlib import sha256
from itertools import islice
from typing import TypeVar

from fastapi.testclient import TestClient

from exodus_gw.main import app
from exodus_gw.models import Publish

T = TypeVar("T")


def object_key(name: str):
    return sha256(name.encode()).hexdigest().lower()


def origin_items(count: int):
    for i in range(1, count + 1):
        filename = f"test-package-{i}.noarch.rpm"
        yield {
            "web_uri": f"/origin/rpms/{filename}",
            "object_key": object_key(filename),
            "content_type": "application/x-rpm",
        }


def package_items(count: int):
    for i in range(1, count + 1):
        filename = f"test-package-{i}.noarch.rpm"
        yield {
            "web_uri": f"/content/some-repo/Packages/{filename}",
            "link_to": f"/origin/rpms/{filename}",
        }


# TODO: in python 3.12 use itertools.batched
def batched(iterable: Iterable[T], n: int):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def test_update_publish_items_large(db, auth_header):
    """Performance test putting a large number of items onto a publish."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"

    publish = Publish(id=publish_id, env="test", state="PENDING")
    db.add(publish)
    db.commit()

    # This test is trying to simulate performance of a force publish of
    # a large rhsm-pulp repo. 35000 is a realistic count of RPMs for
    # some repos.
    #
    # 10000 is the default batch size used by exodus-rsync.
    package_count = 35000
    batch_size = 10000

    # Produce two lists of items to add to the publish.
    #
    # The 'origin' list represents pulp's rsync under /origin (cdn_path) and
    # uses non-link items.
    #
    # The 'package' list represents Pulp's rsync of Packages directory in a yum
    # repo, which uses link items to /origin.
    #
    # In both cases we force eager creation of the lists now so it doesn't count
    # against later performance measurements.
    all_origin_items = list(origin_items(package_count))
    all_package_items = list(package_items(package_count))

    # Now arrange them in the actual batches which will be used during PUT.
    # This should be similar to the way exodus-rsync would batch them in real usage.
    batched_origin_items = batched(all_origin_items, batch_size)
    batched_package_items = batched(all_package_items, batch_size)

    with TestClient(app) as client:
        for batch in batched_origin_items:
            r = client.put(
                "/test/publish/%s" % publish_id,
                json=batch,
                headers=auth_header(roles=["test-publisher"]),
            )
            assert r.status_code == 200

        for batch in batched_package_items:
            r = client.put(
                "/test/publish/%s" % publish_id,
                json=batch,
                headers=auth_header(roles=["test-publisher"]),
            )
            assert r.status_code == 200

    # Verify expected number of items were added
    db.refresh(publish)
    assert len(publish.items) == package_count * 2
