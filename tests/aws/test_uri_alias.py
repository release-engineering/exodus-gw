from logging import DEBUG

import pytest

from exodus_gw.aws.util import uri_alias


@pytest.mark.parametrize(
    "input,aliases,output",
    [
        (
            "/content/origin/rpms/path/to/file.iso",
            [
                ("/content/origin", "/origin"),
                ("/origin/rpm", "/origin/rpms"),
            ],
            "/origin/rpms/path/to/file.iso",
        ),
        (
            "/content/dist/rhel8/8/path/to/file.rpm",
            [
                ("/content/dist/rhel8/8", "/content/dist/rhel8/8.5"),
            ],
            "/content/dist/rhel8/8.5/path/to/file.rpm",
        ),
    ],
    ids=["origin", "releasever"],
)
def test_uri_alias(input, aliases, output, caplog):
    caplog.set_level(DEBUG, logger="exodus-gw")
    assert uri_alias(input, aliases) == output
    assert (
        f'"message": "Resolved alias:\\n\\tsrc: {input}\\n\\tdest: {output}", '
        '"event": "publish", '
        '"success": true' in caplog.text
    )
