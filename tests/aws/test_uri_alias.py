from logging import DEBUG

import pytest

from exodus_gw.aws.util import uri_alias


@pytest.mark.parametrize(
    "input,aliases,output",
    [
        (
            "/content/origin/rpms/path/to/file.iso",
            [
                {"dest": "/origin", "src": "/content/origin"},
                {"dest": "/origin/rpms", "src": "/origin/rpm"},
            ],
            "/origin/rpms/path/to/file.iso",
        ),
        (
            "/content/dist/rhel8/8/path/to/file.rpm",
            [
                {
                    "src": "/content/dist/rhel8/8",
                    "dest": "/content/dist/rhel8/8.5",
                }
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
