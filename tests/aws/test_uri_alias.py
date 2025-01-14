from logging import DEBUG

import pytest

from exodus_gw.aws.util import uri_alias


@pytest.mark.parametrize(
    "input,aliases,output",
    [
        (
            "/content/origin/rpms/path/to/file.iso",
            [
                ("/content/origin", "/origin", []),
                ("/origin/rpm", "/origin/rpms", []),
            ],
            [
                "/origin/rpms/path/to/file.iso",
                "/content/origin/rpms/path/to/file.iso",
            ],
        ),
        (
            "/content/dist/rhel8/8/path/to/file.rpm",
            [
                ("/content/dist/rhel8/8", "/content/dist/rhel8/8.5", []),
            ],
            [
                "/content/dist/rhel8/8.5/path/to/file.rpm",
                "/content/dist/rhel8/8/path/to/file.rpm",
            ],
        ),
    ],
    ids=["origin", "releasever"],
)
def test_uri_alias(input, aliases, output, caplog):
    caplog.set_level(DEBUG, logger="exodus-gw")
    assert uri_alias(input, aliases) == output
    assert (
        f'"message": "Resolved alias:\\n\\tsrc: {input}\\n\\tdest: {output[0]}", '
        '"event": "publish", '
        '"success": true' in caplog.text
    )


def test_uri_alias_multi_level_write():
    # uri_alias should support resolving aliases multiple levels deep
    # and return values in the right order, using single-direction aliases
    # (as is typical in the write case)
    uri = "/content/other/1/repo"
    aliases = [
        # The data here is made up as there is not currently any identified
        # realistic scenario having multi-level aliases during write.
        ("/content/testproduct/1", "/content/testproduct/1.1.0", []),
        ("/content/other", "/content/testproduct", []),
    ]

    out = uri_alias(uri, aliases)
    assert out == [
        "/content/testproduct/1.1.0/repo",
        "/content/testproduct/1/repo",
        "/content/other/1/repo",
    ]


def test_uri_alias_multi_level_flush():
    # uri_alias should support resolving aliases multiple levels deep
    # and return values in the right order, using bi-directional aliases
    # (as is typical in the flush case).
    #
    # This data is realistic for the common case where releasever
    # and rhui aliases are both in play.

    uri = "/content/dist/rhel8/8/some-repo/"
    aliases = [
        # The caller is providing aliases in both src => dest and
        # dest => src directions, as in the "cache flush" case.
        ("/content/dist/rhel8/8", "/content/dist/rhel8/8.8", []),
        ("/content/dist/rhel8/8.8", "/content/dist/rhel8/8", []),
        ("/content/dist/rhel8/rhui", "/content/dist/rhel8", []),
        ("/content/dist/rhel8", "/content/dist/rhel8/rhui", []),
    ]

    out = uri_alias(uri, aliases)
    # We don't verify the order here because, with bi-directional aliases
    # provided, it does not really make sense to consider either side of
    # the alias as "deeper" than the other.
    assert sorted(out) == sorted(
        [
            # It should return the repo on both sides of the
            # releasever alias...
            "/content/dist/rhel8/8/some-repo/",
            "/content/dist/rhel8/8.8/some-repo/",
            # And *also* on both sides of the releasever alias, beyond
            # the RHUI alias.
            "/content/dist/rhel8/rhui/8/some-repo/",
            "/content/dist/rhel8/rhui/8.8/some-repo/",
        ]
    )


def test_uri_alias_limit(caplog: pytest.LogCaptureFixture):
    # uri_alias applies some limit on the alias resolution depth.
    #
    # This test exists to exercise the path of code intended to
    # prevent runaway recursion. There is no known way how to
    # actually trigger runaway recursion, so we are just providing
    # an unrealistic config with more levels of alias than are
    # actually used on production.

    uri = "/path/a/repo"
    aliases: list[tuple[str, str, list[str]]] = [
        ("/path/a", "/path/b", []),
        ("/path/b", "/path/c", []),
        ("/path/c", "/path/d", []),
        ("/path/d", "/path/e", []),
        ("/path/e", "/path/f", []),
        ("/path/f", "/path/g", []),
        ("/path/g", "/path/h", []),
        ("/path/h", "/path/i", []),
    ]

    out = uri_alias(uri, aliases)

    # It should have stopped resolving aliases at some point.
    # Note that exactly where it stops is rather abitrary, the
    # max depth currently is simply hardcoded.
    assert out == [
        "/path/f/repo",
        "/path/e/repo",
        "/path/d/repo",
        "/path/c/repo",
        "/path/b/repo",
        "/path/a/repo",
    ]

    # It should have warned us about this.
    assert (
        "Aliases too deeply nested, bailing out at /path/f/repo" in caplog.text
    )


@pytest.mark.parametrize(
    "input,aliases,output,log_message",
    [
        (
            "/content/dist/rhel9/9/x86_64/baseos/iso/PULP_MANIFEST",
            [
                (
                    "/content/dist/rhel9/9",
                    "/content/dist/rhel9/9.5",
                    ["/iso/"],
                ),
            ],
            # just returns the original
            ["/content/dist/rhel9/9/x86_64/baseos/iso/PULP_MANIFEST"],
            "Aliasing for /content/dist/rhel9/9/x86_64/baseos/iso/PULP_MANIFEST "
            "was not applied as it matches one of the following exclusion paths: /iso/.",
        ),
        (
            "/some/path/with/file/in/it",
            [
                (
                    "/some/path",
                    "/another/different/path",
                    ["/none/", "/here/"],
                ),
                ("/another/different/path", "/this/wont/alias", ["/file/"]),
            ],
            [
                "/another/different/path/with/file/in/it",
                "/some/path/with/file/in/it",
            ],
            "Aliasing for /another/different/path/with/file/in/it was not "
            "applied as it matches one of the following exclusion paths: /file/.",
        ),
        (
            "/my/base/content/path/cool_iso_tool.rpm",
            [
                ("/my/base", "/your/own", ["/iso/"]),
            ],
            [
                "/your/own/content/path/cool_iso_tool.rpm",
                "/my/base/content/path/cool_iso_tool.rpm",
            ],
            "Resolved alias:\\n\\tsrc: /my/base/content/path/cool_iso_tool.rpm"
            "\\n\\tdest: /your/own/content/path/cool_iso_tool.rpm",
        ),
        (
            "/content/dist/rhel9/9.5/x86_64/baseos/iso/PULP_MANIFEST",
            [
                ("/content/dist", "/alias/path", ["/rhel[89]/"]),
            ],
            ["/content/dist/rhel9/9.5/x86_64/baseos/iso/PULP_MANIFEST"],
            "Aliasing for /content/dist/rhel9/9.5/x86_64/baseos/iso/PULP_MANIFEST "
            "was not applied as it matches one of the following exclusion "
            "paths: /rhel[89]/.",
        ),
        (
            "/content/dist/rhel7/7.5/x86_64/baseos/iso/PULP_MANIFEST",
            [
                ("/content/dist", "/alias/path", ["/rhel[89]/"]),
            ],
            [
                "/alias/path/rhel7/7.5/x86_64/baseos/iso/PULP_MANIFEST",
                "/content/dist/rhel7/7.5/x86_64/baseos/iso/PULP_MANIFEST",
            ],
            "Resolved alias:\\n\\tsrc: /content/dist/rhel7/7.5/x86_64/baseos/iso/PULP_MANIFEST"
            "\\n\\tdest: /alias/path/rhel7/7.5/x86_64/baseos/iso/PULP_MANIFEST",
        ),
    ],
    ids=[
        "basic",
        "transitive",
        "filename",
        "pattern_include",
        "pattern_exclude",
    ],
)
def test_uri_alias_exclusions(input, aliases, output, log_message, caplog):
    caplog.set_level(DEBUG, logger="exodus-gw")
    assert uri_alias(input, aliases) == output
    assert (
        f'"message": "{log_message}", '
        '"event": "publish", '
        '"success": true' in caplog.text
    )
