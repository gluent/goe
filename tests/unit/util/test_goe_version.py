from goe.util.goe_version import GOEVersion


def test_goe_version():
    assert GOEVersion("3.0") == GOEVersion("3.0.0")
    assert GOEVersion("3.0") == GOEVersion("3.0")
    assert GOEVersion("3.0") == GOEVersion("3")

    assert GOEVersion("3.0") != GOEVersion("3.0.1")
    assert GOEVersion("3.1") != GOEVersion("3.0")
    assert GOEVersion("3.1") != GOEVersion("3")

    assert GOEVersion("3.1") > GOEVersion("3.0.1")
    assert GOEVersion("3.1") > GOEVersion("3.0")
    assert GOEVersion("3.1") > GOEVersion("3")

    assert GOEVersion("3.0") < GOEVersion("3.0.1")
    assert GOEVersion("3.0") < GOEVersion("3.1")
    assert GOEVersion("3.0") < GOEVersion("4")

    assert GOEVersion("3.0.0") == GOEVersion("3.0.0")
    assert GOEVersion("3.0.0") == GOEVersion("3.0")
    assert GOEVersion("3.0.0") == GOEVersion("3")

    assert GOEVersion("3.0.0") != GOEVersion("3.0.1")
    assert GOEVersion("3.0.1") != GOEVersion("3.0")
    assert GOEVersion("3.0.1") != GOEVersion("3")

    assert GOEVersion("3.0.2") > GOEVersion("3.0.1")
    assert GOEVersion("3.0.1") > GOEVersion("3.0")
    assert GOEVersion("3.0.1") > GOEVersion("3")

    assert GOEVersion("3.0.0") < GOEVersion("3.0.1")
    assert GOEVersion("3.0.0") < GOEVersion("3.1")
    assert GOEVersion("3.0.0") < GOEVersion("4")


def test_goe_version_with_alpha():
    assert GOEVersion("3.1.2-RC") == GOEVersion("3.1.2-RC")
    assert GOEVersion("3.1.2-DEV") == GOEVersion("3.1.2-DEV")
    assert GOEVersion("3.1.2-RC") != GOEVersion("3.1.2-DEV")

    assert GOEVersion("3.1.0-RC") > GOEVersion("3.0.0-RC")
    assert GOEVersion("3.1-RC") > GOEVersion("3.0-RC")
    assert GOEVersion("3.1.0-RC") > GOEVersion("3.0.0-DEV")
