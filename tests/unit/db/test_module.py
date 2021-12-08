import firebolt.db


def test_has_exceptions(db_api_exceptions):
    """Verify sync module has top-level dbapi exceptions exposed"""
    for ex_name, ex_class in db_api_exceptions.items():
        assert issubclass(getattr(firebolt.db, ex_name), ex_class)
