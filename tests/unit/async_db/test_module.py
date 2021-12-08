import firebolt.async_db


def test_has_exceptions(db_api_exceptions):
    """Verify async module has top-level dbapi exceptions exposed"""
    for ex_name, ex_class in db_api_exceptions.items():
        assert issubclass(getattr(firebolt.async_db, ex_name), ex_class)
