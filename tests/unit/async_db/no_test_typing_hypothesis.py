from datetime import date, datetime

import hypothesis.strategies as st
from hypothesis import given, settings
from pytest import mark, raises

from firebolt.common._types import parse_value


@given(st.integers())
def test_parse_value_int(i) -> None:
    """parse_value parses all int values correctly."""
    assert parse_value(i, int) == i
    assert parse_value(str(i), int) == i


def everything_except(excluded_types):
    return (
        st.from_type(type)
        .flatmap(st.from_type)
        .filter(lambda x: not isinstance(x, excluded_types) and x is not None)
    )


@mark.skip
@given(everything_except(int))
def test_no_parse_value_int(i):
    with raises(ValueError):
        parse_value(i, int)


@given(st.dates())
def test_parse_value_date(d) -> None:
    assert parse_value(str(d), date) == d


# @given(st.from_regex("\d{4}-(0?[1-9]|1[012])-(0?[1-9]|[12]\d|3[01])"))
# def test_parse_value_date_generated(d):
#     assert type(parse_value(d, date)) == date

# dt = re.compile("/\d{4}-(0?[1-9]|1[012])-(0?[1-9]|[12]\d|3[01])( |T)(0?[1-9]|1\d|2[0-3]):(0?[1-9]|[1-5]\d):(0?[1-9]|[1-5]\d)(\.\d{0,6})?$/gm")

# @settings(suppress_health_check=[HealthCheck.filter_too_much])
# @given(st.from_regex(dt))
# def test_parse_value_date_generated(d):
#     assert type(parse_value(d, datetime)) == datetime


@st.composite
def various_dates_zp(draw, d=st.datetimes()):
    dt: datetime = draw(d)
    # zero-padded
    s = dt.strftime(r"%Y-%m-%d %H:%M:%S.%f%z")
    return (s, dt)


@st.composite
def various_dates_nzp(draw, d=st.datetimes()):
    dt: datetime = draw(d)
    # zero-padded
    s = dt.strftime(r"%Y-%-m-%-d %-H:%-M:%-S.%f%z")
    return (s, dt)


@settings(max_examples=500)
@given(various_dates_zp())
def test_parse_value_date_generated(d):
    string_date = d[0]
    date = d[1]
    assert parse_value(string_date, datetime) == date


@settings(max_examples=500)
@given(various_dates_nzp())
def test_parse_value_date_generated_nzp(d):
    string_date = d[0]
    date = d[1]
    assert parse_value(string_date, datetime) == date


@st.composite
def various_dates_iso(draw, d=st.datetimes(), sep=st.sampled_from([" ", "T"])):
    dt: datetime = draw(d)
    # zero-padded
    s = dt.isoformat(sep=draw(sep))
    return (s, dt)


@settings(max_examples=500)
@given(various_dates_iso())
def test_parse_value_date_generated_iso(d):
    string_date, date = d
    assert parse_value(string_date, datetime) == date
