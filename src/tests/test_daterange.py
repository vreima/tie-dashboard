import pytest

from src.util.daterange import DateRange
import arrow

@pytest.fixture
def before():
    return arrow.get("2023-05-08")

@pytest.fixture
def at_start():
    return arrow.get("2023-05-10")

@pytest.fixture
def in_span():
    return arrow.get("2023-05-12")

@pytest.fixture
def at_end():
    return arrow.get("2023-05-15")

@pytest.fixture
def after():
    return arrow.get("2023-05-17")

@pytest.fixture
def span(at_start, at_end):
    return DateRange(at_start, at_end)

class TestDateRange:
    def test_cut_before(self, span, before):
        print(span)
        print(before)
        a,b = span.cut(before)
        assert bool(a) == False
        assert b == span

    def test_cut_after(self, span, after):
        a,b = span.cut(after)
        assert bool(b) == False
        assert a == span