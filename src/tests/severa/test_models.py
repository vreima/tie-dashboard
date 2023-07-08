import arrow
import pytest

from src.util.process import search_string_for_datetime


@pytest.fixture
def list_of_strings_with_valid_datetimes():
    return [
        (
            "Tarjouspyyntö DL 31.3.2023",
            arrow.Arrow(2023, 3, 31, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 31.03.2023",
            arrow.Arrow(2023, 3, 31, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 01.03.2023",
            arrow.Arrow(2023, 3, 1, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 31.3.2023 klo 12",
            arrow.Arrow(2023, 3, 31, 12, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 12.3.2023 klo 12:30",
            arrow.Arrow(2023, 3, 12, 12, 30, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 12.3.2023 12:30",
            arrow.Arrow(2023, 3, 12, 12, 30, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 12.3.2023   12:30",
            arrow.Arrow(2023, 3, 12, 12, 30, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 12.3. klo 12:30",
            arrow.Arrow(2023, 3, 12, 12, 30, tzinfo="Europe/Helsinki"),
        ),
        (
            "*Tarjouspyyntö DL 12.3. klo 12:30*",
            arrow.Arrow(2023, 3, 12, 12, 30, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 12.3. 12:30",
            arrow.Arrow(2023, 3, 12, 12, 30, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 12-3-2023",
            arrow.Arrow(2023, 3, 12, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL 2023-3-12",
            arrow.Arrow(2023, 3, 12, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö tietomallikoordinointi YIT Kruunis - DL 13.3.",
            arrow.Arrow(2023, 3, 13, tzinfo="Europe/Helsinki"),
        ),
        (
            "*Tarjouspyyntö tietomallikoordinointi YIT Kruunis - DL 13.3.*",
            arrow.Arrow(2023, 3, 13, tzinfo="Europe/Helsinki"),
        ),
        # ("Karhulan koulukeskus ja kirjasto, tietomallikoordinointi. DL ma.6.3. klo.12:00", arrow.Arrow(2023, 3, 6, 12, tzinfo="Europe/Helsinki")),
        (
            "Ahvenisjärven koulu, DL 27.02.2023 klo 10:00",
            arrow.Arrow(2023, 2, 27, 10, tzinfo="Europe/Helsinki"),
        ),
        (
            "Tarjouspyyntö DL vko 27",
            arrow.Arrow(2023, 7, 7, 12, tzinfo="Europe/Helsinki"),
        ),
    ]


@pytest.fixture
def list_of_strings_with_invalid_datetimes():
    return [
        "Tarjouspyyntö DL 31.3.203 klo 12",
        "Tarjouspyyntö DL 31.13.",
        "",
        "Ei numeroita",
    ]


class TestDateTimeSearch:
    def test_valid_string_datetime_search(self, list_of_strings_with_valid_datetimes):
        for q, a in list_of_strings_with_valid_datetimes:
            assert search_string_for_datetime(q) == a

    def test_invalid_string_datetime_search(
        self, list_of_strings_with_invalid_datetimes
    ):
        for q in list_of_strings_with_invalid_datetimes:
            assert search_string_for_datetime(q) is None
