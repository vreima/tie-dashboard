from typing import Optional, Union

import arrow


class DateRange:
    def __init__(
        self,
        start: Optional[Union[arrow.Arrow, int]] = None,
        end: Optional[Union[arrow.Arrow, int]] = None,
    ):
        if start is None:
            # Empty range
            self._start = None
            self._end = None
        else:
            if end is None:
                if isinstance(start, int):
                    self._start = arrow.utcnow().floor("day")
                    self._end = self._start.shift(days=start)
                else:
                    self._start = start
                    self._end = start
            elif isinstance(end, int) and isinstance(start, arrow.Arrow):
                self._start = start
                self._end = self._start.shift(days=end)
            else:
                assert isinstance(start, arrow.Arrow)  # Keeps mypy happy
                assert isinstance(end, arrow.Arrow)
                self._start = start
                self._end = end

            # Sort the range
            if self._end < self._start:
                self._start, self._end = self._end, self._start

    @property
    def start(self) -> arrow.Arrow:
        if not self:
            raise ValueError(f"{self} is an empty range")

        assert self._start is not None
        return self._start.floor("day")

    @property
    def end(self) -> arrow.Arrow:
        if not self:
            raise ValueError(f"{self} is an empty range")

        assert self._end is not None
        return self._end.ceil("day")

    def __bool__(self) -> bool:
        return self._start is not None

    def __getitem__(self, key: str) -> str:
        if not self:
            raise ValueError(f"{self} is an empty range")

        if key == "startDate":
            return self.start.format("YYYY-MM-DD")
        elif key == "endDate":
            return self.end.format("YYYY-MM-DD")

        raise KeyError(f"key '{key}' not applicaple to DateRange")

    def __len__(self) -> int:
        if not self:
            return 0

        return (self.end - self.start).days + 1

    def __hash__(self):
        return hash((self.start, self.end))

    def __eq__(self, other) -> bool:
        return (
            (bool(self) == bool(other))
            and isinstance(other, type(self))
            and (self.start, self.end)
            == (
                other.start,
                other.end,
            )
        )

    def intersection(self, other: "DateRange") -> "DateRange":
        if (not self) or (not other):
            # Intersections with empty ranges are empty
            return DateRange()

        if self.start <= other.start <= self.end:
            return DateRange(other.start, min(other.end, self.end))
        elif other.start <= self.start <= other.end:
            return DateRange(self.start, min(other.end, self.end))

        # Otherwise the intersection is empty
        return DateRange()

    def contains(self, date: arrow.Arrow) -> bool:
        return bool(self) and (self.start <= date <= self.end)

    def __and__(self, other: "DateRange") -> "DateRange":
        # Intersection of two ranges
        return self.intersection(other)

    def keys(self) -> tuple[str, str]:
        return ("startDate", "endDate")

    def __repr__(self) -> str:
        if self:
            return f"<DateRange [{self['startDate']} .. {self['endDate']}]>"
        else:
            return "<DateRange [Empty]>"

    def __str__(self) -> str:
        if self:
            return f"{self['startDate']} .. {self['endDate']}"
        else:
            return "<DateRange [Empty]>"
