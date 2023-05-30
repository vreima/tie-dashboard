import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
import param
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from loguru import logger
from tornado.ioloop import IOLoop

from src.daterange import DateRange
from src.severa.fetch import Fetcher


class SineWave(param.Parameterized):
    offset = param.Number(default=0.0, bounds=(-5.0, 5.0))
    amplitude = param.Number(default=1.0, bounds=(-5.0, 5.0))
    phase = param.Number(default=0.0, bounds=(0.0, 2 * np.pi))
    frequency = param.Number(default=1.0, bounds=(0.1, 5.1))
    N = param.Integer(default=30, bounds=(0, None))
    # x_range = param.Range(default=(0, 30), bounds=(0, 1000))
    x_range = param.CalendarDateRange(
        (
            datetime.date.fromisoformat("2023-01-01"),
            datetime.date.fromisoformat("2023-12-31"),
        )
    )
    y_range = param.Range(default=(-1, 30), bounds=(-10, 40))

    def __init__(self, **params):
        super(SineWave, self).__init__(**params)
        self._data = None
        x, y = self.sine()
        self.cds = ColumnDataSource(data=dict(x=x, y=y))

        loop = IOLoop.current()
        loop.add_callback(self.fetch_data)

        self.plot = figure(
            height=400,
            width=400,
            tools="crosshair, pan, reset, save, wheel_zoom",
            x_axis_type="datetime",
            # x_range=self.x_range,
            # y_range=self.y_range,
        )
        self.plot.line("x", "y", source=self.cds, line_width=3, line_alpha=0.6)

    @param.depends(
        "N",
        "frequency",
        "amplitude",
        "offset",
        "phase",
        "x_range",
        "y_range",
        watch=True,
    )
    def update_plot(self):
        x, y = self.sine()
        self.cds.data = dict(x=x, y=y)
        # self.plot.x_range.start, self.plot.x_range.end = self.x_range
        # self.plot.y_range.start, self.plot.y_range.end = self.y_range

    async def fetch_data(self):
        async with Fetcher() as fetcher:
            self._data = await fetcher.get_resource_allocations(DateRange(540))

        logger.debug("fetch ready, updating")

    def sine(self):
        if self._data is None:
            x = pd.date_range("2023-01-01", "2023-12-31", freq="D").to_pydatetime()
            y = np.arange(len(x))

        else:
            delta = timedelta(days=self.N)
            grouped = (
                self._data[
                    self._data["forecast-date"].between(
                        self._data["date"], self._data["date"] + delta
                    )
                ]
                .groupby(
                    [
                        "forecast-date",
                    ]
                )["value"]
                .sum()
                .reset_index()
            )
            x = grouped["forecast-date"]
            y = grouped["value"]
            print(f"x: {x.min()}, {x.max()}")
            print(f"y: {y.min()}, {y.max()}")
            # x = np.arange(len(y))
            print(f"len: {len(x)}")
        return x, y


class Allocations(param.Parameterized):
    N = param.Integer(default=30, bounds=(0, 540))
    x_range = param.Range(default=(0, 4 * np.pi), bounds=(0, 4 * np.pi))
    y_range = param.CalendarDateRange(
        (
            datetime.date.fromisoformat("2023-01-01"),
            datetime.date.fromisoformat("2023-12-31"),
        )
    )

    def __init__(self, **params):
        super(Allocations, self).__init__(**params)
        self._data = None
        loop = IOLoop.current()
        loop.add_callback(self.fetch_data)
        # self._data = run(self.fetch_data)
        x = pd.date_range(
            "2023-05-01",
            "2023-05-11",
            freq="D",
        )
        self.cds = ColumnDataSource(data=dict(x=x, y=list(range(10))))

        self.plot = figure(
            height=400,
            width=800,
            tools="crosshair, pan, reset, save, wheel_zoom",
        )
        self.plot.line("x", "y", source=self.cds, line_width=3, line_alpha=0.6)

    async def fetch_data(self):
        async with Fetcher() as fetcher:
            self._data = await fetcher.get_resource_allocations(DateRange(540))

        logger.debug("fetch ready, updating")
        x, y = self.data()
        self.cds = ColumnDataSource(data=dict(x=x, y=y))

    @param.depends("N", "x_range", "y_range", watch=True)
    def update_plot(self):
        x, y = self.data()
        self.cds.data = dict(x=x, y=y)
        self.plot.x_range.start, self.plot.x_range.end = self.x_range
        self.plot.y_range.start, self.plot.y_range.end = self.y_range

    def data(self):
        if self._data is None:
            logger.debug("data is None")
            return list(range(10)), list(range(10))

        logger.debug("data is not None")

        delta = timedelta(days=self.N)
        grouped = (
            self._data[
                self._data["forecast-date"].between(
                    self._data["date"], self._data["date"] + delta
                )
            ]
            .groupby(
                [
                    "forecast-date",
                ]
            )["value"]
            .sum()
            .reset_index()
        )
        grouped["forecast-date"]
        grouped["value"]
        return list(range(10)), list(range(20, 0, -2))
        # return x.tolist(), y.tolist()
