from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse

from src.logic.kpi import kpi
from src.logic.slack.client import (
    send_weekly_slack_update,
    send_weekly_slack_update_debug,
)
from src.ui import routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    jobs = routes.get_cronjobs()

    jobs.add_jobs(
        [
            routes.Cronjob(*params)
            for params in [
                (routes.save_sparse, "0 2 * * *"),
                (routes.save_sparse, "30 2 * * *"),
                (send_weekly_slack_update, "0 5 * * MON"),
                (send_weekly_slack_update_debug, "10 5/3 * * MON-FRI"),
            ]
        ]
    )

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            tg.start_soon(jobs.start, app)

            yield

            scope.cancel()


app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)

app.include_router(routes.default_router)

app.mount("/static", StaticFiles(directory="src/static"), name="static")
