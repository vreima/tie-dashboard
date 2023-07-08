from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.logic.kpi import kpi
from src.logic.slack.client import send_weekly_slack_update, send_weekly_slack_update_debug
from src.ui import routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    jobs = routes.get_cronjobs()

    jobs.add_jobs(
        [
            routes.Cronjob(*params)
            for params in [
                (routes.save_sparse, "0 2 * * *"),
                (send_weekly_slack_update, "0 8 * * *"),
                (send_weekly_slack_update_debug, "0 * * * *"),
            ]
        ]
    )

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            tg.start_soon(jobs.start, app)

            yield

            scope.cancel()


app = FastAPI(lifespan=lifespan)


app.include_router(kpi.router)
app.include_router(routes.default_router)


app.mount("/static", StaticFiles(directory="src/static"), name="static")
