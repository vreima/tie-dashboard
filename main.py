import random
import string
import sys
import time
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

import src.config  # noqa: F401
from src.logic.slack.client import (
    send_weekly_slack_update,
    send_weekly_slack_update_debug,
)
from src.logic.slack.logger import slack_logger
from src.ui import routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    jobs = routes.get_cronjobs()

    jobs.add_jobs(
        [
            routes.Cronjob(*params)
            for params in [
                (routes.save_sparse, "0 2 * * *", "database-save"),
                (send_weekly_slack_update, "0 5 * * MON", "weekly-slack-msg"),
                # (
                #     send_weekly_slack_update_debug,
                #     "20 4 * * MON-FRI",
                #     "debug-slack-msg",
                # ),
            ]
        ]
    )

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            tg.start_soon(jobs.start, app)

            yield

            scope.cancel()


logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> [{extra[source]}] - <level>{message}</level>",
    level="TRACE",
)
logger.add(slack_logger, level="ERROR")
logger.configure(extra={"source": "root"})


app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)


# a
@app.middleware("http")
async def log_requests(request: Request, call_next):
    idem = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))

    start_time = time.monotonic()

    with logger.contextualize(source=idem):
        logger.info(
            f"Incoming request {request.method} {request.url.path} (query: {request.query_params}) from {request.client.host}:{request.client.port}."
        )
        response = await call_next(request)
        logger.info(
            f"Request {request.method} {request.url.path}: {response.status_code} in {time.monotonic() - start_time:.2f}s."
        )

    return response


app.include_router(routes.default_router)
app.mount("/static", StaticFiles(directory="src/static"), name="static")
