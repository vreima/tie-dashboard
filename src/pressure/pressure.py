import arrow
import pandas as pd
from fastapi import APIRouter, Request, HTTPException
from fastapi.templating import Jinja2Templates
from loguru import logger

from src.database import Base

router = APIRouter()

templates = Jinja2Templates(directory="src/static")


@router.get("/pressure")
async def pressure(
    request: Request,
    startDate: str = "",
    endDate: str = "",
    users: str = "",
    businessunits: str = "",
):  # noqa: ARG001
    if not startDate:
        start = arrow.utcnow().shift(days=-7).floor("day")
    else:
        try:
            start = arrow.get(startDate).floor("day")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Query parameter 'startDate' has invalid date format, expected YYYY-mm-dd",
            )

    if not endDate:
        end = arrow.utcnow().ceil("day")
    else:
        try:
            end = arrow.get(endDate).ceil("day")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Query parameter 'endDate' has invalid date format, expected YYYY-mm-dd",
            )

    filter = {"date": {"$gte": start.datetime, "$lte": end.datetime}}

    if users:
        filter["user"] = {"$in": users.split(",")}

    # TODO: businessunits

    results = Base("pressure", "pressure").find(filter).to_dict(orient="records")
    return results


@router.get("/kiire/")
async def pressure_dashboard(
    request: Request,
):
    return templates.TemplateResponse(
        "pressure_dashboard.html",
        {
            "request": request,
            "base_url": request.base_url,
            "hostname": request.base_url.hostname,
        },
    )


@router.get("/kiire/{user_name}")
async def user_pressure(request: Request, user_name: str):
    return templates.TemplateResponse(
        "pressure.html",
        {
            "request": request,
            "user_name": user_name,
            "base_url": request.base_url,
            "hostname": request.base_url.hostname,
        },
    )


@router.get("/pressure/save/{user_name}")
async def save_user_pressure(
    request: Request,  # noqa: ARG001
    user_name: str,
    x: float | None = None,
    y: float | None = None,
):
    Base("pressure", "pressure").upsert(
        pd.DataFrame(
            [
                {
                    "user": user_name,
                    "date": pd.Timestamp(arrow.utcnow().datetime),
                    "x": x,
                    "y": y,
                }
            ]
        )
    )
    return "OK"


@router.get("/pressure/load/{user_name}")
async def load_user_pressure(request: Request, user_name: str):  # noqa: ARG001
    results = (
        Base("pressure", "pressure").find({"user": user_name}).to_dict(orient="records")
    )
    return results
