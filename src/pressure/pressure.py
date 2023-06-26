
import arrow
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from src.database import Base
from loguru import logger

router = APIRouter()

templates = Jinja2Templates(directory="src/static")


@router.get("/pressure")
async def pressure(request: Request, offset: int = 7):  # noqa: ARG001
    results = (
        Base("pressure", "pressure")
        .find(
            {"date": {"$gte": arrow.utcnow().shift(days=-offset).floor("day").datetime}}
        )
        .to_dict(orient="records")
    )
    return results


@router.get("/kiire/")
async def pressure_dashboard(
    request: Request,
):
    return templates.TemplateResponse(
        "pressure_dashboard.html",
        {"request": request, "base_url": request.base_url, "hostname": request.base_url.hostname},
    )


@router.get("/kiire/{user_name}")
async def user_pressure(request: Request, user_name: str):
    return templates.TemplateResponse(
        "pressure.html",
        {"request": request, "user_name": user_name, "base_url": request.base_url, "hostname": request.base_url.hostname},
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
