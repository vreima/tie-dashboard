from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.logic.kpi import kpi
from src.logic.pressure import pressure
from src.ui import routes

app = FastAPI()


app.include_router(kpi.router)
app.include_router(routes.default_router)
app.include_router(pressure.router)


app.mount("/static", StaticFiles(directory="src/static"), name="static")
