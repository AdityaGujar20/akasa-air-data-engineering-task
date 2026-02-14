from fastapi import APIRouter, Request, Query
from typing import Literal
from fastapi.templating import Jinja2Templates

from app.services.orchestrator import run_orchestrator

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/")
def dashboard(
    request: Request,
    engine: Literal["db", "pandas", "spark"] = Query("db")
):
    """
    Dashboard now runs the orchestrator before rendering KPIs.
    """

    result = run_orchestrator(mode=engine)
    kpis = result["kpis"]

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "engine": engine,
            "kpis": kpis
        }
    )
