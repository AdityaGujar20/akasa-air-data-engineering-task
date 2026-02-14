# app/api/routes/run_pipeline.py

from fastapi import APIRouter, Form, HTTPException
from typing import Literal
from loguru import logger
from app.services.orchestrator import run_orchestrator

router = APIRouter()


@router.post("/run")
def run_pipeline(mode: Literal["db", "pandas", "spark"] = Form(...)):
    """
    Trigger the complete pipeline:
    - cleaning
    - table-based or in-memory approach
    - kpi computation
    """
    try:
        result = run_orchestrator(mode)
        return {
            "status": "success",
            "mode": mode,
            "result": result
        }
    except Exception as e:
        logger.exception(f"Pipeline run failed: {e}")
        # Surface a clear message (e.g., DB connection issues)
        raise HTTPException(status_code=500, detail=str(e))
