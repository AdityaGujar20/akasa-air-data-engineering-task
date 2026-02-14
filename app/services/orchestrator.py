from loguru import logger
from app.services.cleaning_pipeline import run_cleaning_pipeline
from app.services.db_pipeline import run_db_pipeline
from app.services.kpis.orm_kpis import compute_kpis_orm
from app.services.kpis.base_kpis import bundle_to_dict
# (If you keep pandas/spark paths, wire them here similarly.)

def run_orchestrator(mode: str = "db"):
    """
    Mini-DAG style orchestration:
      1) Clean raw -> data/cleaned/*.csv
      2) Run chosen pipeline (db / pandas / spark). Here we focus on db.
      3) Compute KPIs via ORM (for db mode).
    """
    logger.info(f"🧭 Orchestrator start, mode={mode}")
    clean_result = run_cleaning_pipeline()

    if mode == "db":
        db_result = run_db_pipeline()
        kpis = compute_kpis_orm()
        out = {"clean": clean_result, "pipeline": db_result, "kpis": bundle_to_dict(kpis)}
    else:
        # stub paths if needed later
        out = {"clean": clean_result, "pipeline": {"message": f"{mode} path not implemented"}, "kpis": None}

    logger.success("🧭 Orchestrator finished.")
    return out
