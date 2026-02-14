from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ✅ Routers
from app.api.routes.upload import router as upload_router
from app.api.routes.run_pipeline import router as run_router
from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.ui import router as ui_router

app = FastAPI(title="Akasa Air Data Platform", version="1.0")

# Static assets
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Templates
templates = Jinja2Templates(directory="app/templates")

# ✅ API Routers
app.include_router(upload_router, prefix="/api", tags=["upload"])
app.include_router(run_router, prefix="/api", tags=["pipeline"])

# ✅ Dashboard
app.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])

# ✅ UI Router
app.include_router(ui_router, tags=["ui"])
