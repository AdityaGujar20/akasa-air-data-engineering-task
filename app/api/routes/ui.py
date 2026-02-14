from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

@router.get("/")
def root_redirect():
    return RedirectResponse(url="/ui/upload", status_code=307)

@router.get("/ui/upload")
def upload_page(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})
