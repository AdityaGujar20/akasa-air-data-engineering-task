# app/api/routes/upload.py

import os
from fastapi import APIRouter, UploadFile, File
from loguru import logger

router = APIRouter()

UPLOAD_DIR = "data/upload"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload")
async def upload_api(files: list[UploadFile] = File(...)):
    """
    API endpoint to upload files.
    UI upload is handled in ui.py (GET /ui/upload page).
    """
    saved = []

    for f in files:
        file_path = os.path.join(UPLOAD_DIR, f.filename)
        with open(file_path, "wb") as out:
            out.write(await f.read())
        saved.append(f.filename)

    logger.info(f"✅ Uploaded files: {saved}")
    return {"status": "ok", "saved": saved}
