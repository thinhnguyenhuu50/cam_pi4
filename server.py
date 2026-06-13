"""
Minimal Image Upload API
=========================
Receives images from camera clients and saves them to disk.

Run:
    uvicorn server:app --host 0.0.0.0 --port 8000
"""

from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request

# ======================== CONFIGURATION ========================

# Directory where uploaded images are stored
UPLOAD_DIR = Path("./images")

# Maximum upload size in bytes (20 MB)
MAX_UPLOAD_SIZE = 20 * 1024 * 1024

# Allowed image extensions
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}

# ==============================================================

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Camera Image Upload API")


@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.post("/api/login")
async def login():
    """Returns a dummy token — auth is disabled for local testing."""
    return "ok"


@app.post("/api/resources/{folder}/{filename}")
async def upload_image(folder: str, filename: str, request: Request):
    """
    Receive a raw image upload (application/octet-stream).
    """
    # Validate extension
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Extension '{ext}' not allowed")

    # Sanitise folder name
    safe_folder = "".join(c for c in folder if c.isalnum() or c in ("-", "_")) or "default"

    # Organise by date: images/<folder>/YYYY-MM-DD/
    date_dir = datetime.now().strftime("%Y-%m-%d")
    dest_dir = UPLOAD_DIR / safe_folder / date_dir
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Read body
    body = await request.body()
    if len(body) == 0:
        raise HTTPException(status_code=400, detail="Empty body")
    if len(body) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail="File too large")

    # Save
    dest = dest_dir / filename
    dest.write_bytes(body)

    print(f"[SAVED] {dest}  ({len(body)} bytes)")

    return {"status": "ok", "filename": filename, "size": len(body)}
