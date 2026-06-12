"""
Image Upload API Server
========================
A production-ready FastAPI server that receives images from remote camera clients.
Designed to run behind Nginx with Systemd on Ubuntu.

Usage (development):
    uvicorn server:app --host 0.0.0.0 --port 8000

Usage (production):
    gunicorn server:app -w 4 -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:8000
"""

import os
import shutil
import secrets
import logging
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request
from fastapi.responses import JSONResponse

# ======================== CONFIGURATION ========================

# API authentication token — override via environment variable in production
API_TOKEN = os.environ.get("CAM_API_TOKEN", "changeme-use-a-strong-token")

# Base directory where uploaded images are stored
UPLOAD_DIR = Path(os.environ.get("CAM_UPLOAD_DIR", "/home/thinhnguyenk23/poc-server/images"))

# Maximum upload size in bytes (default: 20 MB)
MAX_UPLOAD_SIZE = int(os.environ.get("CAM_MAX_UPLOAD_SIZE", 20 * 1024 * 1024))

# Allowed image extensions
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}

# ==============================================================

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cam-server")

# Ensure upload directory exists
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# FastAPI application
app = FastAPI(
    title="Camera Image Upload API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
)


# ─── Middleware: request logging ───────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = datetime.now()
    response = await call_next(request)
    elapsed = (datetime.now() - start).total_seconds()
    logger.info(
        "%s %s %s %.3fs",
        request.client.host if request.client else "-",
        request.method,
        request.url.path,
        elapsed,
    )
    return response


# ─── Auth helper ───────────────────────────────────────────────
def verify_token(x_auth: str | None = Header(None)):
    """Validate the X-Auth header against the configured API token."""
    if not x_auth or not secrets.compare_digest(x_auth, API_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ─── Routes ────────────────────────────────────────────────────

@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring and load balancers."""
    disk = shutil.disk_usage(UPLOAD_DIR)
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "disk_free_gb": round(disk.free / (1024 ** 3), 2),
    }


@app.post("/api/login")
async def login(request: Request):
    """
    Authenticate and return a token.
    Accepts JSON: {"username": "...", "password": "..."}

    In this simple implementation the username/password are checked against
    environment variables. The returned token is the same API_TOKEN.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    expected_user = os.environ.get("CAM_USERNAME", "poc")
    expected_pass = os.environ.get("CAM_PASSWORD", "cselabc5c6")

    username = body.get("username", "")
    password = body.get("password", "")

    if (
        secrets.compare_digest(username, expected_user)
        and secrets.compare_digest(password, expected_pass)
    ):
        logger.info("[LOGIN] User '%s' authenticated", username)
        return JSONResponse(content=API_TOKEN, status_code=200)

    logger.warning("[LOGIN] Failed attempt for user '%s'", username)
    raise HTTPException(status_code=403, detail="Invalid credentials")


@app.post("/api/resources/{folder}/{filename}")
async def upload_image(
    folder: str,
    filename: str,
    request: Request,
    x_auth: str | None = Header(None),
):
    """
    Receive a raw image upload (application/octet-stream).
    Compatible with the existing camera_capture_hpclab.py client.

    Path: /api/resources/<folder>/<filename>
    Headers: X-Auth: <token>
    Body: raw image bytes
    """
    # ── Auth ──
    verify_token(x_auth)

    # ── Validate filename ──
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File extension '{ext}' not allowed. Allowed: {ALLOWED_EXTENSIONS}",
        )

    # ── Sanitise folder name (prevent directory traversal) ──
    safe_folder = "".join(
        c for c in folder if c.isalnum() or c in ("-", "_")
    )
    if not safe_folder:
        safe_folder = "default"

    # ── Organise by date: <upload_dir>/<folder>/YYYY/MM/DD/ ──
    now = datetime.now()
    date_path = now.strftime("%Y/%m/%d")
    dest_dir = UPLOAD_DIR / safe_folder / date_path
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_file = dest_dir / filename

    # ── Read body with size limit ──
    body = await request.body()
    if len(body) == 0:
        raise HTTPException(status_code=400, detail="Empty body")
    if len(body) > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({len(body)} bytes). Max: {MAX_UPLOAD_SIZE}",
        )

    # ── Write to disk ──
    try:
        dest_file.write_bytes(body)
    except OSError as e:
        logger.error("[UPLOAD] Write failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save file")

    logger.info(
        "[UPLOAD] Saved %s (%d bytes) -> %s",
        filename,
        len(body),
        dest_file,
    )

    return {
        "status": "ok",
        "filename": filename,
        "folder": safe_folder,
        "size_bytes": len(body),
        "path": str(dest_file),
        "timestamp": now.isoformat(),
    }


@app.get("/api/stats")
async def stats(x_auth: str | None = Header(None)):
    """Return upload directory statistics (requires auth)."""
    verify_token(x_auth)

    total_files = 0
    total_bytes = 0
    folders = {}

    for folder_path in UPLOAD_DIR.iterdir():
        if folder_path.is_dir():
            count = sum(1 for _ in folder_path.rglob("*") if _.is_file())
            size = sum(f.stat().st_size for f in folder_path.rglob("*") if f.is_file())
            folders[folder_path.name] = {"files": count, "size_mb": round(size / (1024 ** 2), 2)}
            total_files += count
            total_bytes += size

    disk = shutil.disk_usage(UPLOAD_DIR)

    return {
        "total_files": total_files,
        "total_size_mb": round(total_bytes / (1024 ** 2), 2),
        "disk_free_gb": round(disk.free / (1024 ** 3), 2),
        "disk_total_gb": round(disk.total / (1024 ** 3), 2),
        "folders": folders,
    }
