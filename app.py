from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import os, pathlib, subprocess

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="DataRes Scraper API")

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/run")
def run_all():
    try:
        subprocess.check_call(["python", "run_all.py"])
        return {"status": "success"}
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Scrape failed: {e}")

@app.get("/list")
def list_files():
    files = sorted([p.name for p in DATA_DIR.rglob("*") if p.is_file()])
    return {"count": len(files), "files": files}

@app.get("/files/{name:path}")
def get_file(name: str):
    p = DATA_DIR / name
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(p), filename=p.name)
