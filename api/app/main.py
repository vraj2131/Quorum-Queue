import os
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from app.routes.jobs import router as jobs_router

app = FastAPI(
    title="forge Job Submission API",
    description="Distributed Task Queue Job Submission Service",
    version="0.1.0",
)

app.include_router(jobs_router)

STATIC_INDEX_PATH = os.path.join(os.path.dirname(__file__), "static", "index.html")


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def read_root():
    if os.path.exists(STATIC_INDEX_PATH):
        with open(STATIC_INDEX_PATH, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>forge Control Plane</h1><p>API is running. Visit <a href='/docs'>/docs</a></p>"


@app.get("/healthz", tags=["Health"])
def health_check():
    return {"status": "ok", "service": "forge-api"}


@app.get("/metrics", tags=["Observability"])
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
