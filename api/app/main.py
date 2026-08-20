from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from app.routes.jobs import router as jobs_router

app = FastAPI(
    title="forge Job Submission API",
    description="Distributed Task Queue Job Submission Service",
    version="0.1.0",
)

app.include_router(jobs_router)


@app.get("/healthz", tags=["Health"])
def health_check():
    return {"status": "ok", "service": "forge-api"}


@app.get("/metrics", tags=["Observability"])
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
