from fastapi import FastAPI
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
