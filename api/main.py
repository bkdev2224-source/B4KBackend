"""
Step 1-8  FastAPI 서비스 엔트리포인트
  uvicorn api.main:app --reload
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.places import router as places_router
from api.routes.users import auth_router, bookmark_router, review_router

app = FastAPI(
    title="K-Culture Platform API",
    version="1.0.0",
    description="K-Culture 장소 정보 · 다국어 번역",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(places_router)
app.include_router(auth_router)
app.include_router(bookmark_router)
app.include_router(review_router)


@app.get("/health")
def health():
    return {"status": "ok"}
