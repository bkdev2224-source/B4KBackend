"""
Step 1-9  User API — JWT 인증, 북마크, 리뷰
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr

from config.settings import settings
from database.db import get_conn

router = APIRouter(tags=["users"])
pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2 = OAuth2PasswordBearer(tokenUrl="/auth/token")


# ── JWT ───────────────────────────────────────────────────────────────────────

def _create_token(user_id: int) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_expire_minutes)
    return jwt.encode(
        {"sub": str(user_id), "exp": expire},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )


def _current_user_id(token: str = Depends(oauth2)) -> int:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        return int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# ── 스키마 ────────────────────────────────────────────────────────────────────

class UserRegister(BaseModel):
    email: EmailStr
    password: str
    name: str | None = None
    preferred_lang: str = "ko"


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class ReviewCreate(BaseModel):
    rating: int
    content: str | None = None
    lang: str = "ko"


# ── Auth 엔드포인트 ───────────────────────────────────────────────────────────

auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.post("/register", response_model=Token)
def register(body: UserRegister):
    hashed = pwd_ctx.hash(body.password)
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO "user".users (email, password_hash, name, preferred_lang)
                VALUES (%s, %s, %s, %s) RETURNING user_id
                """,
                (body.email, hashed, body.name, body.preferred_lang),
            )
        except Exception:
            raise HTTPException(status_code=400, detail="Email already exists")
        user_id = cur.fetchone()["user_id"]
    return Token(access_token=_create_token(user_id))


@auth_router.post("/token", response_model=Token)
def login(form: OAuth2PasswordRequestForm = Depends()):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT user_id, password_hash FROM "user".users WHERE email = %s AND is_active',
            (form.username,),
        )
        row = cur.fetchone()

    if not row or not pwd_ctx.verify(form.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return Token(access_token=_create_token(row["user_id"]))


# ── Bookmark 엔드포인트 ───────────────────────────────────────────────────────

bookmark_router = APIRouter(prefix="/bookmarks", tags=["bookmarks"])


@bookmark_router.post("/{place_id}", status_code=201)
def add_bookmark(place_id: int, user_id: int = Depends(_current_user_id)):
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                'INSERT INTO "user".bookmarks (user_id, place_id) VALUES (%s, %s)',
                (user_id, place_id),
            )
        except Exception:
            raise HTTPException(status_code=409, detail="Already bookmarked")
    return {"status": "ok"}


@bookmark_router.delete("/{place_id}")
def remove_bookmark(place_id: int, user_id: int = Depends(_current_user_id)):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            'DELETE FROM "user".bookmarks WHERE user_id = %s AND place_id = %s',
            (user_id, place_id),
        )
    return {"status": "ok"}


@bookmark_router.get("")
def list_bookmarks(user_id: int = Depends(_current_user_id), lang: str = "ko"):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.place_id, s.name_ko, s.name_en, s.display_domain, s.display_region,
                   s.primary_image_url, b.created_at
              FROM "user".bookmarks b
              JOIN service.places_snapshot s ON s.place_id = b.place_id
             WHERE b.user_id = %s
             ORDER BY b.created_at DESC
            """,
            (user_id,),
        )
        return list(cur.fetchall())


# ── Review 엔드포인트 ─────────────────────────────────────────────────────────

review_router = APIRouter(prefix="/reviews", tags=["reviews"])


@review_router.post("/{place_id}", status_code=201)
def write_review(place_id: int, body: ReviewCreate, user_id: int = Depends(_current_user_id)):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO "user".reviews (user_id, place_id, rating, content, lang)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
            """,
            (user_id, place_id, body.rating, body.content, body.lang),
        )
        review_id = cur.fetchone()["id"]
    return {"review_id": review_id}


@review_router.get("/{place_id}")
def get_reviews(place_id: int, page: int = 1, size: int = 20):
    offset = (page - 1) * size
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT r.id, r.rating, r.content, r.lang, r.created_at,
                   u.name AS user_name
              FROM "user".reviews r
              JOIN "user".users u ON u.user_id = r.user_id
             WHERE r.place_id = %s
             ORDER BY r.created_at DESC
             LIMIT %s OFFSET %s
            """,
            (place_id, size, offset),
        )
        return list(cur.fetchall())
