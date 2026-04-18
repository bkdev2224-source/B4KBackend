"""
Central configuration — reads from environment variables / .env file.
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent


class Settings(BaseSettings):
    # ── Database ──────────────────────────────────────────────────────────────
    db_host: str = Field("localhost", env="DB_HOST")
    db_port: int = Field(5432, env="DB_PORT")
    db_name: str = Field("kculture", env="DB_NAME")
    db_user: str = Field("postgres", env="DB_USER")
    db_password: str = Field("", env="DB_PASSWORD")

    @property
    def db_dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # ── TourAPI ───────────────────────────────────────────────────────────────
    tourapi_key: str = Field("", env="TOURAPI_KEY")
    tourapi_base_url: str = "https://apis.data.go.kr/B551011/KorService2"

    # ── MOIS (행정안전부) ──────────────────────────────────────────────────────
    mois_api_key: str = Field("", env="MOIS_API_KEY")

    # ── OpenAI ────────────────────────────────────────────────────────────────
    openai_api_key: str = Field("", env="OPENAI_API_KEY")
    openai_translation_model: str = "gpt-4.1-mini"
    openai_chat_model: str = "gpt-4.1"
    openai_embedding_model: str = "text-embedding-3-small"

    # ── Cloudinary ────────────────────────────────────────────────────────────
    cloudinary_cloud_name: str = Field("", env="CLOUDINARY_CLOUD_NAME")
    cloudinary_api_key: str = Field("", env="CLOUDINARY_API_KEY")
    cloudinary_api_secret: str = Field("", env="CLOUDINARY_API_SECRET")

    # ── Auth ──────────────────────────────────────────────────────────────────
    jwt_secret: str = Field("change-me-in-production", env="JWT_SECRET")
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24 * 7  # 1 week

    # ── Pipeline ──────────────────────────────────────────────────────────────
    translation_batch_size: int = 40_000   # OpenAI Batch API limit
    dedup_auto_merge_threshold: float = 0.92
    dedup_review_threshold: float = 0.82
    dedup_spatial_radius_m: float = 50.0

    # ── Supported languages ───────────────────────────────────────────────────
    supported_languages: list[str] = [
        "en", "ja", "zh-CN", "zh-TW", "th",
    ]

    # ── Paths ─────────────────────────────────────────────────────────────────
    domain_map_path: Path = BASE_DIR / "config" / "domain_map.json"
    region_map_path: Path = BASE_DIR / "config" / "region_map.json"

    class Config:
        env_file = str(BASE_DIR / ".env")
        env_file_encoding = "utf-8"


settings = Settings()
