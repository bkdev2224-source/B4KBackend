from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # DB
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "postgres"
    db_user: str = "postgres"
    db_password: str = ""
    database_url: str = ""

    # API Keys
    tour_api_key: str = ""
    deepl_api_key: str = ""
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    # Cloudinary
    cloudinary_url: str = ""


settings = Settings()
