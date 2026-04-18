import json
import asyncpg
from abc import ABC, abstractmethod
from datetime import datetime, timezone


class BaseCollector(ABC):
    source_name: str = ""

    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        self._source_id: int | None = None

    async def get_source_id(self) -> int:
        if self._source_id is None:
            self._source_id = await self.conn.fetchval(
                "SELECT id FROM stage.api_sources WHERE name = $1",
                self.source_name,
            )
            if self._source_id is None:
                raise RuntimeError(f"api_sources에 '{self.source_name}' 없음. seed_data.py 실행 필요.")
        return self._source_id

    async def get_api_key(self) -> str:
        source_id = await self.get_source_id()
        key = await self.conn.fetchval(
            "SELECT key_value FROM stage.api_keys WHERE source_id = $1 AND is_active = TRUE LIMIT 1",
            source_id,
        )
        if not key:
            raise RuntimeError(f"'{self.source_name}' API 키가 등록되어 있지 않습니다.")
        return key

    async def get_config(self) -> dict:
        source_id = await self.get_source_id()
        row = await self.conn.fetchrow(
            "SELECT config FROM stage.api_sources WHERE id = $1", source_id
        )
        return json.loads(row["config"]) if row else {}

    async def start_sync_run(self, run_type: str, language_code: str) -> int:
        source_id = await self.get_source_id()
        run_id = await self.conn.fetchval(
            """
            INSERT INTO stage.sync_runs (source_id, run_type, language_code)
            VALUES ($1, $2, $3) RETURNING id
            """,
            source_id, run_type, language_code,
        )
        return run_id

    async def finish_sync_run(self, run_id: int, status: str, records: int, error: str | None = None) -> None:
        await self.conn.execute(
            """
            UPDATE stage.sync_runs
            SET status = $1, records_collected = $2, finished_at = $3, error_message = $4
            WHERE id = $5
            """,
            status, records, datetime.now(timezone.utc), error, run_id,
        )

    async def save_raw_document(
        self,
        external_id: str,
        language_code: str,
        raw_json: dict,
        sync_run_id: int,
    ) -> None:
        source_id = await self.get_source_id()
        await self.conn.execute(
            """
            INSERT INTO stage.raw_documents
                (source_id, external_id, language_code, raw_json, sync_run_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_id, external_id, language_code)
            DO UPDATE SET raw_json = EXCLUDED.raw_json,
                          sync_run_id = EXCLUDED.sync_run_id,
                          collected_at = NOW()
            """,
            source_id, external_id, language_code, json.dumps(raw_json), sync_run_id,
        )

    async def save_checkpoint(self, language_code: str, page: int, total: int, status: str = "running") -> None:
        source_id = await self.get_source_id()
        await self.conn.execute(
            """
            INSERT INTO stage.source_sync_state
                (source_id, language_code, last_page, total_count, status, last_synced_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (source_id, language_code)
            DO UPDATE SET last_page = EXCLUDED.last_page,
                          total_count = EXCLUDED.total_count,
                          status = EXCLUDED.status,
                          last_synced_at = NOW()
            """,
            source_id, language_code, page, total, status,
        )

    async def get_checkpoint(self, language_code: str) -> dict:
        source_id = await self.get_source_id()
        row = await self.conn.fetchrow(
            "SELECT last_page, total_count, status FROM stage.source_sync_state WHERE source_id = $1 AND language_code = $2",
            source_id, language_code,
        )
        return dict(row) if row else {"last_page": 0, "total_count": None, "status": "idle"}

    @abstractmethod
    async def full_load(self, language_code: str) -> int: ...

    @abstractmethod
    async def fetch_updated(self, language_code: str) -> int: ...
