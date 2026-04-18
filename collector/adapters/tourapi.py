import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import asyncpg
from collector.base import BaseCollector

ROWS_PER_PAGE = 1000
CONTENT_TYPE_IDS = ["12", "14", "15", "25", "28", "32", "38", "39"]
# 12:관광지 14:문화시설 15:축제/공연 25:여행코스 28:레포츠 32:숙박 38:쇼핑 39:음식점


class TourAPICollector(BaseCollector):
    source_name = "tourapi"

    def __init__(self, conn: asyncpg.Connection):
        super().__init__(conn)
        self._api_key: str | None = None
        self._config: dict | None = None

    async def _init(self) -> None:
        if self._api_key is None:
            self._api_key = await self.get_api_key()
        if self._config is None:
            self._config = await self.get_config()

    def _base_url(self, language_code: str) -> str:
        urls = self._config.get("language_urls", {})
        return urls.get(language_code, urls.get("ko", ""))

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        content_type_id: str,
        page: int,
    ) -> dict:
        url = f"{base_url}/areaBasedList2"
        params = {
            "serviceKey": self._api_key,
            "numOfRows": ROWS_PER_PAGE,
            "pageNo": page,
            "MobileOS": "ETC",
            "MobileApp": "B4KDataBase",
            "_type": "json",
            "arrange": "A",
            "contentTypeId": content_type_id,
        }
        r = await client.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        header = data["response"]["header"]
        if header["resultCode"] != "0000":
            raise RuntimeError(f"TourAPI 오류: {header['resultCode']} - {header['resultMsg']}")

        return data["response"]["body"]

    async def _collect_language(
        self,
        language_code: str,
        run_type: str,
        resume: bool = True,
    ) -> int:
        await self._init()
        base_url = self._base_url(language_code)

        checkpoint = await self.get_checkpoint(language_code)
        start_page = (checkpoint["last_page"] + 1) if (resume and checkpoint["status"] == "running") else 1

        run_id = await self.start_sync_run(run_type, language_code)
        total_collected = 0

        try:
            async with httpx.AsyncClient() as client:
                for content_type_id in CONTENT_TYPE_IDS:
                    page = start_page
                    while True:
                        body = await self._fetch_page(client, base_url, content_type_id, page)
                        total_count = body.get("totalCount", 0)
                        items = body.get("items")

                        if not items or not items.get("item"):
                            break

                        raw_items = items["item"]
                        if isinstance(raw_items, dict):
                            raw_items = [raw_items]

                        for item in raw_items:
                            await self.save_raw_document(
                                external_id=str(item["contentid"]),
                                language_code=language_code,
                                raw_json=item,
                                sync_run_id=run_id,
                            )
                            total_collected += 1

                        await self.save_checkpoint(language_code, page, total_count, "running")
                        print(f"  [{language_code}] type={content_type_id} page={page}/{-(-total_count // ROWS_PER_PAGE)} collected={total_collected}")

                        if page * ROWS_PER_PAGE >= total_count:
                            break
                        page += 1
                        await asyncio.sleep(0.1)  # rate limit

            await self.save_checkpoint(language_code, 0, 0, "done")
            await self.finish_sync_run(run_id, "done", total_collected)

        except Exception as e:
            await self.finish_sync_run(run_id, "failed", total_collected, str(e))
            raise

        return total_collected

    async def full_load(self, language_code: str) -> int:
        print(f"[TourAPI] full_load 시작: {language_code}")
        return await self._collect_language(language_code, "full_load", resume=False)

    async def fetch_updated(self, language_code: str) -> int:
        print(f"[TourAPI] fetch_updated 시작: {language_code}")
        return await self._collect_language(language_code, "fetch_updated", resume=True)
