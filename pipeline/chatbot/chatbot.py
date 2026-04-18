"""
Step 1-10  AI 챗봇 (GPT-4.1)
  - pgvector 의미 검색 (text-embedding-3-small)
  - GPT-4.1 Function Calling 멀티턴 대화
  - 일정 추천 (itineraries)
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from openai import OpenAI

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)
client = OpenAI(api_key=settings.openai_api_key)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_places",
            "description": "K-culture 장소를 자연어로 검색. BTS 관련 장소, 한식당, 뷰티숍 등",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "검색 질의"},
                    "domain": {
                        "type": "string",
                        "enum": ["kfood", "kbeauty", "ktourism", "kshopping", "kleisure"],
                        "description": "카테고리 필터 (생략 가능)",
                    },
                    "region": {"type": "string", "description": "지역 필터 (예: 서울, 부산)"},
                    "top_k": {"type": "integer", "default": 5},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_itinerary",
            "description": "방문할 장소 목록으로 여행 일정 생성",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "travel_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "region": {"type": "string"},
                    "place_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "방문할 place_id 목록",
                    },
                },
                "required": ["title", "place_ids"],
            },
        },
    },
]

SYSTEM_PROMPT = (
    "당신은 K-culture 여행 전문 AI 어시스턴트입니다. "
    "한국의 음식, 뷰티, 관광 명소를 추천하고 맞춤 일정을 제안합니다. "
    "사용자의 언어에 맞춰 답변하세요. "
    "장소를 추천할 때는 search_places 함수를 반드시 사용하세요."
)


class KCultureChatbot:
    """
    Usage:
        bot = KCultureChatbot()
        session_id = bot.new_session(lang='ko')
        response = bot.chat(session_id, "BTS 멤버들이 자주 가는 식당 알려줘")
    """

    def new_session(self, user_id: int | None = None, lang: str = "ko") -> str:
        session_id = str(uuid.uuid4())
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO ai.chat_sessions (session_id, user_id, lang) VALUES (%s, %s, %s)",
                (session_id, user_id, lang),
            )
        return session_id

    def chat(self, session_id: str, user_message: str) -> str:
        history = self._load_history(session_id)
        history.append({"role": "user", "content": user_message})
        self._save_message(session_id, "user", user_message)

        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history

        # GPT-4.1 호출
        response = client.chat.completions.create(
            model=settings.openai_chat_model,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
        )

        msg = response.choices[0].message

        # Function Calling 처리
        while msg.tool_calls:
            messages.append(msg)
            for tool_call in msg.tool_calls:
                fn_name = tool_call.function.name
                fn_args = json.loads(tool_call.function.arguments)
                fn_result = self._dispatch(fn_name, fn_args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(fn_result, ensure_ascii=False),
                })

            response = client.chat.completions.create(
                model=settings.openai_chat_model,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
            )
            msg = response.choices[0].message

        reply = msg.content or ""
        self._save_message(session_id, "assistant", reply)
        return reply

    # ── Function Dispatch ─────────────────────────────────────────────────────

    def _dispatch(self, fn_name: str, args: dict) -> Any:
        if fn_name == "search_places":
            return self._search_places(**args)
        if fn_name == "create_itinerary":
            return self._create_itinerary(**args)
        return {"error": f"unknown function: {fn_name}"}

    def _search_places(
        self,
        query: str,
        domain: str | None = None,
        region: str | None = None,
        top_k: int = 5,
    ) -> list[dict]:
        embedding = self._embed(query)
        with get_conn() as conn:
            cur = conn.cursor()
            where_clauses = ["si.embedding IS NOT NULL", "p.is_publishable = TRUE"]
            params: list[Any] = [embedding, top_k]

            if domain:
                where_clauses.append("p.display_domain = %s")
                params.insert(-1, domain)
            if region:
                where_clauses.append("p.display_region = %s")
                params.insert(-1, region)

            where_sql = " AND ".join(where_clauses)
            cur.execute(
                f"""
                SELECT p.place_id, p.name, p.address, p.display_domain, p.display_region,
                       si.embedding <=> %s::vector AS distance
                  FROM core.places p
                  JOIN service.search_index si ON si.place_id = p.place_id
                 WHERE {where_sql}
                 ORDER BY distance
                 LIMIT %s
                """,
                [embedding] + params[1:],
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    def _create_itinerary(
        self,
        title: str,
        place_ids: list[int],
        travel_date: str | None = None,
        region: str | None = None,
    ) -> dict:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO ai.itineraries (title, travel_date, region)
                VALUES (%s, %s, %s) RETURNING itinerary_id
                """,
                (title, travel_date, region),
            )
            itinerary_id = cur.fetchone()["itinerary_id"]

            for order, pid in enumerate(place_ids, 1):
                cur.execute(
                    """
                    INSERT INTO ai.itinerary_items (itinerary_id, place_id, visit_order)
                    VALUES (%s, %s, %s)
                    """,
                    (itinerary_id, pid, order),
                )
        return {"itinerary_id": itinerary_id, "place_count": len(place_ids)}

    # ── 벡터 검색 인덱스 구축 ─────────────────────────────────────────────────

    def index_places(self, batch_size: int = 100) -> int:
        """core.places 전체를 임베딩해 search_index에 저장."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT p.place_id, p.name, p.description
                  FROM core.places p
                 WHERE p.is_publishable = TRUE
                   AND NOT EXISTS (
                       SELECT 1 FROM service.search_index si WHERE si.place_id = p.place_id
                   )
                 LIMIT %s
                """,
                (batch_size,),
            )
            rows = cur.fetchall()

        indexed = 0
        for row in rows:
            text = f"{row['name']} {row['description'] or ''}".strip()
            embedding = self._embed(text)
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO service.search_index (place_id, embedding)
                    VALUES (%s, %s::vector)
                    ON CONFLICT (place_id) DO UPDATE SET embedding = EXCLUDED.embedding, indexed_at = now()
                    """,
                    (row["place_id"], embedding),
                )
            indexed += 1

        return indexed

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _embed(self, text: str) -> list[float]:
        resp = client.embeddings.create(
            model=settings.openai_embedding_model,
            input=text[:8000],
        )
        return resp.data[0].embedding

    def _load_history(self, session_id: str) -> list[dict]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT role, content FROM ai.chat_messages WHERE session_id = %s ORDER BY created_at LIMIT 40",
                (session_id,),
            )
            return [{"role": r["role"], "content": r["content"]} for r in cur.fetchall()]

    def _save_message(self, session_id: str, role: str, content: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO ai.chat_messages (session_id, role, content) VALUES (%s, %s, %s)",
                (session_id, role, content),
            )
            cur.execute(
                "UPDATE ai.chat_sessions SET last_active = now() WHERE session_id = %s",
                (session_id,),
            )
