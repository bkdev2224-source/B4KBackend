"""
범용 CSV 구조 분석기.

컬럼명을 하드코딩하지 않습니다.
좌표/ID 컬럼 해석은 각 adapter의 FIELD_MAP/ALIASES에서 처리합니다.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_ENCODINGS = ["utf-8-sig", "cp949", "euc-kr"]


def detect_encoding(csv_path: Path) -> str:
    """CSV 파일 인코딩 자동 감지. 실패 시 utf-8-sig 반환."""
    for enc in _ENCODINGS:
        try:
            with open(csv_path, encoding=enc) as f:
                f.read(4096)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "utf-8-sig"


class CsvSchemaInspector:
    """
    CSV 헤더·행 수·샘플 데이터 분석 후 schema_report.json 저장.

    컬럼 해석(좌표계, ID 컬럼 등)은 하지 않습니다.
    각 adapter의 FIELD_MAP/ALIASES를 통해 처리하세요.
    """

    def inspect(self, csv_path: Path, sample_rows: int = 5) -> dict[str, Any]:
        csv_path = Path(csv_path)
        logger.info("[inspect] CSV 구조 분석 시작: %s", csv_path.name)

        enc = detect_encoding(csv_path)
        df = pd.read_csv(csv_path, encoding=enc, nrows=sample_rows, dtype=str)
        original_columns = [c.strip() for c in df.columns]
        df.columns = original_columns

        with open(csv_path, encoding=enc) as f:
            row_count = sum(1 for _ in f) - 1

        report: dict[str, Any] = {
            "file":         str(csv_path),
            "encoding":     enc,
            "row_count":    row_count,
            "columns":      original_columns,
            "sample":       df.head(2).to_dict(orient="records"),
            "inspected_at": datetime.now(timezone.utc).isoformat(),
        }

        out_path = csv_path.parent / f"{csv_path.stem}_schema_report.json"
        out_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        logger.info(
            "[inspect] 완료 - 총 %d행 / 컬럼 %d개 / 인코딩: %s / report -> %s",
            row_count,
            len(original_columns),
            enc,
            out_path.name,
        )
        return report
