"""
번역 파이프라인 오케스트레이터
  ① 주소 (한→영): 주소정보누리집 API  → place_translations.address (lang='en')
  ② zh-CN/zh-TW : DeepSeek           → place_translations (name, description)
  ③ en/ja/th    : Gemini             → place_translations (name, description)
  ④ 스냅샷 갱신  : DB 트리거가 자동 처리

  도로명 주소는 ko·en만 보유. ja/zh-CN/zh-TW/th는 address 번역하지 않음.
"""
from __future__ import annotations

import logging

from pipeline.translator.deepseek_translator import DeepSeekTranslator
from pipeline.translator.gemini_translator import GeminiBatchTranslator
from pipeline.translator.juso_translator import JusoAddressTranslator

logger = logging.getLogger(__name__)


class TranslationOrchestrator:
    """
    Usage:
        result = TranslationOrchestrator().run()
        # result = {"address_en": 120, "deepseek": 340, "gemini": 510}
    """

    def run(self) -> dict[str, int]:
        logger.info("=== 번역 파이프라인 시작 ===")

        addr_count    = JusoAddressTranslator().run()
        deepseek_count = DeepSeekTranslator().run()
        gemini_count  = GeminiBatchTranslator().run()

        result = {
            "address_en": addr_count,
            "deepseek":   deepseek_count,
            "gemini":     gemini_count,
        }
        logger.info("=== 번역 파이프라인 완료: %s ===", result)
        return result


# 하위 호환 alias — 기존 스크립트에서 BatchTranslator를 import하는 경우 대비
BatchTranslator = TranslationOrchestrator
