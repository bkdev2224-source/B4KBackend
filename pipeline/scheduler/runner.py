"""
APScheduler 기반 파이프라인 스케줄러
  - 관광공사 Sync: 매주 월요일 02:00 KST
  - 번역 Batch 제출: 매일 01:00
  - 번역 결과 수거: 매시간
  - 이미지 업로드: 매일 03:00
  - Domain/Region 매핑: Sync 이후 자동
"""
from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from adapters.tourapi.sync_checker import TourApiSyncChecker
from adapters.mois.sync_checker import MoisSyncChecker
from pipeline.dedup.ensemble import DedupEnsemble
from pipeline.domain_mapper import DomainMapper
from pipeline.image_pipeline import ImagePipeline
from pipeline.normalizer.base import PlaceNormalizer
from pipeline.region_mapper import RegionMapper
from pipeline.translator.batch_translator import BatchTranslator

logger = logging.getLogger(__name__)
scheduler = BlockingScheduler(timezone="Asia/Seoul")


def job_tourapi_sync():
    logger.info("=== [CRON] 관광공사 Sync 시작 ===")
    try:
        TourApiSyncChecker().run()
        PlaceNormalizer().run(source_name="tourapi")
        DomainMapper().run(source_name="tourapi")
        RegionMapper().run(source_name="tourapi")
    except Exception as exc:
        logger.error("관광공사 Sync 오류: %s", exc)


def job_translation_submit():
    logger.info("=== [CRON] 번역 Batch 제출 ===")
    try:
        BatchTranslator().submit()
    except Exception as exc:
        logger.error("번역 제출 오류: %s", exc)


def job_translation_collect():
    logger.info("=== [CRON] 번역 결과 수거 ===")
    try:
        BatchTranslator().collect()
    except Exception as exc:
        logger.error("번역 수거 오류: %s", exc)


def job_mois_sync():
    logger.info("=== [CRON] 행안부 Sync 시작 ===")
    try:
        MoisSyncChecker().run()
        DedupEnsemble().run(source_name="mois")
        PlaceNormalizer().run(source_name="mois")
        DomainMapper().run(source_name="mois")
        RegionMapper().run(source_name="mois")
    except Exception as exc:
        logger.error("행안부 Sync 오류: %s", exc)


def job_image_upload():
    logger.info("=== [CRON] 이미지 업로드 ===")
    try:
        ImagePipeline().run()
    except Exception as exc:
        logger.error("이미지 업로드 오류: %s", exc)


def start():
    scheduler.add_job(
        job_tourapi_sync,
        CronTrigger(day_of_week="mon", hour=2, minute=0),
        id="tourapi_sync",
        name="관광공사 주간 Sync",
        max_instances=1,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        job_translation_submit,
        CronTrigger(hour=1, minute=0),
        id="trans_submit",
        name="번역 Batch 제출",
        max_instances=1,
    )
    scheduler.add_job(
        job_translation_collect,
        CronTrigger(minute=0),
        id="trans_collect",
        name="번역 결과 수거 (매시간)",
        max_instances=1,
    )
    scheduler.add_job(
        job_mois_sync,
        CronTrigger(day_of_week="tue", hour=3, minute=0),
        id="mois_sync",
        name="행안부 주간 Sync",
        max_instances=1,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        job_image_upload,
        CronTrigger(hour=4, minute=0),
        id="image_upload",
        name="Cloudinary 이미지 업로드",
        max_instances=1,
    )

    logger.info("스케줄러 시작 (Ctrl+C로 종료)")
    scheduler.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start()
