"""
Phase 1 전체 파이프라인 실행 스크립트
  python scripts/run_phase1.py --csv /data/tourapi_places.csv

  옵션:
    --csv     관광정보 CSV 경로 (필수 — 최초 전체 수집)
    --sync    CSV 없이 API sync만 실행
    --normalize  stage → core 정규화
    --translate  번역 큐 제출
    --collect    번역 결과 수거
    --images     Cloudinary 이미지 업로드
    --all        위 전체 실행 (순서대로)
"""
import argparse
import logging
import sys
import traceback
from pathlib import Path

# Windows 콘솔 UTF-8 강제 설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s %(levelname)-5s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
))
logging.basicConfig(
    level=logging.INFO,
    handlers=[_handler],
    force=True,          # 기존 핸들러 덮어쓰기
)
logger = logging.getLogger("run_phase1")

SEP = "-" * 60


def step_start(tag: str, desc: str):
    logger.info(SEP)
    logger.info(">> START  [%s] %s", tag, desc)
    logger.info(SEP)


def step_ok(tag: str, detail: str = ""):
    msg = f"OK DONE   [{tag}]"
    if detail:
        msg += f"  {detail}"
    logger.info(msg)


def step_fail(tag: str, exc: Exception) -> int:
    logger.error(SEP)
    logger.error("!! FAILED [%s] %s: %s", tag, type(exc).__name__, exc)
    logger.error("   원인: %s", traceback.format_exc().strip().splitlines()[-1])
    logger.error(SEP)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 1 파이프라인 실행")
    parser.add_argument("--csv",       help="관광정보 CSV 경로")
    parser.add_argument("--sync",      action="store_true", help="TourAPI sync (areaBasedSyncList2)")
    parser.add_argument("--normalize", action="store_true", help="stage → core 정규화")
    parser.add_argument("--translate", action="store_true", help="번역 실행 (Juso+DeepSeek+Gemini)")
    parser.add_argument("--images",    action="store_true", help="Cloudinary 이미지 업로드")
    parser.add_argument("--all",       action="store_true", help="전체 실행")
    args = parser.parse_args()

    if args.all:
        args.sync      = True
        args.normalize = True
        args.translate = True
        args.images    = True

    if not any([args.csv, args.sync, args.normalize, args.translate, args.images]):
        parser.print_help()
        return 0

    # ──────────────────────────────────────────────────────────────
    # [1-1] CSV 수집
    # ──────────────────────────────────────────────────────────────
    if args.csv:
        tag = "1-1"
        step_start(tag, f"CSV 전체 수집  <-  {args.csv}")
        try:
            from adapters.tourapi.collector import TourApiCollector
            run_id = TourApiCollector().run_full(args.csv)
            step_ok(tag, f"run_id={run_id}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # [1-2] API Sync
    # ──────────────────────────────────────────────────────────────
    if args.sync:
        tag = "1-2"
        step_start(tag, "areaBasedSyncList2 API Sync")
        try:
            from adapters.tourapi.sync_checker import TourApiSyncChecker
            run_id = TourApiSyncChecker().run()
            step_ok(tag, f"run_id={run_id}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # [1-3] 정규화
    # ──────────────────────────────────────────────────────────────
    if args.normalize:
        tag = "1-3"
        step_start(tag, "stage -> core 정규화  (PlaceNormalizer)")
        try:
            from pipeline.normalizer.base import PlaceNormalizer
            cnt = PlaceNormalizer().run(source_name="tourapi")
            step_ok(tag, f"{cnt}건 처리")
        except Exception as exc:
            return step_fail(tag, exc)

        # [1-4a] 도메인 매핑
        tag = "1-4a"
        step_start(tag, "domain_map.json -> display_domain 매핑")
        try:
            from pipeline.domain_mapper import DomainMapper
            cnt = DomainMapper().run(source_name="tourapi")
            step_ok(tag, f"{cnt}건 갱신")
        except Exception as exc:
            return step_fail(tag, exc)

        # [1-4b] 지역 매핑
        tag = "1-4b"
        step_start(tag, "region_map.json -> display_region 매핑")
        try:
            from pipeline.region_mapper import RegionMapper
            cnt = RegionMapper().run(source_name="tourapi")
            step_ok(tag, f"{cnt}건 갱신")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # [1-7] 번역 실행
    #   ① 주소(한→영): 주소정보누리집 API
    #   ② zh-CN/zh-TW: DeepSeek
    #   ③ en/ja/th   : Gemini
    # ──────────────────────────────────────────────────────────────
    if args.translate:
        tag = "1-7"
        step_start(tag, "번역 (Juso주소 + DeepSeek zh + Gemini en/ja/th)")
        try:
            from pipeline.translator.batch_translator import TranslationOrchestrator
            result = TranslationOrchestrator().run()
            step_ok(tag, f"주소={result['address_en']} DeepSeek={result['deepseek']} Gemini={result['gemini']}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # [1-5] 이미지 업로드
    # ──────────────────────────────────────────────────────────────
    if args.images:
        tag = "1-5"
        step_start(tag, "Cloudinary 이미지 업로드")
        try:
            from pipeline.image_pipeline import ImagePipeline
            result = ImagePipeline().run()
            step_ok(tag, f"업로드 {result.get('uploaded', 0)}, 스킵 {result.get('skipped', 0)}, 에러 {result.get('error', 0)}")
        except Exception as exc:
            return step_fail(tag, exc)

    logger.info(SEP)
    logger.info("OK Phase 1 파이프라인 완료")
    logger.info(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
