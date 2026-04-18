"""
Phase 2 파이프라인 실행 스크립트 — 행정안전부 (행안부)
  python scripts/run_phase2.py --csv /data/mois_restaurant.csv

  옵션:
    --csv     인허가 데이터 CSV 경로 (최초 전체 수집)
    --sync    API history sync 실행
    --categories  sync 할 업종 목록 (쉼표 구분, 기본: 전체)
                  예) --categories "일반음식점,미용업"
    --dedup   Dedup 앙상블 → core merge
    --normalize   stage → core 정규화 (dedup 이후 신규 항목)
    --translate   번역 큐 제출 (공통 파이프라인 재사용)
    --collect     번역 결과 수거
    --all         위 전체 실행 (순서대로)
"""
import argparse
import logging
import sys
import traceback
from pathlib import Path

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
logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
logger = logging.getLogger("run_phase2")

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
    parser = argparse.ArgumentParser(description="Phase 2 파이프라인 — 행정안전부")
    parser.add_argument("--csv",        help="인허가 CSV 경로 (전체 수집)")
    parser.add_argument("--sync",       action="store_true", help="MOIS history API sync")
    parser.add_argument("--categories", help="sync 업종 (쉼표 구분). 기본: 전체")
    parser.add_argument("--dedup",      action="store_true", help="Dedup 앙상블 → core merge")
    parser.add_argument("--normalize",  action="store_true", help="stage → core 정규화 (dedup 후 신규)")
    parser.add_argument("--translate",  action="store_true", help="번역 Batch 제출")
    parser.add_argument("--collect",    action="store_true", help="번역 결과 수거")
    parser.add_argument("--all",        action="store_true", help="전체 실행")
    args = parser.parse_args()

    if args.all:
        args.sync      = True
        args.dedup     = True
        args.normalize = True
        args.translate = True
        args.collect   = True

    if not any([args.csv, args.sync, args.dedup, args.normalize,
                args.translate, args.collect]):
        parser.print_help()
        return 0

    # ──────────────────────────────────────────────────────────────
    # ① CSV 전체 수집 (2-1)
    # ──────────────────────────────────────────────────────────────
    if args.csv:
        tag = "2-1"
        csv_target = Path(args.csv)
        csv_files = sorted(csv_target.glob("*.csv")) if csv_target.is_dir() else [csv_target]
        step_start(tag, f"MOIS CSV 수집 ({len(csv_files)}개 파일)")
        try:
            from adapters.mois.collector import MoisCollector
            collector = MoisCollector()
            for csv_file in csv_files:
                logger.info("[2-1] 처리 중: %s", csv_file.name)
                run_id = collector.run_full(csv_file)
                step_ok(tag, f"{csv_file.name}  run_id={run_id}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # ② API Sync (2-3)
    # ──────────────────────────────────────────────────────────────
    if args.sync:
        tag = "2-3"
        categories = None
        if args.categories:
            categories = [c.strip() for c in args.categories.split(",") if c.strip()]
        desc = f"MOIS history API Sync ({', '.join(categories) if categories else '전체 업종'})"
        step_start(tag, desc)
        try:
            from adapters.mois.sync_checker import MoisSyncChecker
            run_id = MoisSyncChecker().run(categories=categories)
            step_ok(tag, f"run_id={run_id}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # ③ Dedup 앙상블 → core merge (2-4)
    # ──────────────────────────────────────────────────────────────
    if args.dedup:
        tag = "2-4"
        step_start(tag, "Dedup 3단계 앙상블 → core merge")
        try:
            from pipeline.dedup.ensemble import DedupEnsemble
            result = DedupEnsemble().run(source_name="mois")
            step_ok(tag, f"병합 {result['merged']}, 검토큐 {result['review']}, 신규 {result['inserted']}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # ④ 정규화 + 도메인/지역 매핑 (dedup 신규 항목)
    # ──────────────────────────────────────────────────────────────
    if args.normalize:
        tag = "2-4b"
        step_start(tag, "stage → core 정규화 (Dedup 신규 항목)")
        try:
            from pipeline.normalizer.base import PlaceNormalizer
            cnt = PlaceNormalizer().run(source_name="mois")
            step_ok(tag, f"{cnt}건 처리")
        except Exception as exc:
            return step_fail(tag, exc)

        tag = "2-4c"
        step_start(tag, "domain_map.json → display_domain 매핑")
        try:
            from pipeline.domain_mapper import DomainMapper
            cnt = DomainMapper().run(source_name="mois")
            step_ok(tag, f"{cnt}건 갱신")
        except Exception as exc:
            return step_fail(tag, exc)

        tag = "2-4d"
        step_start(tag, "region_map.json → display_region 매핑")
        try:
            from pipeline.region_mapper import RegionMapper
            cnt = RegionMapper().run(source_name="mois")
            step_ok(tag, f"{cnt}건 갱신")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # ⑤ 번역 제출 (2-5, 공통 재사용)
    # ──────────────────────────────────────────────────────────────
    if args.translate:
        tag = "2-5 submit"
        step_start(tag, "번역 Batch 제출 (신규·변경분만, GPT-4.1 mini)")
        try:
            from pipeline.translator.batch_translator import BatchTranslator
            job_ids = BatchTranslator().submit()
            step_ok(tag, f"Batch Job {len(job_ids)}개 제출: {job_ids}")
        except Exception as exc:
            return step_fail(tag, exc)

    # ──────────────────────────────────────────────────────────────
    # ⑥ 번역 수거 (2-5, 공통 재사용)
    # ──────────────────────────────────────────────────────────────
    if args.collect:
        tag = "2-5 collect"
        step_start(tag, "번역 결과 수거 → place_translations 저장")
        try:
            from pipeline.translator.batch_translator import BatchTranslator
            cnt = BatchTranslator().collect()
            step_ok(tag, f"{cnt}건 저장")
        except Exception as exc:
            return step_fail(tag, exc)

    logger.info(SEP)
    logger.info("OK Phase 2 파이프라인 완료")
    logger.info(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
