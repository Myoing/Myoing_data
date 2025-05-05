#!/usr/bin/env python3
"""
전체 워크플로우 실행 스크립트

1) kakao_map_basic_crawler 모듈 실행 → data/1_location_categories 생성
2) filter_utils 모듈 실행 → data/3_filtered_location_categories, data/4_filtered_all 생성
3) review_crawler 모듈 실행 → data/6_reviews_about_4 생성

--start-step 옵션으로 1, 2, 3 중 원하는 단계부터 시작할 수 있습니다.
"""

import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor

from crawler.config import (
    initialize_logging,
    DATA_DIR_1,
    DATA_DIR_3,
    DATA_DIR_4,
    MAX_DRIVERS,
)
from crawler.basic_crawler import initialize_driver_pool, process_location_category
from crawler.filter_utils import process_all_locations as filter_main
from crawler.review_crawler import main as review_main

logging.getLogger().setLevel(logging.INFO)


def main(start_step: int):
    # 1단계: 기본 크롤러
    if start_step <= 1:
        logging.info("1단계: 기본 크롤러 시작")
        initialize_driver_pool()
        locations = ["강남역", "홍대입구역", "성수역", "압구정역", "이태원역"]
        categories = [
            "식당",
            "카페",
            "술집",
            "노래방",
            "PC방",
            "볼링장",
            "당구장",
            "클럽",
        ]
        tasks = [(loc, cat) for loc in locations for cat in categories]

        with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
            futures = [
                executor.submit(process_location_category, loc, cat)
                for loc, cat in tasks
            ]
            for f in futures:
                f.result()
        logging.info("1단계 완료\n")

    # 2단계: 필터
    if start_step <= 2:
        logging.info("2단계: 필터 시작")
        filter_main()
        logging.info("2단계 완료\n")

    # 3단계: 리뷰 크롤러
    if start_step <= 3:
        logging.info("3단계: 리뷰 크롤러 시작")
        review_main()
        logging.info("3단계 완료\n")

    logging.info("전체 워크플로우 실행 완료")


if __name__ == "__main__":
    initialize_logging()

    # 폴더가 없으면 만들어 두기
    os.makedirs(DATA_DIR_1, exist_ok=True)
    os.makedirs(DATA_DIR_3, exist_ok=True)
    os.makedirs(DATA_DIR_4, exist_ok=True)
    os.makedirs("data/6_reviews_about_4", exist_ok=True)

    parser = argparse.ArgumentParser(description="파이프라인 단계별 실행")
    parser.add_argument(
        "--start-step",
        type=int,
        choices=[1, 2, 3],
        default=1,
        help="1: 전체 (기본) | 2: 필터부터 | 3: 리뷰크롤러만",
    )
    args = parser.parse_args()

    main(args.start_step)
