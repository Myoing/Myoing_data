import logging
import time
import os
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
from threading import Lock
from datetime import datetime

# 공통으로 사용할 드라이버 풀 관련 변수
driver_pool = Queue()
MAX_DRIVERS = 4
driver_lock = Lock()

# 각 모듈 임포트
import code.kakao_map_basic_crawler as basic_crawler
import code.filters as filters
import code.review_crawler as review_crawler

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crawler.log"),
    ],
)

# 디렉토리 경로 상수 정의
DATA_DIRS = {
    "basic": "data/1_location_categories",
    "combined": "data/2_combined_location_categories",
    "filtered": "data/3_filtered_location_categories_hour_club",
    "all_filtered": "data/4_filtered_all_hour_club",
    "review_filtered": "data/5_filtered_all_hour_club_reviewcount",
    "reviews": "data/6_reviews_about_5",
}


def create_required_directories():
    """필요한 모든 디렉토리 생성"""
    for dir_path in DATA_DIRS.values():
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f"디렉토리 생성/확인: {dir_path}")


def setup_driver():
    """셀레니움 드라이버 설정 함수"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # headless 모드 사용 시 아래 주석 해제
    # options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()
    return driver


def initialize_driver_pool():
    """드라이버 풀 초기화"""
    # global driver_pool - 참조만 하는 경우 global 선언 불필요

    # 기존 드라이버 풀 정리
    cleanup_driver_pool()

    # 새 드라이버 생성
    logging.info(f"{MAX_DRIVERS}개의 웹 드라이버를 초기화합니다...")
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())
    logging.info("드라이버 풀 초기화 완료")


def cleanup_driver_pool():
    """드라이버 풀의 모든 드라이버 종료 및 정리"""
    global driver_pool

    logging.info("드라이버 풀 정리 중...")
    temp_pool = Queue()

    # 기존 드라이버 풀에서 모든 드라이버를 가져와 종료
    while not driver_pool.empty():
        try:
            driver = driver_pool.get(block=False)
            try:
                driver.quit()
                logging.info("드라이버 종료 완료")
            except Exception as e:
                logging.warning(f"드라이버 종료 중 오류 발생: {e}")
        except Exception:
            break

    # 빈 큐로 초기화
    driver_pool = Queue()


def main():
    try:
        # 1단계: 기본 크롤링 (kakao_map_basic_crawler.py)
        logging.info("====== 1단계: 기본 크롤링 작업 시작 ======")

        # 드라이버 풀 초기화
        initialize_driver_pool()

        # 크롤러 모듈 설정
        basic_crawler.driver_pool = driver_pool
        basic_crawler.MAX_DRIVERS = MAX_DRIVERS
        basic_crawler.driver_lock = driver_lock
        basic_crawler.original_initialize_driver_pool = (
            basic_crawler.initialize_driver_pool
        )
        basic_crawler.initialize_driver_pool = lambda: None

        # 크롤링 실행 - 기본 위치와 카테고리 설정
        str_location_keywords = [
            "강남역",
            "홍대입구역",
            "성수역",
            "압구정역",
            "이태원역",
        ]
        str_main_categories = [
            "식당",
            "카페",
            "술집",
            "노래방",
            "PC방",
            "볼링장",
            "당구장",
            "클럽",
        ]

        # 병렬 처리를 위한 스레드풀 생성
        with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
            # 위치 및 카테고리별 작업 제출
            futures = []
            for str_location_keyword in str_location_keywords:
                for str_main_category in str_main_categories:
                    future = executor.submit(
                        basic_crawler.process_location_category,
                        (str_location_keyword, str_main_category),
                    )
                    futures.append(future)

            # 모든 작업이 완료될 때까지 대기
            for future in futures:
                future.result()

        # 드라이버 풀 정리
        cleanup_driver_pool()
        logging.info("====== 1단계: 기본 크롤링 작업 완료 ======")

        # 2단계: 필터링 (filters.py)
        logging.info("====== 2단계: 필터링 작업 시작 ======")

        # 드라이버 풀 초기화
        initialize_driver_pool()

        # 필터링 모듈 설정
        filters.driver_pool = driver_pool
        filters.MAX_DRIVERS = MAX_DRIVERS
        filters.driver_lock = driver_lock
        filters.original_initialize_driver_pool = filters.initialize_driver_pool
        filters.initialize_driver_pool = lambda: None

        # 필터링 실행
        filters.process_all_locations()

        # 드라이버 풀 정리
        cleanup_driver_pool()
        logging.info("====== 2단계: 필터링 작업 완료 ======")

        # 3단계: 리뷰 크롤링 (review_crawler.py)
        logging.info("====== 3단계: 리뷰 크롤링 시작 ======")

        # 드라이버 풀 다시 초기화
        initialize_driver_pool()

        # 리뷰 크롤러 모듈 설정
        review_crawler.driver_pool = driver_pool
        review_crawler.MAX_DRIVERS = MAX_DRIVERS
        review_crawler.driver_lock = driver_lock
        review_crawler.original_initialize_driver_pool = (
            review_crawler.initialize_driver_pool
        )
        review_crawler.initialize_driver_pool = lambda: None

        # 리뷰 크롤링 실행
        review_crawler.main()

        # 드라이버 풀 정리
        cleanup_driver_pool()
        logging.info("====== 3단계: 리뷰 크롤링 완료 ======")

        logging.info("모든 작업이 완료되었습니다!")

    except Exception as e:
        logging.error(f"실행 중 오류 발생: {e}")

    finally:
        # 최종 드라이버 정리
        cleanup_driver_pool()


if __name__ == "__main__":
    main()
