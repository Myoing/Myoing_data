"""
카카오맵 크롤러 설정 모듈

- 드라이버풀 크기, 데이터 저장 경로, 로깅 포맷 등  
  전체 크롤러 실행에 필요한 고정 설정값 정의 모듈
"""

import os
import logging

# ─────────────────────────────────────────────────────────────────────────────
# 드라이버풀 설정값
MAX_DRIVERS = 4                      # 동시 실행할 크롬 드라이버 인스턴스 개수 설정값

# ─────────────────────────────────────────────────────────────────────────────
# 데이터 경로 설정값
DATA_DIR_1 = "data/1_location_categories"      # 기본 크롤링 결과 저장 디렉터리
DATA_DIR_2 = "data/2_combined_location_categories"  # 결합된 CSV 저장 디렉터리
DATA_DIR_3 = "data/3_filtered_location_categories"  # 필터링 결과 저장 디렉터리
DATA_DIR_4 = "data/4_filtered_all"                   # 통합된 필터링 결과 저장 디렉터리
DATA_DIR_5 = "data/5_filtered_clubs"                # 클럽 카테고리별 저장 디렉터리
DATA_DIR_6 = "data/6_reviews_about_4"               # 리뷰 결과 저장 디렉터리

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 포맷 설정값
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = logging.INFO

def initialize_logging():
    """
    로깅 초기화 함수

    - LOG_FORMAT, LOG_DATEFMT, LOG_LEVEL에 정의된 값으로  
      전역 로거 설정 실행
    """
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
    )
