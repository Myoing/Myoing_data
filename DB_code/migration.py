"""
─────────────────────────────────────────────────────────────────────────────
[DB 마이그레이션 스크립트]

이 파일은 데이터베이스의 **초기 적재(마이그레이션)**를 위한 스크립트입니다.

- 목적: 데이터베이스 테이블 구조를 생성하고, CSV 파일의 데이터를 최초로 적재합니다.
- 사용 시점: 프로젝트 초기 세팅 또는 전체 데이터베이스를 새로 구축할 때 1회 실행합니다.
- 동작 방식:
    1. 테이블 생성 (없으면 생성)
    2. CSV 파일에서 데이터 읽기
    3. 결측값(누락 데이터) 분석 및 로그 출력
    4. 시간/날짜 타입 변환 및 NaN/NaT → None 변환
    5. 모든 데이터를 DB에 삽입

※ 이 스크립트는 **초기 적재(마이그레이션)** 용도로만 사용합니다.

─────────────────────────────────────────────────────────────────────────────
[업데이트(증분 적재)와의 역할 분리]

- **초기 적재(마이그레이션)**: 이 파일(DB_code/migration.py)에서 담당
- **데이터 업데이트(증분 적재)**: DB_code/data_updater.py에서 담당
    - 새로운 데이터가 주기적으로 쌓일 때, 기존 DB와 중복되지 않는 데이터만 추가
    - 데이터 동기화/갱신이 필요할 때 여러 번 실행 가능

─────────────────────────────────────────────────────────────────────────────
"""

import os
import pandas as pd
from datetime import datetime
from database import engine
from models import Base, Store, Review
from sqlalchemy.orm import Session
import logging
from check_missing_values import check_missing_values

# [로깅 설정]
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# [경로 설정] 현재 파일 기준의 절대경로를 기반으로 상대경로 생성
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORES_CSV_PATH = os.path.join(
    BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv"
)
REVIEWS_CSV_PATH = os.path.join(
    BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv"
)


# [테이블 생성 함수 정의]
def create_tables():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("테이블이 성공적으로 생성되었습니다.")
    except Exception as e:
        logger.error(f"테이블 생성 중 오류 발생: {e}")
        raise


# [시간 문자열을 datetime.time 객체로 변환하는 함수 정의]
def convert_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except:
        return None


# [NaN/NaT 값을 삽입 가능한 None으로 안전하게 변환하는 함수 정의]
def row_to_dict_safe(row):
    """
    모든 칼럼의 NaN/NaT 값을 None으로 변환하여 SQL 삽입 시 오류 방지
    """
    return {k: None if pd.isna(v) else v for k, v in row.items()}


# [마이그레이션 수행 함수 정의]
def migrate_data():
    try:
        # 1. CSV 파일 읽기
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # 2. 결측값 분석 로그 출력
        logger.info("데이터 마이그레이션 전 결측값 분석을 시작합니다...")
        check_missing_values(stores_df, reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # 3. 데이터 타입 변환
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)
        reviews_df["review_date"] = pd.to_datetime(
            reviews_df["review_date"], errors="coerce"
        )

        # 4. SQLAlchemy 세션 내에서 행 삽입 (NaN/NaT → None 변환 포함)
        with Session(engine) as session:
            for _, row in stores_df.iterrows():
                session.merge(Store(**row_to_dict_safe(row)))
            for _, row in reviews_df.iterrows():
                session.merge(Review(**row_to_dict_safe(row)))
            session.commit()

        logger.info("데이터 마이그레이션이 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 마이그레이션 중 오류 발생: {e}")
        raise


# [메인 함수 실행 구간]
if __name__ == "__main__":
    create_tables()
    migrate_data()
