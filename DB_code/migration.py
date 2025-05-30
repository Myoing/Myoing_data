import os
import pandas as pd
from datetime import datetime
from database import engine
from models import Base, Store, Review
from sqlalchemy.orm import Session
import logging
from check_missing_values import check_missing_values

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
"""

# [로깅 설정]
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# [경로 설정]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORES_CSV_PATH = os.path.join(BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv")
REVIEWS_CSV_PATH = os.path.join(BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv")


# [테이블 생성 함수]
def create_tables():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("테이블이 성공적으로 생성되었습니다.")
    except Exception as e:
        logger.error(f"테이블 생성 중 오류 발생: {e}")
        raise


# [시간 문자열을 datetime.time 객체로 변환]
def convert_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except:
        return None


# [NaN 값을 None으로 변환]
def row_to_dict_safe(row):
    return {k: None if pd.isna(v) else v for k, v in row.items()}


# [마이그레이션 수행 함수]
def migrate_data():
    try:
        # 1. CSV 불러오기
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # 2. 결측값 분석
        logger.info("데이터 마이그레이션 전 결측값 분석을 시작합니다...")
        check_missing_values(stores_df, reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # 3. 타입 변환
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)
        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"], errors="coerce")
        reviews_df = reviews_df.dropna(subset=["review_date"])

        # 4. 리뷰 중복 제거 (reviewer_name + review_date 기준)
        existing_review = pd.read_sql("SELECT reviewer_name, review_date FROM review_table", engine)
        existing_review["pk"] = existing_review["reviewer_name"].astype(str) + "-" + \
                                pd.to_datetime(existing_review["review_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        reviews_df["pk"] = reviews_df["reviewer_name"].astype(str) + "-" + \
                           reviews_df["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        reviews_df = reviews_df[~reviews_df["pk"].isin(existing_review["pk"])]
        reviews_df = reviews_df.drop(columns=["pk"])
        logger.info(f"중복 제거 후 삽입할 리뷰 수: {len(reviews_df)}건")

        # 5. DB에 삽입
        with Session(engine) as session:
            store_count = 0
            for _, row in stores_df.iterrows():
                session.merge(Store(**row_to_dict_safe(row)))
                store_count += 1

            review_count = 0
            for _, row in reviews_df.iterrows():
                session.merge(Review(**row_to_dict_safe(row)))
                review_count += 1

            session.commit()

        logger.info(f"store_table에 {store_count}개, review_table에 {review_count}개 데이터가 삽입되었습니다.")
        logger.info("데이터 마이그레이션이 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 마이그레이션 중 오류 발생: {e}")
        raise


# [메인 함수 실행]
if __name__ == "__main__":
    create_tables()
    migrate_data()