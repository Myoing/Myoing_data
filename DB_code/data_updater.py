import pandas as pd
import logging
import os
from datetime import datetime
from database import engine
from check_missing_values import check_missing_values

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 현재 파일 위치 기준 절대경로 생성
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 파일 경로 설정
STORES_CSV_PATH = os.path.join(
    BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv"
)
REVIEWS_CSV_PATH = os.path.join(
    BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv"
)

# 시간 문자열을 datetime.time 객체로 변환
def convert_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except:
        return None

def update_database():
    """크롤링된 새로운 데이터를 데이터베이스에 업데이트"""
    try:
        logger.info("데이터베이스 업데이트 시작")

        # 새로운 크롤링 데이터 로드
        new_stores_df = pd.read_csv(STORES_CSV_PATH)
        new_reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # 결측값 분석
        logger.info("업데이트 전 결측값 분석을 시작합니다...")
        check_missing_values(new_stores_df, new_reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # NaN → None 변환 (MySQL 삽입 오류 방지)
        new_stores_df = new_stores_df.applymap(lambda x: None if pd.isna(x) else x)
        new_reviews_df = new_reviews_df.applymap(lambda x: None if pd.isna(x) else x)

        # 시간 및 날짜 타입 변환
        new_stores_df["run_time_start"] = new_stores_df["run_time_start"].apply(convert_time)
        new_stores_df["run_time_end"] = new_stores_df["run_time_end"].apply(convert_time)
        new_reviews_df["review_date"] = pd.to_datetime(new_reviews_df["review_date"], errors="coerce")

        # 기존 DB 데이터 로드
        existing_stores = pd.read_sql("SELECT str_url FROM store_table", engine)
        existing_reviews = pd.read_sql("SELECT review_content FROM review_table", engine)

        # 새로운 데이터만 필터링 (URL 기준 중복 제거)
        new_stores = new_stores_df[
            ~new_stores_df["str_url"].isin(existing_stores["str_url"])
        ]
        new_reviews = new_reviews_df[
            ~new_reviews_df["review_content"].isin(existing_reviews["review_content"])
        ]

        # 새로운 데이터만 DB에 추가
        if not new_stores.empty:
            new_stores.to_sql("store_table", engine, if_exists="append", index=False)
            logger.info(f"새로운 가게 데이터 {len(new_stores)}개 추가됨")

        if not new_reviews.empty:
            new_reviews.to_sql("review_table", engine, if_exists="append", index=False)
            logger.info(f"새로운 리뷰 데이터 {len(new_reviews)}개 추가됨")

        logger.info("데이터베이스 업데이트 완료")

    except Exception as e:
        logger.error(f"데이터베이스 업데이트 중 오류 발생: {e}")
        raise

if __name__ == "__main__":
    update_database()