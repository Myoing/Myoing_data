import pandas as pd
import logging
from database import engine

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def update_database():
    """크롤링된 새로운 데이터를 데이터베이스에 업데이트"""
    try:
        logger.info("데이터베이스 업데이트 시작")

        # 새로운 크롤링 데이터 로드
        new_stores_df = pd.read_csv("data/4_filtered_all_hour_club_data.csv")
        new_reviews_df = pd.read_csv("data/kakao_map_reviews_filtered.csv")

        # DB에서 기존 데이터 로드
        existing_stores = pd.read_sql("SELECT * FROM store_table", engine)
        existing_reviews = pd.read_sql("SELECT * FROM review_table", engine)

        # 새로운 데이터만 필터링 (URL 기준으로 중복 체크)
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
        raise e


if __name__ == "__main__":
    update_database()
