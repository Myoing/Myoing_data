import os
import pandas as pd
from datetime import datetime
from database import engine
from models import Base, Store, Review
from sqlalchemy.orm import Session
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 현재 파일 위치 기준 절대경로 생성
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 파일 경로 설정
STORES_CSV_PATH = os.path.join(BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv")
REVIEWS_CSV_PATH = os.path.join(BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv")


def create_tables():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("테이블이 성공적으로 생성되었습니다.")
    except Exception as e:
        logger.error(f"테이블 생성 중 오류 발생: {e}")
        raise


def convert_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except:
        return None


def migrate_data():
    try:
        # CSV 파일 읽기
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # NaN → None (MySQL에 삽입 가능한 값으로 변환)
        stores_df = stores_df.where(pd.notnull(stores_df), None)
        reviews_df = reviews_df.where(pd.notnull(reviews_df), None)

        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)
        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"])

        with Session(engine) as session:
            for _, row in stores_df.iterrows():
                session.merge(Store(**row.to_dict()))
            for _, row in reviews_df.iterrows():
                session.merge(Review(**row.to_dict()))
            session.commit()

        logger.info("데이터 마이그레이션이 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 마이그레이션 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    create_tables()
    migrate_data()