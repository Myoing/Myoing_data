import pandas as pd
from datetime import datetime
from database import engine
from models import Base, Store, Review
from sqlalchemy.orm import Session
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tables():
    """데이터베이스 테이블 생성"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("테이블이 성공적으로 생성되었습니다.")
    except Exception as e:
        logger.error(f"테이블 생성 중 오류 발생: {e}")
        raise


def convert_time(time_str):
    """문자열 시간을 datetime.time 객체로 변환"""
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except:
        return None


def migrate_data():
    """CSV 데이터를 데이터베이스로 마이그레이션"""
    try:
        # CSV 파일 읽기
        stores_df = pd.read_csv(
            "data/4_filtered_all_hour_club/filtered_all_hour_club_data.csv"
        )
        reviews_df = pd.read_csv(
            "data/6_reviews_about_5/kakao_map_reviews_filtered.csv"
        )

        # 데이터 타입 변환
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)
        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"])

        # 세션 생성
        with Session(engine) as session:
            # 가게 데이터 삽입
            for _, row in stores_df.iterrows():
                store = Store(**row.to_dict())
                session.merge(store)  # merge를 사용하여 중복 데이터 처리

            # 리뷰 데이터 삽입
            for _, row in reviews_df.iterrows():
                review = Review(**row.to_dict())
                session.merge(review)  # merge를 사용하여 중복 데이터 처리

            # 변경사항 저장
            session.commit()

        logger.info("데이터 마이그레이션이 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 마이그레이션 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    create_tables()
    migrate_data()
