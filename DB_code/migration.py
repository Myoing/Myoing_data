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
STORES_CSV_PATH = os.path.join(BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv")
REVIEWS_CSV_PATH = os.path.join(BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv")

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
        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"], errors='coerce')

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