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

# [NaN 값을 None으로 안전하게 변환]
def row_to_dict_safe(row):
    return {k: None if pd.isna(v) else v for k, v in row.items()}

# [마이그레이션 수행 함수]
def migrate_data():
    try:
        # 1. CSV 파일 읽기
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # 2. 결측값 확인
        logger.info("데이터 마이그레이션 전 결측값 분석을 시작합니다...")
        check_missing_values(stores_df, reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # 3. 시간 및 날짜 타입 변환
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)
        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"], errors="coerce")

        # 4. 기존 데이터와 중복 제거 (str_name + str_address 기준)
        existing_store = pd.read_sql("SELECT str_name, str_address FROM store_table", engine)
        stores_df["pk"] = stores_df["str_name"] + "-" + stores_df["str_address"]
        existing_store["pk"] = existing_store["str_name"] + "-" + existing_store["str_address"]
        stores_df = stores_df[~stores_df["pk"].isin(existing_store["pk"])]
        stores_df = stores_df.drop(columns=["pk"])

        # 5. 세션 내 안전한 삽입 수행
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

# [메인 함수]
if __name__ == "__main__":
    create_tables()
    migrate_data()