import os
import pandas as pd
from datetime import datetime
from database import engine
from models import Base, Store, Review
from sqlalchemy.orm import Session
import logging
from check_missing_values import check_missing_values
from sqlalchemy.exc import IntegrityError

# [로깅 설정]
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger(__name__)

# [설정: review 중복 비교 시 시간까지 비교할지 여부]
COMPARE_DATE_ONLY = False  # True: 'YYYY-MM-DD', False: 'YYYY-MM-DD HH:MM:SS'

# [경로 설정]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORES_CSV_PATH = os.path.join(BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv")
REVIEWS_CSV_PATH = os.path.join(BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv")

# [시간 문자열을 datetime.time 객체로 변환]
def convert_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except:
        return None

# [NaN 값을 None으로 안전하게 변환]
def row_to_dict_safe(row):
    return {k: None if pd.isna(v) else v for k, v in row.items()}

# [데이터베이스 업데이트 수행 함수]
def update_data():
    try:
        logger.info("데이터 업데이트 전 결측값 분석을 시작합니다...")
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        check_missing_values(stores_df, reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # [타입 변환]
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)

        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"], errors="coerce")
        reviews_df["reviewer_name"] = reviews_df["reviewer_name"].astype(str).str.strip()
        reviews_df = reviews_df.dropna(subset=["review_date"])

        # [중복 제거 - Store]
        existing_store = pd.read_sql("SELECT str_name, str_address FROM store_table", engine)
        stores_df["pk"] = stores_df["str_name"].astype(str).str.strip() + "|" + stores_df["str_address"].astype(str).str.strip()
        existing_store["pk"] = existing_store["str_name"].astype(str).str.strip() + "|" + existing_store["str_address"].astype(str).str.strip()
        stores_df = stores_df[~stores_df["pk"].isin(existing_store["pk"])]
        stores_df = stores_df.drop(columns=["pk"])
        logger.info(f"store_table에 추가될 신규 데이터: {len(stores_df)}건")

        # [중복 제거 - Review (str_name, str_address 제거)]
        existing_review = pd.read_sql("SELECT reviewer_name, review_date FROM review_table", engine)
        existing_review["reviewer_name"] = existing_review["reviewer_name"].astype(str).str.strip()
        existing_review["review_date"] = pd.to_datetime(existing_review["review_date"], errors="coerce")
        existing_review = existing_review.dropna(subset=["review_date"])

        # pk 생성 (중복 탐지용)
        if COMPARE_DATE_ONLY:
            existing_review["pk"] = (
                existing_review["reviewer_name"] + "|" +
                existing_review["review_date"].dt.strftime("%Y-%m-%d")
            )
            reviews_df["pk"] = (
                reviews_df["reviewer_name"] + "|" +
                reviews_df["review_date"].dt.strftime("%Y-%m-%d")
            )
        else:
            existing_review["pk"] = (
                existing_review["reviewer_name"] + "|" +
                existing_review["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            reviews_df["pk"] = (
                reviews_df["reviewer_name"] + "|" +
                reviews_df["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
            )

        # 삽입 대상 리뷰 필터링
        reviews_to_insert = reviews_df[~reviews_df["pk"].isin(existing_review["pk"])]
        reviews_to_insert = reviews_to_insert.drop(columns=["pk"])
        logger.info(f"review_table에 추가될 신규 데이터: {len(reviews_to_insert)}건")

        logger.info("⬇ 중복 제거 후 삽입 대상 리뷰 전체:")
        if reviews_to_insert.empty:
            logger.info("삽입할 리뷰 데이터가 없습니다.")
        else:
            logger.info("\n" + reviews_to_insert[["reviewer_name", "review_date"]].to_string(index=False))

        # [DB 삽입]
        with Session(engine) as session:
            initial_store_count = session.query(Store).count()
            initial_review_count = session.query(Review).count()

            # Store 삽입
            for _, row in stores_df.iterrows():
                session.merge(Store(**row_to_dict_safe(row)))

            # Review 삽입 (실패 항목 추적)
            failed_reviews = []
            for _, row in reviews_to_insert.iterrows():
                try:
                    session.merge(Review(**row_to_dict_safe(row)))
                except IntegrityError as e:
                    failed_reviews.append((row["reviewer_name"], row["review_date"], str(e)))
                    session.rollback()

            session.commit()

            final_store_count = session.query(Store).count()
            final_review_count = session.query(Review).count()

        # [요약 로그]
        logger.info(f"store_table: {initial_store_count} → {final_store_count} (증가: {final_store_count - initial_store_count})")
        logger.info(f"review_table: {initial_review_count} → {final_review_count} (증가: {final_review_count - initial_review_count})")

        # [에러 로그]
        if failed_reviews:
            logger.warning(f"총 {len(failed_reviews)}건의 리뷰가 삽입되지 않았습니다. 사유:")
            for name, date, reason in failed_reviews:
                logger.warning(f"삽입 실패 - {name}, {date} → {reason.splitlines()[0]}")
        else:
            logger.info("삽입 실패한 리뷰는 없습니다.")

        logger.info("데이터 업데이트가 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 업데이트 중 오류 발생: {e}")
        raise

# [메인 실행]
if __name__ == "__main__":
    update_data()