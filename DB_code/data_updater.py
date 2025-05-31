import os
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from database import engine
from models import Store, Review
import logging
from check_missing_values import check_missing_values

# [로깅 설정]
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        # CSV 파일 읽기
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # 결측값 확인
        logger.info("데이터 업데이트 전 결측값 분석을 시작합니다...")
        check_missing_values(stores_df, reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # 시간 필드 변환
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)

        # 문자열 정리 및 타입 변환
        reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"], errors="coerce")
        reviews_df["reviewer_name"] = reviews_df["reviewer_name"].astype(str).str.strip()
        reviews_df["str_name"] = reviews_df["str_name"].astype(str).str.strip()
        reviews_df["str_address"] = reviews_df["str_address"].astype(str).str.strip()
        reviews_df = reviews_df.dropna(subset=["review_date"])

        # 기존 가게 제거
        existing_store = pd.read_sql("SELECT str_name, str_address FROM store_table", engine)
        stores_df["pk"] = stores_df["str_name"].astype(str).str.strip() + "-" + stores_df["str_address"].astype(str).str.strip()
        existing_store["pk"] = existing_store["str_name"].astype(str).str.strip() + "-" + existing_store["str_address"].astype(str).str.strip()
        stores_df = stores_df[~stores_df["pk"].isin(existing_store["pk"])]
        stores_df = stores_df.drop(columns=["pk"])

        logger.info(f"store_table에 추가될 신규 데이터: {len(stores_df)}건")

        # 기존 리뷰 제거
        existing_review = pd.read_sql("SELECT reviewer_name, review_date, str_name, str_address FROM review_table", engine)
        existing_review["reviewer_name"] = existing_review["reviewer_name"].astype(str).str.strip()
        existing_review["str_name"] = existing_review["str_name"].astype(str).str.strip()
        existing_review["str_address"] = existing_review["str_address"].astype(str).str.strip()
        existing_review["review_date"] = pd.to_datetime(existing_review["review_date"], errors="coerce")
        existing_review = existing_review.dropna(subset=["review_date"])

        # 중복 판별용 pk 생성
        existing_review["pk"] = (
            existing_review["reviewer_name"] + "|" +
            existing_review["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S") + "|" +
            existing_review["str_name"] + "|" +
            existing_review["str_address"]
        )
        reviews_df["pk"] = (
            reviews_df["reviewer_name"] + "|" +
            reviews_df["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S") + "|" +
            reviews_df["str_name"] + "|" +
            reviews_df["str_address"]
        )

        # 중복 제거
        reviews_df = reviews_df[~reviews_df["pk"].isin(existing_review["pk"])]
        logger.info(f"review_table에 추가될 신규 데이터: {len(reviews_df)}건")

        # 중복 제거 후 삽입될 리뷰 출력
        if not reviews_df.empty:
            logger.info("⬇ 중복 제거 후 삽입 대상 리뷰 전체:")
            logger.info(f"\n{reviews_df[['reviewer_name', 'review_date']].to_string(index=False)}")

        # pk 컬럼 제거 (모델에 존재하지 않음)
        reviews_df = reviews_df.drop(columns=["pk"], errors="ignore")

        # DB 삽입
        with Session(engine) as session:
            before_store = session.query(Store).count()
            before_review = session.query(Review).count()

            store_count = 0
            for _, row in stores_df.iterrows():
                session.merge(Store(**row_to_dict_safe(row)))
                store_count += 1

            review_count = 0
            failed_reviews = []
            for _, row in reviews_df.iterrows():
                try:
                    session.add(Review(**row_to_dict_safe(row)))
                    review_count += 1
                except Exception as e:
                    failed_reviews.append((row["reviewer_name"], row["review_date"], str(e)))

            session.commit()

            after_store = session.query(Store).count()
            after_review = session.query(Review).count()

        logger.info(f"store_table: {before_store} → {after_store} (증가: {after_store - before_store})")
        logger.info(f"review_table: {before_review} → {after_review} (증가: {after_review - before_review})")

        if failed_reviews:
            logger.warning("삽입 실패한 리뷰가 존재합니다:")
            for name, date, reason in failed_reviews:
                logger.warning(f"- {name} ({date}) → {reason}")
        else:
            logger.info("삽입 실패한 리뷰는 없습니다.")

        logger.info("데이터 업데이트가 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 업데이트 중 오류 발생: {e}")
        raise

# [메인 실행]
if __name__ == "__main__":
    update_data()