import os
import pandas as pd
from datetime import datetime
from DB_code.database import engine
from DB_code.models import Base, Store, Review
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import logging
from DB_code.check_missing_values import check_missing_values

# [로깅 설정]
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# [설정: review 중복 비교 시 시간까지 비교할지 여부]
COMPARE_DATE_ONLY = False  # True: 'YYYY-MM-DD'까지만 비교, False: 시간까지 비교

# [경로 설정]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORES_CSV_PATH = os.path.join(
    BASE_DIR, "../data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv"
)
REVIEWS_CSV_PATH = os.path.join(
    BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv"
)


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
        # 1. CSV 파일 읽기
        stores_df = pd.read_csv(STORES_CSV_PATH)
        reviews_df = pd.read_csv(REVIEWS_CSV_PATH)

        # 2. 결측값 확인
        logger.info("데이터 업데이트 전 결측값 분석을 시작합니다...")
        check_missing_values(stores_df, reviews_df)
        logger.info("결측값 분석이 완료되었습니다.")

        # 3. 타입 변환
        stores_df["run_time_start"] = stores_df["run_time_start"].apply(convert_time)
        stores_df["run_time_end"] = stores_df["run_time_end"].apply(convert_time)

        reviews_df["review_date"] = pd.to_datetime(
            reviews_df["review_date"], errors="coerce"
        )
        reviews_df["reviewer_name"] = (
            reviews_df["reviewer_name"].astype(str).str.strip()
        )
        reviews_df["str_name"] = reviews_df["str_name"].astype(str).str.strip()
        reviews_df["str_address"] = reviews_df["str_address"].astype(str).str.strip()

        # 4. 리뷰어 이름 또는 리뷰 날짜 결측인 경우 제거
        reviews_df = reviews_df.dropna(
            subset=["review_date"]
        )  # review_date가 NaT인 행 제거
        reviews_df = reviews_df[
            reviews_df["reviewer_name"] != ""
        ]  # reviewer_name이 빈 문자열인 행 제거
        reviews_df = reviews_df[
            ~reviews_df["reviewer_name"].isna()
        ]  # reviewer_name이 NaN인 행 제거

        # 5. 기존 store 중복 제거
        existing_store = pd.read_sql(
            "SELECT str_name, str_address FROM store_table", engine
        )
        stores_df["pk"] = (
            stores_df["str_name"].astype(str).str.strip()
            + "-"
            + stores_df["str_address"].astype(str).str.strip()
        )
        existing_store["pk"] = (
            existing_store["str_name"].astype(str).str.strip()
            + "-"
            + existing_store["str_address"].astype(str).str.strip()
        )
        stores_df = stores_df[~stores_df["pk"].isin(existing_store["pk"])]
        stores_df = stores_df.drop(columns=["pk"])
        logger.info(f"store_table에 추가될 신규 데이터: {len(stores_df)}건")

        # 6. 기존 review 중복 제거
        existing_review = pd.read_sql(
            "SELECT reviewer_name, review_date FROM review_table", engine
        )
        existing_review["reviewer_name"] = (
            existing_review["reviewer_name"].astype(str).str.strip()
        )
        existing_review["review_date"] = pd.to_datetime(
            existing_review["review_date"], errors="coerce"
        )
        existing_review = existing_review.dropna(subset=["review_date"])

        # 7. 중복 판별용 pk 생성
        if COMPARE_DATE_ONLY:
            existing_review["pk"] = (
                existing_review["reviewer_name"]
                + "|"
                + existing_review["review_date"].dt.strftime("%Y-%m-%d")
            )
            reviews_df["pk"] = (
                reviews_df["reviewer_name"]
                + "|"
                + reviews_df["review_date"].dt.strftime("%Y-%m-%d")
            )
        else:
            existing_review["pk"] = (
                existing_review["reviewer_name"]
                + "|"
                + existing_review["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            reviews_df["pk"] = (
                reviews_df["reviewer_name"]
                + "|"
                + reviews_df["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
            )

        # 8. 중복 제거
        reviews_df = reviews_df.drop_duplicates(subset=["pk"])
        reviews_df = reviews_df[~reviews_df["pk"].isin(existing_review["pk"])]
        logger.info(f"review_table에 추가될 신규 데이터: {len(reviews_df)}건")

        # 9. 중복 제거 후 pk 컬럼 제거
        reviews_df = reviews_df.drop(columns=["pk"])

        # 10. 삽입 대상 리뷰 출력
        if not reviews_df.empty:
            logger.info("⬇ 중복 제거 후 삽입 대상 리뷰 전체:")
            logger.info(
                "\n%s",
                reviews_df[["reviewer_name", "review_date"]].to_string(index=False),
            )
        else:
            logger.info("중복 제거 후 삽입 대상 리뷰 없음")

        # 11. DB 삽입
        with Session(engine) as session:
            store_before = pd.read_sql("SELECT COUNT(*) FROM store_table", engine).iloc[
                0, 0
            ]
            review_before = pd.read_sql(
                "SELECT COUNT(*) FROM review_table", engine
            ).iloc[0, 0]

            store_count, review_count, failed_count = 0, 0, 0

            # 11-1) store 삽입
            for _, row in stores_df.iterrows():
                session.add(Store(**row_to_dict_safe(row)))
                store_count += 1

            # 11-2) review 삽입
            for _, row in reviews_df.iterrows():
                try:
                    session.add(Review(**row_to_dict_safe(row)))
                    session.flush()  # 삽입 시점에서 제약 조건 검증
                    review_count += 1
                except IntegrityError:
                    session.rollback()
                    failed_count += 1
                    logger.warning(
                        "중복된 리뷰로 인해 삽입되지 않음: %s", row.to_dict()
                    )

            session.commit()

            store_after = pd.read_sql("SELECT COUNT(*) FROM store_table", engine).iloc[
                0, 0
            ]
            review_after = pd.read_sql(
                "SELECT COUNT(*) FROM review_table", engine
            ).iloc[0, 0]

        # 12. 결과 요약
        logger.info(
            f"store_table: {store_before} → {store_after} (증가: {store_after - store_before})"
        )
        logger.info(
            f"review_table: {review_before} → {review_after} (증가: {review_after - review_before})"
        )

        if failed_count > 0:
            logger.warning(f"삽입 실패한 리뷰가 {failed_count}건 있습니다.")
        else:
            logger.info("삽입 실패한 리뷰는 없습니다.")

        logger.info("데이터 업데이트가 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"데이터 업데이트 중 오류 발생: {e}")
        raise


# [메인 실행]
if __name__ == "__main__":
    update_data()
