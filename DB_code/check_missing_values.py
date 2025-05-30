import pandas as pd
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_missing_values(stores_df: pd.DataFrame, reviews_df: pd.DataFrame):
    """
    가게와 리뷰 데이터프레임의 결측값을 분석하는 함수

    Args:
        stores_df (pd.DataFrame): 가게 정보 데이터프레임
        reviews_df (pd.DataFrame): 리뷰 정보 데이터프레임
    """
    try:
        # 가게 데이터 결측값 확인
        logger.info("\n\n\n=== 가게 데이터 결측값 확인 ===\n\n\n")
        missing_stores = stores_df[stores_df.isna().any(axis=1)]
        logger.info(f"\n\n\n결측값이 있는 행의 수: {len(missing_stores)}\n\n\n")
        if not missing_stores.empty:
            logger.info("\n\n\n결측값이 있는 행의 데이터:\n\n\n")
            logger.info(missing_stores)

            # 각 컬럼별 결측값 개수 확인
            logger.info("\n\n\n각 컬럼별 결측값 개수:\n\n\n")
            logger.info(stores_df.isna().sum())

        # 리뷰 데이터 결측값 확인
        logger.info("\n\n\n=== 리뷰 데이터 결측값 확인 ===\n\n\n")
        missing_reviews = reviews_df[reviews_df.isna().any(axis=1)]
        logger.info(f"\n\n\n결측값이 있는 행의 수: {len(missing_reviews)}\n\n\n")
        if not missing_reviews.empty:
            logger.info("\n\n\n결측값이 있는 행의 데이터:\n\n\n")
            logger.info(missing_reviews)

            # 각 컬럼별 결측값 개수 확인
            logger.info("\n\n\n각 컬럼별 결측값 개수:\n\n\n")
            logger.info(reviews_df.isna().sum())

    except Exception as e:
        logger.error(f"결측값 확인 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    # CSV 파일 읽기
    stores_df = pd.read_csv(
        "data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv"
    )
    reviews_df = pd.read_csv("data/6_reviews_about_5/kakao_map_reviews_filtered.csv")

    # 결측값 확인 실행
    check_missing_values(stores_df, reviews_df)
