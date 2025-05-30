import pandas as pd
import os

# 현재 파일 위치 기준 절대경로 생성
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 파일 경로 설정
ALL_PATH = os.path.join(BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_all.csv")
FILTERED_PATH = os.path.join(
    BASE_DIR, "../data/6_reviews_about_5/kakao_map_reviews_filtered.csv"
)

df = pd.read_csv(ALL_PATH)

# 주요 컬럼 중 하나라도 비어있거나 NaN인 행은 모두 제거
required_cols = [
    "str_name",
    "str_address",
    "str_location_keyword",
    "str_main_category",
    "reviewer_name",
    "reviewer_score",
    "review_date",
    "review_content",
]
filtered_df = df.dropna(subset=required_cols)
filtered_df = filtered_df[filtered_df["review_content"].str.strip() != ""]

print(f"원본 리뷰 수: {len(df)}")
print(f"주요 정보가 모두 있는 리뷰 수: {len(filtered_df)}")
print("\n주요 정보가 모두 있는 샘플 데이터:")
print(filtered_df.head())

# 리뷰 주요 정보가 모두 있는 데이터만 저장
filtered_df.to_csv(FILTERED_PATH, index=False, encoding="utf-8-sig")
print(f"\n주요 정보가 모두 있는 데이터가 '{FILTERED_PATH}'에 저장되었습니다.")
