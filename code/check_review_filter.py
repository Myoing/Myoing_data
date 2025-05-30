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

# 리뷰 내용이 있는 데이터만 필터링
filtered_df = df[
    df["review_content"].notna() & (df["review_content"].str.strip() != "")
]

print(f"원본 리뷰 수: {len(df)}")
print(f"리뷰 내용이 있는 리뷰 수: {len(filtered_df)}")
print("\n리뷰 내용이 있는 샘플 데이터:")
print(filtered_df.head())

# 리뷰 내용이 있는 데이터만 저장
filtered_df.to_csv(FILTERED_PATH, index=False, encoding="utf-8-sig")
print(f"\n리뷰 내용이 있는 데이터가 '{FILTERED_PATH}'에 저장되었습니다.")
