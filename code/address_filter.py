import pandas as pd
import os
import re

# 지역별 도로명 주소 정의
ADDRESS_FILTERS = {
    "강남역": [
        # 1-1
        "서초대로 73길",
        "서초대로 75길",
        "서초대로 77길",
        # 1-2
        "테헤란로 1길",
        "테헤란로 5길",
        "강남대로 102길",
        "강남대로 106길",
        "강남대로 110길",
        # 1-3
        "서초대로 74길",
        "서초대로 78길",
        # 1-4
        "테헤란로 2길",
        "테헤란로 4길",
        "테헤란로 6길",
        "테헤란로 8길",
    ],
    "홍대입구역": [
        # 2-1
        "동교로 27길",
        "연남로3길",
        "연남로 5길",
        "월드컵북로 2길",
        "월드컵북로 6길",
        # 2-2
        "동교로 30길",
        "동교로 32길",
        "동교로 34길",
        "동교로 36길",
        "동교로 38길",
        "동교로 46길",
        "연희로 1길",
        # 2-3
        "홍익로 2길",
        "홍익로 4길",
        "와우산로 23길",
        "와우산로 25길",
        "와우산로 27길",
        "와우산로 29길",
        "와우산로 33길",
        "와우산로 35길",
        # 2-4
        "신촌로 2안길",
        "신촌로 4길",
        "신촌로 6길",
        "신촌로 10길",
        "신촌로 12길",
        "와우산로 32길",
        "와우산로 37길",
    ],
    "성수역": [
        # 3-1
        "아차산로 5길",
        "아차산로 7길",
        "성수일로 8길",
        "성수일로 10길",
        "성수일로 12길",
        # 3-2
        "아차산로 9길",
        "아차산로 11길",
        "아차산로 13길",
        "아차산로 15길",
        # 3-3
        "성수이로 7길",
        "성수이로 7가길",
        "연무장 5길",
        "연무장 5가길",
        "연무장 7길",
        "연무장 7가길",
        "연무장 9길",
        # 3-4
        "성수이로 18길",
        "성수이로 20길",
        "연무장 13길",
        "연무장 15길",
    ],
    "압구정역": [
        # 4-1
        "논현로 157길",
        "논현로 161길",
        "논현로 163길",
        "논현로 167길",
        "논현로 175길",
        "압구정로 20길",
        "압구정로 28길",
        # 4-2
        "논현로 172길",
        "논현로 176길",
        "압구정로 30길",
        "압구정로 32길",
        "압구정로 36길",
        "언주로 167길",
        # 4-3
        "논현로 152길",
        "논현로 158길",
        "논현로 164길",
        "도산대로 33길",
        "도산대로 35길",
        "도산대로 37길",
        # 4-4
        "압구정로 42길",
        "압구정로 46길",
        "압구정로 48길",
    ],
    "이태원역": [
        # 5-1
        "이태원로 19길",
        "이태원로 23길",
        "이태원로 27가길",
        # 5-2
        "이태원로 16길",
        "이태원로 20길",
        "이태원로 20가길",
        "이태원로 26길",
        "보광로 59길",
        # 5-3
        "이태원로 45길",
        "이태원로 49길",
        "이태원로 55가길",
        "이태원로 55나길",
        # 5-4
        "이태원로 42길",
        "이태원로 54길",
        "이태원로 54가길",
        "대사관로 5길",
    ],
}


def normalize_address(address):
    """
    주소를 정규화하는 함수

    Args:
        address (str): 정규화할 주소

    Returns:
        str: 정규화된 주소
    """
    if not isinstance(address, str):
        return ""

    # 소문자로 변환
    address = address.lower()

    # 공백 제거
    address = re.sub(r"\s+", "", address)

    # 특수문자 제거
    address = re.sub(r"[^\w\s가-힣]", "", address)

    return address


def filter_by_address(df, location):
    """
    주어진 데이터프레임에서 특정 지역의 도로명 주소에 해당하는 가게만 필터링하는 함수

    Args:
        df (pandas.DataFrame): 필터링할 데이터프레임
        location (str): 지역명 (예: "강남역", "홍대입구역" 등)

    Returns:
        pandas.DataFrame: 필터링된 데이터프레임
    """
    if location not in ADDRESS_FILTERS:
        print(f"경고: {location}에 대한 주소 필터가 정의되어 있지 않습니다.")
        return df

    # 해당 지역의 주소 필터 목록
    address_filters = ADDRESS_FILTERS[location]

    # 정규화된 필터 주소 목록 생성
    normalized_filters = [normalize_address(addr) for addr in address_filters]

    # 주소 컬럼 정규화
    df["normalized_address"] = df["address"].apply(normalize_address)

    # 필터링 적용 (정규화된 주소로 매칭)
    filtered_indices = []
    for idx, row in df.iterrows():
        normalized_addr = row["normalized_address"]
        for filter_addr in normalized_filters:
            if filter_addr in normalized_addr:
                filtered_indices.append(idx)
                break

    # 필터링된 데이터프레임 생성
    filtered_df = df.iloc[filtered_indices].copy()

    # 임시 컬럼 제거
    if "normalized_address" in filtered_df.columns:
        filtered_df = filtered_df.drop("normalized_address", axis=1)

    return filtered_df


def process_all_locations():
    """
    모든 지역의 데이터를 필터링하고 결과를 저장하는 함수
    """
    # 데이터 디렉토리 경로
    data_dir = "data/1_location_categories"
    filtered_dir = "data/3_filtered_location_categories"
    filtered_all_dir = "data/4_filtered_all"
    os.makedirs(filtered_dir, exist_ok=True)
    os.makedirs(filtered_all_dir, exist_ok=True)

    # 모든 필터링된 데이터를 저장할 리스트
    all_filtered_data = []

    # 각 지역별로 처리
    for location in ADDRESS_FILTERS.keys():
        print(f"{location} 데이터 필터링 중...")

        # 해당 지역의 모든 카테고리 파일 처리
        for category in [
            "식당",
            "카페",
            "술집",
            "노래방",
            "PC방",
            "클럽",
            "볼링장",
            "당구장",
        ]:
            file_path = os.path.join(data_dir, f"{location}_{category}.csv")

            # 파일이 존재하는 경우에만 처리
            if os.path.exists(file_path):
                try:
                    # 데이터 로드
                    df = pd.read_csv(file_path, encoding="utf-8-sig")

                    # 필터링 적용
                    filtered_df = filter_by_address(df, location)

                    # 필터링된 데이터가 있는 경우에만 저장
                    if not filtered_df.empty:
                        # 필터링된 데이터 저장
                        filtered_file_path = os.path.join(
                            filtered_dir, f"{location}_{category}_filtered.csv"
                        )
                        filtered_df.to_csv(
                            filtered_file_path, index=False, encoding="utf-8-sig"
                        )

                        # 전체 데이터에 추가
                        all_filtered_data.append(filtered_df)

                        print(f"  - {category}: {len(filtered_df)}개 가게 필터링 완료")
                    else:
                        print(f"  - {category}: 필터링된 가게 없음")

                except Exception as e:
                    print(f"  - {category} 처리 중 오류 발생: {e}")

    # 모든 필터링된 데이터 통합
    if all_filtered_data:
        combined_df = pd.concat(all_filtered_data, ignore_index=True)
        combined_file_path = os.path.join(filtered_all_dir, "all_filtered_data.csv")
        combined_df.to_csv(combined_file_path, index=False, encoding="utf-8-sig")
        print(f"\n전체 필터링된 데이터 저장 완료: {len(combined_df)}개 가게")
    else:
        print("\n필터링된 데이터가 없습니다.")


if __name__ == "__main__":
    process_all_locations()
