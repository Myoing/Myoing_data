import pandas as pd
import os
import re
import datetime

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
        "선릉로 155길",
        "선릉로 157길",
        "선릉로 161길",
        "도산대로 49길",
        "도산대로 51길",
        "도산대로 53길",
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


def filter_by_opening_hours(df):
    """
    21시부터 09시 사이에 영업하는 가게만 필터링합니다.

    Args:
        df (pandas.DataFrame): 가게 정보가 담긴 데이터프레임

    Returns:
        pandas.DataFrame: 필터링된 데이터프레임
    """
    # 결과를 저장할 빈 데이터프레임 생성
    filtered_df = pd.DataFrame(columns=df.columns)

    for _, row in df.iterrows():
        hours = row.get("hours", "")
        if not hours or pd.isna(hours):
            continue

        # 영업 시간 문자열에서 시간 정보 추출
        # 예: "09:00 - 19:00" 또는 "17:00 - 00:00" 또는 "22:30 - 06:00"
        time_pattern = r"(\d{1,2}):(\d{2})\s*[~-]\s*(\d{1,2}):(\d{2})"
        matches = re.findall(time_pattern, hours)

        if not matches:
            continue

        # 첫 번째 매칭된 시간 정보 사용
        start_hour, start_min, end_hour, end_min = map(int, matches[0])

        # 종료 시간이 24시(00시)인 경우 처리
        if end_hour == 0:
            end_hour = 24

        # 영업 시간이 21시부터 09시 사이에 걸치는지 확인
        is_night_business = False

        # 시작 시간이 종료 시간보다 작은 경우 (예: 09:00 - 19:00)
        if start_hour < end_hour:
            # 21시부터 24시 사이에 시작하거나, 0시부터 9시 사이에 종료하는 경우
            if start_hour >= 21 or end_hour <= 9:
                is_night_business = True
        # 시작 시간이 종료 시간보다 큰 경우 (예: 22:30 - 06:00)
        else:
            # 항상 밤 영업으로 간주
            is_night_business = True

        if is_night_business:
            filtered_df = pd.concat(
                [filtered_df, pd.DataFrame([row])], ignore_index=True
            )

    return filtered_df


def filter_by_reviews(df):
    """
    리뷰가 있는 가게만 필터링합니다.

    Args:
        df (pandas.DataFrame): 가게 정보가 담긴 데이터프레임

    Returns:
        pandas.DataFrame: 필터링된 데이터프레임
    """
    # review_count 컬럼이 있는지 확인
    if "review_count" not in df.columns:
        print("경고: 'review_count' 컬럼이 데이터프레임에 없습니다.")
        return df

    # 리뷰 수가 0보다 큰 가게만 필터링
    filtered_df = df[df["review_count"] > 0]

    # 필터링 결과가 비어있는 경우 원본 데이터프레임 반환
    if filtered_df.empty:
        print("경고: 리뷰가 있는 가게가 없습니다. 모든 가게를 포함합니다.")
        return df

    return filtered_df


def filter_club_data(df):
    """
    클럽 데이터에서 '나이트,클럽' 카테고리에 해당하는 행만 필터링합니다.

    Args:
        df (pandas.DataFrame): 클럽 데이터가 담긴 데이터프레임

    Returns:
        pandas.DataFrame: 필터링된 데이터프레임
    """
    # category 열에서 '나이트,클럽'에 해당하는 행만 필터링
    filtered_df = df[df["category"].str.contains("나이트,클럽", na=False)]
    return filtered_df


def process_all_locations():
    """
    모든 지역의 데이터를 필터링하고 결과를 저장하는 함수
    """
    # 데이터 디렉토리 경로
    data_dir = "data/1_location_categories"
    filtered_dir = "data/3_filtered_location_categories"
    filtered_all_dir = "data/4_filtered_all"
    club_filtered_dir = "data/5_filtered_clubs"
    os.makedirs(filtered_dir, exist_ok=True)
    os.makedirs(filtered_all_dir, exist_ok=True)
    os.makedirs(club_filtered_dir, exist_ok=True)

    # 모든 필터링된 데이터를 저장할 리스트
    all_filtered_data = []
    all_club_data = []

    # 필터링 결과가 없는 카테고리 목록
    empty_categories = []

    # 각 지역별로 처리
    for location in ADDRESS_FILTERS.keys():
        print(f"{location} 데이터 필터링 중...")

        # 해당 지역의 모든 카테고리 파일 처리 (클럽 제외)
        for category in ["식당", "카페", "술집", "노래방", "PC방", "볼링장", "당구장"]:
            file_path = os.path.join(data_dir, f"{location}_{category}.csv")

            # 파일이 존재하는 경우에만 처리
            if os.path.exists(file_path):
                try:
                    # 데이터 로드
                    df = pd.read_csv(file_path, encoding="utf-8-sig")
                    print(f"  - {category}: {len(df)}개 가게 로드됨")

                    # 필터링 적용
                    filtered_df = filter_by_address(df, location)
                    print(
                        f"  - {category}: 주소 필터링 후 {len(filtered_df)}개 가게 남음"
                    )

                    # 필터링된 데이터가 있는 경우에만 계속 진행
                    if not filtered_df.empty:
                        # 영업 시간 필터링 적용 (21시-09시 영업 가게만)
                        time_filtered_df = filter_by_opening_hours(filtered_df)
                        print(
                            f"  - {category}: 영업 시간 필터링 후 {len(time_filtered_df)}개 가게 남음"
                        )

                        # 리뷰 필터링 적용 (리뷰가 있는 가게만)
                        review_filtered_df = filter_by_reviews(time_filtered_df)
                        print(
                            f"  - {category}: 리뷰 필터링 후 {len(review_filtered_df)}개 가게 남음"
                        )

                        # 필터링된 데이터가 있는 경우에만 저장
                        if not review_filtered_df.empty:
                            # 필터링된 데이터 저장
                            filtered_file_path = os.path.join(
                                filtered_dir, f"{location}_{category}_filtered.csv"
                            )
                            review_filtered_df.to_csv(
                                filtered_file_path, index=False, encoding="utf-8-sig"
                            )

                            # 전체 데이터에 추가
                            all_filtered_data.append(review_filtered_df)

                            print(
                                f"  - {category}: {len(review_filtered_df)}개 가게 필터링 완료"
                            )
                        else:
                            empty_categories.append(f"{location}_{category}")
                            if len(time_filtered_df) == 0:
                                print(
                                    f"  - {category}: 영업 시간 필터링(21시-09시) 조건을 만족하는 가게가 없어 제외됨"
                                )
                            else:
                                print(
                                    f"  - {category}: 리뷰가 있는 가게가 없어 제외됨 (영업시간 조건 만족: {len(time_filtered_df)}개)"
                                )
                    else:
                        empty_categories.append(f"{location}_{category}")
                        print(
                            f"  - {category}: 지정된 도로명 주소에 해당하는 가게가 없어 제외됨 (전체 {len(df)}개 중 0개 만족)"
                        )

                except Exception as e:
                    print(f"  - {category} 처리 중 오류 발생: {e}")

        # 클럽 데이터 별도 처리
        club_file_path = os.path.join(data_dir, f"{location}_클럽.csv")
        if os.path.exists(club_file_path):
            try:
                # 클럽 데이터 로드
                club_df = pd.read_csv(club_file_path, encoding="utf-8-sig")
                print(f"  - 클럽: {len(club_df)}개 가게 로드됨")

                # 클럽 데이터 필터링
                filtered_club_df = filter_club_data(club_df)
                print(
                    f"  - 클럽: '나이트,클럽' 카테고리 필터링 후 {len(filtered_club_df)}개 가게 남음"
                )

                if not filtered_club_df.empty:
                    # 주소 필터링 적용
                    address_filtered_club_df = filter_by_address(
                        filtered_club_df, location
                    )
                    print(
                        f"  - 클럽: 주소 필터링 후 {len(address_filtered_club_df)}개 가게 남음"
                    )

                    if not address_filtered_club_df.empty:
                        # 영업 시간 필터링 적용 (21시-09시 영업 가게만)
                        time_filtered_club_df = filter_by_opening_hours(
                            address_filtered_club_df
                        )
                        print(
                            f"  - 클럽: 영업 시간 필터링 후 {len(time_filtered_club_df)}개 가게 남음"
                        )

                        # 리뷰 필터링 적용 (리뷰가 있는 가게만)
                        review_filtered_club_df = filter_by_reviews(
                            time_filtered_club_df
                        )
                        print(
                            f"  - 클럽: 리뷰 필터링 후 {len(review_filtered_club_df)}개 가게 남음"
                        )

                        if not review_filtered_club_df.empty:
                            # 필터링된 클럽 데이터 저장
                            club_filtered_file_path = os.path.join(
                                club_filtered_dir, f"{location}_클럽_filtered.csv"
                            )
                            review_filtered_club_df.to_csv(
                                club_filtered_file_path,
                                index=False,
                                encoding="utf-8-sig",
                            )
                            all_club_data.append(review_filtered_club_df)
                            all_filtered_data.append(review_filtered_club_df)
                            print(
                                f"  - 클럽: {len(review_filtered_club_df)}개 가게 필터링 완료"
                            )
                        else:
                            empty_categories.append(f"{location}_클럽")
                            if len(time_filtered_club_df) == 0:
                                print(
                                    f"  - 클럽: 영업 시간 필터링(21시-09시) 조건을 만족하는 가게가 없어 제외됨"
                                )
                            else:
                                print(
                                    f"  - 클럽: 리뷰가 있는 가게가 없어 제외됨 (영업시간 조건 만족: {len(time_filtered_club_df)}개)"
                                )
                    else:
                        empty_categories.append(f"{location}_클럽")
                        print(
                            f"  - 클럽: 지정된 도로명 주소에 해당하는 가게가 없어 제외됨 (전체 {len(filtered_club_df)}개 중 0개 만족)"
                        )
                else:
                    print(
                        f"  - 클럽: '나이트,클럽' 카테고리에 해당하는 가게가 없어 제외됨"
                    )

            except Exception as e:
                print(f"  - 클럽 처리 중 오류 발생: {e}")

    # 모든 필터링된 데이터 통합
    if all_filtered_data:
        combined_df = pd.concat(all_filtered_data, ignore_index=True)
        combined_file_path = os.path.join(filtered_all_dir, "all_filtered_data.csv")
        combined_df.to_csv(combined_file_path, index=False, encoding="utf-8-sig")
        print(f"\n전체 필터링된 데이터 저장 완료: {len(combined_df)}개 가게")

    # 모든 클럽 데이터 통합
    if all_club_data:
        combined_club_df = pd.concat(all_club_data, ignore_index=True)
        combined_club_file_path = os.path.join(
            club_filtered_dir, "all_clubs_filtered.csv"
        )
        combined_club_df.to_csv(
            combined_club_file_path, index=False, encoding="utf-8-sig"
        )
        print(f"\n전체 클럽 데이터 저장 완료: {len(combined_club_df)}개 가게")
    else:
        print("\n필터링된 클럽 데이터가 없습니다.")

    # 필터링 결과가 없는 카테고리 목록 출력
    if empty_categories:
        print("\n필터링 결과가 없는 카테고리 목록:")
        print("==============================\n")
        for category in empty_categories:
            print(f"- {category}")
        print("\n※ 필터링 조건:")
        print("1. 지정된 도로명 주소에 위치")
        print("2. 21시부터 09시 사이 영업")
        print("3. 리뷰가 있는 가게")
    else:
        print("\n모든 카테고리에 필터링 결과가 있습니다.")


if __name__ == "__main__":
    process_all_locations()
