import os
import re
import time
import logging
import pandas as pd
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────────────────────
# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ─────────────────────────────────────────────────────────────────────────────
# 1. 영업시간 필터링 (21시–09시)
def filter_by_opening_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    야간 영업 시간(21시~09시)을 기준으로 데이터 필터링 함수.

    입력값:
        df (pandas.DataFrame): 필터링할 데이터프레임(run_time_start, run_time_end 컬럼 포함).

    반환값:
        pandas.DataFrame: 야간 영업 조건을 만족하는 행들만 포함된 데이터프레임.

    설명:
        - 'run_time_start'와 'run_time_end' 컬럼에서 영업 시간 추출.
        - 영업 시작 시간이 21시 이후이거나 종료 시간이 9시 이전인 경우 선택.
        - 영업 시작 시간이 종료 시간보다 큰 경우(ex: 22:00 ~ 02:00) 야간 영업으로 간주.
        - 시간 정보가 없는 행은 제외.
    """
    rows = []
    for _, row in df.iterrows():
        start_time = row.get("run_time_start", "")
        end_time = row.get("run_time_end", "")

        if not isinstance(start_time, str) or not isinstance(end_time, str):
            continue

        if start_time == "상세 정보 확인 요망" or end_time == "상세 정보 확인 요망":
            continue

        try:
            sh, sm = map(int, start_time.split(":"))
            eh, em = map(int, end_time.split(":"))

            if eh == 0:
                eh = 24
            if sh < eh:
                is_night = sh >= 21 or eh <= 9
            else:
                is_night = True
            if is_night:
                rows.append(row)
        except (ValueError, TypeError):
            continue

    return pd.DataFrame(rows, columns=df.columns)


# ─────────────────────────────────────────────────────────────────────────────
# 2. 리뷰 수 필터링 (0보다 큰 가게만)
def filter_by_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    리뷰 수가 0보다 큰 가게만 필터링하는 함수.

    입력값:
        df (pandas.DataFrame): 필터링할 데이터프레임(i_review_count 컬럼 포함).

    반환값:
        pandas.DataFrame: 리뷰 수가 0보다 큰 행만 포함된 데이터프레임.

    설명:
        - 'i_review_count' 컬럼이 없는 경우 경고 로그 출력 후 원본 반환.
        - 리뷰 수가 0보다 큰 행만 선택.
        - 모든 행의 리뷰 수가 0인 경우 경고 로그 출력 후 원본 반환.
    """
    if "i_review_count" not in df.columns:
        logging.warning("⚠️ 'i_review_count' 컬럼이 없습니다. 필터링을 건너뜁니다.")
        return df
    pos = df[df["i_review_count"] > 0]
    if pos.empty:
        logging.warning(
            "⚠️ 모든 가게의 리뷰 수가 0입니다. 필터링을 건너뛰고 원본 데이터를 반환합니다."
        )
        return df
    return pos


# ─────────────────────────────────────────────────────────────────────────────
# 3. 개별 파일 합치기 & 결측값 채우기
def merge_and_fill_filtered_data():
    """
    필터링된 데이터 파일들을 통합하고 결측값 처리하는 함수.

    입력값:
        없음

    반환값:
        없음

    설명:
        - data/3_filtered_location_categories_hour_club/ 폴더의 *.csv 파일들 로드
        - 결측값을 None으로 처리하고 파일 덮어쓰기
        - 모든 데이터를 통합하여 data/4_filtered_all_hour_club/4_filtered_all_hour_club_data.csv로 저장
        - 디렉토리가 없는 경우 자동 생성
    """
    dir3 = "data/3_filtered_location_categories_hour_club"
    dir4 = "data/4_filtered_all_hour_club"
    os.makedirs(dir4, exist_ok=True)

    merged = []
    for fname in os.listdir(dir3):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(dir3, fname)
        df = pd.read_csv(path, encoding="utf-8-sig")

        # 파일명에서 카테고리 정보 추출
        str_main_category = fname.split("_")[1].replace(".csv", "")

        # 클럽 카테고리 파일은 영업시간 필터링 없이 그대로 통합
        if str_main_category != "클럽":
            df = filter_by_opening_hours(df)
            logging.info(f"영업시간 필터링 적용: {fname}")

        df = df.replace("-1", None)  # "-1"을 None으로 변경
        df.to_csv(path, index=False, encoding="utf-8-sig")
        merged.append(df)
        logging.info(f"통합 중: {fname} ({len(df)}개 데이터)")

    if merged:
        all_df = pd.concat(merged, ignore_index=True)
        output_path = os.path.join(dir4, "4_filtered_all_hour_club_data.csv")
        all_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logging.info(f"통합 완료: {output_path} ({len(all_df)}개 데이터)")
    else:
        logging.warning("⚠️ 통합할 데이터가 없습니다.")


# ─────────────────────────────────────────────────────────────────────────────
# 4. 클럽(나이트/클럽) 카테고리 필터링 함수
def filter_club_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    'str_sub_category' 컬럼에 '나이트,클럽'이 포함된 행만 필터링하는 함수.

    입력값:
        df (pandas.DataFrame): 필터링할 데이터프레임(str_sub_category 컬럼 포함).

    반환값:
        pandas.DataFrame: '나이트,클럽'이 포함된 행만 남긴 데이터프레임.

    설명:
        - 'str_sub_category' 컬럼이 없는 경우 경고 로그 출력 후 빈 DataFrame 반환.
        - '나이트,클럽' 문자열이 포함된 행만 선택.
    """
    if "str_sub_category" not in df.columns:
        logging.warning(
            "⚠️ 'str_sub_category' 컬럼이 없습니다. 클럽 필터링을 건너뜁니다."
        )
        return pd.DataFrame(columns=df.columns)

    # '나이트,클럽'이 포함된 행 찾기
    filtered = df[
        df["str_sub_category"].fillna("").str.contains("나이트,클럽", na=False)
    ]

    if filtered.empty:
        logging.info("클럽(나이트,클럽) 카테고리에 해당하는 데이터가 없습니다.")
    else:
        logging.info(f"클럽 필터링 결과: {len(filtered)}개 데이터")

    return filtered


# ─────────────────────────────────────────────────────────────────────────────
# 5. 데이터 필터링 및 저장
def process_and_save_filtered_data():
    """
    데이터셋을 처리하고 필터링된 결과를 저장하는 함수.

    설명:
        - data/1_location_categories 디렉토리에서 CSV 파일들을 읽어옴
        - 각 파일에 대해 영업시간과 클럽 카테고리 필터링 적용
        - 필터링된 결과를 data/3_filtered_location_categories_hour_club 디렉토리에 저장
    """
    # 입력 및 출력 디렉토리 설정
    input_dir = "data/1_location_categories"
    output_dir = "data/3_filtered_location_categories_hour_club"
    os.makedirs(output_dir, exist_ok=True)

    # 입력 디렉토리의 모든 CSV 파일 처리
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)

            # 데이터 읽기
            df = pd.read_csv(input_path)

            # 파일명에서 카테고리 정보 추출
            str_main_category = filename.split("_")[1].replace(".csv", "")

            # 클럽 카테고리인 경우 영업시간 필터링 건너뛰기
            if str_main_category == "클럽":
                # 클럽 카테고리 필터링만 적용
                df = filter_club_category(df)
                logging.info(f"클럽 카테고리 필터링 적용: {filename}")
            else:
                # 영업시간 필터링 적용
                df = filter_by_opening_hours(df)
                logging.info(f"영업시간 필터링 적용: {filename}")

            # 필터링된 데이터 저장
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            logging.info(f"필터링된 데이터 저장 완료: {output_path}")

    logging.info("모든 데이터셋 처리 완료")


# ─────────────────────────────────────────────────────────────────────────────
# 6. 리뷰 수 필터링 및 저장
def process_review_filtered_data():
    """
    4_filtered_all_hour_club의 데이터에 리뷰 수 필터링을 적용하여 저장하는 함수.

    설명:
        - 4_filtered_all_hour_club 폴더의 4_filtered_all_hour_club_data.csv 파일 로드
        - 리뷰 수가 0보다 큰 데이터만 필터링
        - 필터링된 결과를 5_filtered_all_hour_club_reviewcount 폴더에 저장
    """
    input_dir = "data/4_filtered_all_hour_club"
    output_dir = "data/5_filtered_all_hour_club_reviewcount"
    os.makedirs(output_dir, exist_ok=True)

    input_file = os.path.join(input_dir, "4_filtered_all_hour_club_data.csv")
    if not os.path.exists(input_file):
        logging.error(f"⚠️ 입력 파일이 없습니다: {input_file}")
        return

    # 데이터 로드
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    logging.info(f"데이터 로드 완료: {len(df)}개 데이터")

    # 리뷰 수 필터링 적용
    filtered_df = filter_by_reviews(df)
    logging.info(f"리뷰 수 필터링 후: {len(filtered_df)}개 데이터")

    # 결과 저장
    if not filtered_df.empty:
        output_path = os.path.join(
            output_dir, "5_filtered_all_hour_club_reviewcount_data.csv"
        )
        filtered_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logging.info(f"저장 완료: {output_path}")
    else:
        logging.warning("⚠️ 필터링 후 데이터가 없습니다.")


def main():
    """
    데이터 필터링 메인 실행 함수.

    설명:
        전체 데이터 필터링 프로세스를 순차적으로 실행:
        1. 영업시간/클럽 필터링
        2. 데이터 통합
        3. 리뷰 수 필터링
    """
    start_time = time.time()
    logging.info("데이터 필터링 시작")

    try:
        # 1. 영업시간/클럽 필터링
        logging.info("1단계: 영업시간/클럽 필터링 시작")
        process_and_save_filtered_data()
        logging.info("1단계: 영업시간/클럽 필터링 완료")

        # 2. 데이터 통합
        logging.info("2단계: 데이터 통합 시작")
        merge_and_fill_filtered_data()
        logging.info("2단계: 데이터 통합 완료")

        # 3. 리뷰 수 필터링
        logging.info("3단계: 리뷰 수 필터링 시작")
        process_review_filtered_data()
        logging.info("3단계: 리뷰 수 필터링 완료")

    except Exception as e:
        logging.error(f"데이터 필터링 중 오류 발생: {e}")
        raise

    end_time = time.time()
    execution_time = end_time - start_time
    hours = int(execution_time // 3600)
    minutes = int((execution_time % 3600) // 60)
    seconds = int(execution_time % 60)

    logging.info(f"데이터 필터링 완료")
    logging.info(f"총 실행 시간: {hours}시간 {minutes}분 {seconds}초")


if __name__ == "__main__":
    main()
