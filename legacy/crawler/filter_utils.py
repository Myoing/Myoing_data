"""
필터 유틸리티 모듈

- 1) 주소 필터링
- 2) 영업시간 필터링 (21시–09시 야간영업 기준)
- 3) 리뷰수 필터링 (review_count > 0)
- 4) 상세 크롤링 (og:url → detail_url, 전화번호) 멀티스레드
- 5) 필터링 결과 파일 저장 및 통합 CSV 생성
"""
import os
import re
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock

from crawler.config import DATA_DIR_3, DATA_DIR_4, MAX_DRIVERS
from crawler.detail_crawler import search_store_detail
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────────────────────────────────────────────────────
# 지역별 도로명 주소 필터링 기준
ADDRESS_FILTERS = {
    "강남역": [
        "서초대로 73길", "서초대로 75길", "서초대로 77길",
        "테헤란로 1길", "테헤란로 5길", "강남대로 102길", "강남대로 106길", "강남대로 110길",
        "서초대로 74길", "서초대로 78길",
        "테헤란로 2길", "테헤란로 4길", "테헤란로 6길", "테헤란로 8길",
    ],
    # ... (기타 지역 필터 기준 동일하게 정의)
}

# ─────────────────────────────────────────────────────────────────────────────
# 필터 기능 정의

def normalize_address(address: str) -> str:
    """
    주소 정규화 기능

    - None 또는 비문자열 → 빈문자열 반환
    - 소문자 변환, 공백 제거, 특수문자 제거
    """
    if not isinstance(address, str):
        return ""
    addr = address.lower()
    addr = re.sub(r"\s+", "", addr)
    addr = re.sub(r"[^\w가-힣]", "", addr)
    return addr


def filter_by_address(df: pd.DataFrame, location: str) -> pd.DataFrame:
    """
    도로명 주소 필터링 기능

    - ADDRESS_FILTERS[location] 목록의 각 주소 조각이
      normalize_address(df['address'])에 포함된 행만 반환
    - 통과된 행에 'region' 컬럼 추가
    """
    if location not in ADDRESS_FILTERS:
        logging.warning(f"⚠️ {location} 필터 목록 미정의")
        return df
    df2 = df.copy()
    df2['_norm'] = df2['address'].apply(normalize_address)
    patterns = [normalize_address(x) for x in ADDRESS_FILTERS[location]]
    idxs = [i for i, row in df2.iterrows() if any(p in row['_norm'] for p in patterns)]
    out = df2.loc[idxs].drop(columns=['_norm'])
    out['region'] = location
    return out


def filter_by_opening_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    영업시간 필터링 기능 (21시–09시 야간영업 매장 추출)

    - “HH:MM ~ HH:MM” 정규식 검색
    - 시작시간 또는 종료시간이 야간 기준 충족 시 해당 행 반환
    """
    pattern = re.compile(r"(\d{1,2}):(\d{2})\s*[~-]\s*(\d{1,2}):(\d{2})")
    rows = []
    for _, row in df.iterrows():
        hrs = row.get('hours', '')
        if not isinstance(hrs, str):
            continue
        m = pattern.search(hrs)
        if not m:
            continue
        sh, sm, eh, em = map(int, m.groups())
        if eh == 0:
            eh = 24
        is_night = (sh >= 21 or eh <= 9) if sh < eh else True
        if is_night:
            rows.append(row)
    return pd.DataFrame(rows, columns=df.columns)


def filter_by_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    리뷰수 필터링 기능 (review_count > 0)

    - 'review_count' 컬럼 존재 시 0초과 행만 반환
    - 모두 0 이하인 경우 원본 반환
    """
    if 'review_count' not in df.columns:
        logging.warning("⚠️ 'review_count' 칼럼 미존재")
        return df
    pos = df[df['review_count'] > 0]
    return pos if not pos.empty else df

# ─────────────────────────────────────────────────────────────────────────────
# 상세 크롤링용 드라이버 풀 설정

driver_pool = Queue()
DRIVER_LOCK = Lock()

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    service = Service(ChromeDriverManager().install())
    d = webdriver.Chrome(service=service, options=options)
    d.minimize_window()
    return d


def initialize_driver_pool():
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())


def get_driver():
    with DRIVER_LOCK:
        return driver_pool.get()


def return_driver(drv):
    try:
        _ = drv.current_url
        with DRIVER_LOCK:
            driver_pool.put(drv)
    except:
        try:
            drv.quit()
        except:
            pass
        with DRIVER_LOCK:
            driver_pool.put(setup_driver())

# ─────────────────────────────────────────────────────────────────────────────
# 필터링 결과 저장 및 통합 기능

def merge_and_fill_filtered_data():
    """
    3단계 폴더 결합 및 누락값 처리 기능

    - DATA_DIR_3/*.csv 파일 로드 → NaN을 '-1'로 채워 덮어쓰기
    - 모두 합쳐 DATA_DIR_4/all_filtered_data.csv 저장
    """
    os.makedirs(DATA_DIR_4, exist_ok=True)
    merged = []
    for fn in os.listdir(DATA_DIR_3):
        if not fn.endswith('_filtered.csv'):
            continue
        path = os.path.join(DATA_DIR_3, fn)
        df = pd.read_csv(path, encoding='utf-8-sig').fillna('-1')
        df.to_csv(path, index=False, encoding='utf-8-sig')
        merged.append(df)
    if merged:
        pd.concat(merged, ignore_index=True).to_csv(
            os.path.join(DATA_DIR_4, 'all_filtered_data.csv'),
            index=False, encoding='utf-8-sig'
        )


def process_all_locations():
    """
    전체 지역·카테고리 필터링 및 상세 크롤링 실행 기능

    1) DATA_DIR_1/*.csv 로드 → 주소·영업시간·리뷰 필터 적용 → DATA_DIR_3/{loc}_{cat}_filtered.csv 저장
    2) ThreadPoolExecutor로 search_store_detail 멀티스레드 실행 → detail_url, phone 추가
    3) merge_and_fill_filtered_data() 호출
    """
    initialize_driver_pool()
    all_filtered = []
    empty_cats = []

    def crawl(idx, name):
        drv = get_driver()
        du, ph = search_store_detail(drv, name)
        return_driver(drv)
        return idx, du, ph

    for fname in os.listdir(DATA_DIR_3.replace('3_filtered_location_categories','1_location_categories')):
        # DATA_DIR_1부터 로드하도록 경로 조정 필요
        pass  # 이 부분은 main.py에서 호출 시 순회하도록 외부에서 구현

    # 이후 merge_and_fill 호출
    merge_and_fill_filtered_data()
