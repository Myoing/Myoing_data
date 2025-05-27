import os
import re
import time
import logging
import pandas as pd
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─────────────────────────────────────────────────────────────────────────────
# 1. 지역별 도로명 주소 정의
ADDRESS_FILTERS = {
    "강남역": [
        "서초대로 73길",
        "서초대로 75길",
        "서초대로 77길",
        "테헤란로 1길",
        "테헤란로 5길",
        "강남대로 102길",
        "강남대로 106길",
        "강남대로 110길",
        "서초대로 74길",
        "서초대로 78길",
        "테헤란로 2길",
        "테헤란로 4길",
        "테헤란로 6길",
        "테헤란로 8길",
    ],
    "홍대입구역": [
        "동교로 27길",
        "연남로3길",
        "연남로 5길",
        "월드컵북로 2길",
        "월드컵북로 6길",
        "동교로 30길",
        "동교로 32길",
        "동교로 34길",
        "동교로 36길",
        "동교로 38길",
        "동교로 46길",
        "연희로 1길",
        "홍익로 2길",
        "홍익로 4길",
        "와우산로 23길",
        "와우산로 25길",
        "와우산로 27길",
        "와우산로 29길",
        "와우산로 33길",
        "와우산로 35길",
        "신촌로 2안길",
        "신촌로 4길",
        "신촌로 6길",
        "신촌로 10길",
        "신촌로 12길",
        "와우산로 32길",
        "와우산로 37길",
    ],
    "성수역": [
        "아차산로 5길",
        "아차산로 7길",
        "성수일로 8길",
        "성수일로 10길",
        "성수일로 12길",
        "아차산로 9길",
        "아차산로 11길",
        "아차산로 13길",
        "아차산로 15길",
        "성수이로 7길",
        "성수이로 7가길",
        "연무장 5길",
        "연무장 5가길",
        "연무장 7길",
        "연무장 7가길",
        "연무장 9길",
        "성수이로 18길",
        "성수이로 20길",
        "연무장 13길",
        "연무장 15길",
    ],
    "압구정역": [
        "논현로 157길",
        "논현로 161길",
        "논현로 163길",
        "논현로 167길",
        "논현로 175길",
        "압구정로 20길",
        "압구정로 28길",
        "논현로 172길",
        "논현로 176길",
        "압구정로 30길",
        "압구정로 32길",
        "압구정로 36길",
        "언주로 167길",
        "논현로 152길",
        "논현로 158길",
        "논현로 164길",
        "도산대로 33길",
        "도산대로 35길",
        "도산대로 37길",
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
        "이태원로 19길",
        "이태원로 23길",
        "이태원로 27가길",
        "이태원로 16길",
        "이태원로 20길",
        "이태원로 20가길",
        "이태원로 26길",
        "보광로 59길",
        "이태원로 45길",
        "이태원로 49길",
        "이태원로 55가길",
        "이태원로 55나길",
        "이태원로 42길",
        "이태원로 54길",
        "이태원로 54가길",
        "대사관로 5길",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# 2. 주소 정규화
def normalize_address(address: str) -> str:
    """
    주소 문자열 정규화 함수.

    입력값:
        address (str): 정규화할 주소 문자열.

    반환값:
        str: 정규화된 주소 문자열.

    설명:
        - 입력된 주소 문자열을 소문자로 변환.
        - 공백 문자 제거.
        - 특수 문자 제거(영문, 숫자, 한글만 유지).
        - 문자열이 아닌 경우 빈 문자열 반환.
    """
    if not isinstance(address, str):
        return ""
    address = address.lower()
    address = re.sub(r"\s+", "", address)
    address = re.sub(r"[^\w\s가-힣]", "", address)
    return address


# ─────────────────────────────────────────────────────────────────────────────
# 3. 주소 필터링 + 'region' 컬럼 추가
def filter_by_address(df: pd.DataFrame, location: str) -> pd.DataFrame:
    """
    데이터프레임의 주소 필터링 및 지역 정보 추가 함수.

    입력값:
        df (pandas.DataFrame): 필터링할 데이터프레임.
        location (str): 지역명(ADDRESS_FILTERS에 정의된 키 중 하나).

    반환값:
        pandas.DataFrame: 주소 필터링된 데이터프레임(region 컬럼 추가됨).

    설명:
        - ADDRESS_FILTERS에 정의된 지역별 도로명 주소 패턴으로 필터링.
        - 정규화된 주소에 지정된 도로명이 포함된 행만 선택.
        - 원본 데이터프레임 변경 없이 복사본 사용.
        - 필터링된 결과에 'region' 컬럼 추가.
    """
    if location not in ADDRESS_FILTERS:
        logging.warning(f"⚠️ {location}에 대한 주소 필터가 정의되어 있지 않습니다.")
        return df
    df = df.copy()
    df["_addr_norm"] = df["str_address"].apply(normalize_address)
    nf_list = [normalize_address(a) for a in ADDRESS_FILTERS[location]]
    keep = []
    for i, row in df.iterrows():
        for nf in nf_list:
            if nf in row["_addr_norm"]:
                keep.append(i)
                break
    out = df.loc[keep].drop(columns=["_addr_norm"])
    out["region"] = location
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 4. 영업시간 필터링 (21시–09시)
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
# 5. 리뷰 수 필터 (>0)
def filter_by_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    리뷰 수가 0보다 큰 가게만 필터링하는 함수.

    입력값:
        df (pandas.DataFrame): 필터링할 데이터프레임(review_count 컬럼 포함).

    반환값:
        pandas.DataFrame: 리뷰 수가 0보다 큰 행만 포함된 데이터프레임.

    설명:
        - 'review_count' 컬럼이 없는 경우 경고 로그 출력 후 원본 반환.
        - 리뷰 수가 0보다 큰 행만 선택.
        - 모든 행의 리뷰 수가 0인 경우 경고 로그 출력 후 원본 반환.
    """
    if "review_count" not in df.columns:
        logging.warning("⚠️ 'review_count' 컬럼이 없습니다. 스킵합니다.")
        return df
    pos = df[df["review_count"] > 0]
    if pos.empty:
        logging.warning("⚠️ 리뷰수가 0인 모든 행! 원본 반환합니다.")
        return df
    return pos


# ─────────────────────────────────────────────────────────────────────────────
# 6. Selenium 드라이버 풀 설정
driver_pool = Queue()
MAX_DRIVERS = 4
driver_lock = Lock()


def setup_driver():
    """
    Selenium 웹 드라이버 설정 및 생성 함수.

    입력값:
        없음

    반환값:
        webdriver.Chrome: 설정된 Chrome 웹 드라이버 객체.

    설명:
        - Chrome 브라우저의 알림 비활성화 옵션 적용.
        - Headless 모드 옵션(주석처리됨) 제공.
        - ChromeDriverManager를 통한 최신 드라이버 설치 및 초기화.
        - 생성된 브라우저 창 최소화.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # options.add_argument("--headless")  # 필요 시 해제
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()
    return driver


def initialize_driver_pool():
    """
    드라이버 풀 초기화 함수.

    입력값:
        없음

    반환값:
        없음

    설명:
        - MAX_DRIVERS 수만큼 Chrome 웹 드라이버 생성.
        - 생성된 드라이버를 driver_pool 큐에 추가.
        - 병렬 처리를 위한 드라이버 풀 준비.
    """
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())


def get_driver():
    """
    드라이버 풀에서 드라이버 가져오기 함수.

    입력값:
        없음

    반환값:
        webdriver.Chrome: 드라이버 풀에서 가져온 웹 드라이버 객체.

    설명:
        - 스레드 안전하게 드라이버 풀에서 드라이버 가져오기(lock 사용).
        - 풀이 비어있는 경우 사용 가능한 드라이버가 반환될 때까지 대기.
    """
    with driver_lock:
        return driver_pool.get()


def return_driver(driver):
    """
    사용 완료된 드라이버를 풀에 반환하는 함수.

    입력값:
        driver (webdriver.Chrome): 반환할 Chrome 웹 드라이버 객체.

    반환값:
        없음

    설명:
        - 드라이버 유효성 확인 후 드라이버 풀에 반환.
        - 드라이버가 유효하지 않은 경우 종료 후 새 드라이버 생성하여 풀에 추가.
        - 스레드 안전하게 드라이버 풀 접근(lock 사용).
    """
    try:
        _ = driver.current_url
        with driver_lock:
            driver_pool.put(driver)
    except Exception:
        try:
            driver.quit()
        except:
            pass
        with driver_lock:
            driver_pool.put(setup_driver())


# ─────────────────────────────────────────────────────────────────────────────
# 7. 상세 크롤링: og:url → detail_url, 전화번호 추출
def search_store_detail(driver: webdriver.Chrome, store_name: str):
    """
    카카오맵에서 가게의 상세 페이지 URL과 전화번호를 크롤링하는 함수.

    입력값:
        driver (webdriver.Chrome): 사용할 Chrome 웹 드라이버 객체.
        store_name (str): 검색할 가게 이름.

    반환값:
        tuple: (detail_url, phone)
            - detail_url (str): 가게 상세 페이지 URL(추출 실패 시 빈 문자열).
            - phone (str): 가게 전화번호(추출 실패 시 "-1").

    설명:
        - 카카오맵에서 가게 이름으로 검색 후 일치하는 결과 찾기.
        - 일치하는 결과 없을 경우 '장소 더보기' 클릭하여 추가 검색.
        - 가게 상세 페이지 접속하여 og:url 메타 태그에서 URL 추출.
        - 상세 정보 섹션에서 전화번호 추출.
        - 검색 실패 시 ("-1", "-1") 반환.
    """
    driver.get("https://map.kakao.com/")
    time.sleep(1.5)
    inp = driver.find_element(By.ID, "search.keyword.query")
    inp.clear()
    inp.send_keys(store_name)
    btn = driver.find_element(By.ID, "search.keyword.submit")
    driver.execute_script("arguments[0].click();", btn)
    time.sleep(1.5)

    items = driver.find_elements(By.CSS_SELECTOR, "ul.placelist li.PlaceItem")
    found = False
    for it in items:
        nm_el = it.find_element(By.CSS_SELECTOR, "a.link_name")
        nm = (nm_el.get_attribute("title") or nm_el.text).strip()
        if nm == store_name:
            driver.execute_script(
                "arguments[0].click();",
                it.find_element(By.CSS_SELECTOR, "a[data-id='moreview']"),
            )
            found = True
            break
    if not found:
        try:
            mb = driver.find_element(By.ID, "info.search.place.more")
            driver.execute_script("arguments[0].click();", mb)
            time.sleep(1.5)
            items = driver.find_elements(By.CSS_SELECTOR, "ul.placelist li.PlaceItem")
            for it in items:
                nm_el = it.find_element(By.CSS_SELECTOR, "a.link_name")
                if (nm_el.get_attribute("title") or nm_el.text).strip() == store_name:
                    driver.execute_script(
                        "arguments[0].click();",
                        it.find_element(By.CSS_SELECTOR, "a[data-id='moreview']"),
                    )
                    found = True
                    break
        except:
            pass
    if not found:
        return "-1", "-1"

    time.sleep(1.5)
    handles = driver.window_handles
    if len(handles) > 1:
        driver.switch_to.window(handles[-1])
    time.sleep(1.5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    meta = soup.find("meta", {"property": "og:url"})
    detail_url = meta["content"] if meta else ""
    phone = "-1"
    di = soup.select_one("div.section_comm.section_defaultinfo")
    if di:
        for unit in di.select("div.unit_default"):
            if unit.select_one("span.ico_mapdesc.ico_call2"):
                sp = unit.select_one("span.txt_detail")
                if sp:
                    phone = sp.get_text(strip=True)
                break

    return detail_url, phone


# ─────────────────────────────────────────────────────────────────────────────
# 8. 개별 파일 합치기 & 결측값 채우기
def merge_and_fill_filtered_data():
    """
    필터링된 데이터 파일들을 통합하고 결측값 처리하는 함수.

    입력값:
        없음

    반환값:
        없음

    설명:
        - data/3_filtered_location_categories/ 폴더의 *_filtered.csv 파일들 로드.
        - 결측값을 '-1'로 채우고 파일 덮어쓰기.
        - 모든 데이터를 통합하여 data/4_filtered_all/all_filtered_data.csv로 저장.
        - 디렉토리가 없는 경우 자동 생성.
    """
    dir3 = "data/3_filtered_location_categories"
    dir4 = "data/4_filtered_all"
    os.makedirs(dir4, exist_ok=True)

    merged = []
    for fname in os.listdir(dir3):
        if not fname.endswith("_filtered.csv"):
            continue
        path = os.path.join(dir3, fname)
        df = pd.read_csv(path, encoding="utf-8-sig")
        df = df.fillna("-1")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        merged.append(df)

    if merged:
        all_df = pd.concat(merged, ignore_index=True)
        all_df.to_csv(
            os.path.join(dir4, "all_filtered_data.csv"),
            index=False,
            encoding="utf-8-sig",
        )


# ─────────────────────────────────────────────────────────────────────────────
# 9. 전체 처리 및 멀티스레드 크롤링
def process_all_locations():
    """
    모든 지역에 대한 데이터 필터링 및 상세 정보 크롤링 수행 함수.

    입력값:
        없음

    반환값:
        없음

    설명:
        - 지역별, 카테고리별 데이터 로드 및 필터링 작업 수행.
        - 주소, 영업시간, 리뷰 수 기준으로 필터링 적용.
        - 가게 상세 정보(URL, 전화번호) 웹 크롤링을 통해 보강.
        - 클럽 카테고리 데이터 별도 처리(카테고리 필터 및 추가 필터별 데이터 저장).
        - 필터링된 데이터를 지역별, 카테고리별로 저장.
        - 통합 데이터 생성 및 저장.
        - 병렬 처리를 통한 크롤링 작업 수행.
        - 필요한 디렉토리 자동 생성.
    """
    data_dir = "data/1_location_categories"
    out_dir = "data/3_filtered_location_categories"
    all_dir = "data/4_filtered_all"
    club_dir = "data/5_filtered_clubs"
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(all_dir, exist_ok=True)
    os.makedirs(club_dir, exist_ok=True)

    categories = ["식당", "카페", "술집", "노래방", "PC방", "볼링장", "당구장"]
    initialize_driver_pool()

    all_filtered = []
    empty_cats = []

    def crawl(idx, store_name):
        """
        가게 상세 정보 크롤링을 위한 내부 함수.

        입력값:
            idx (int): 가게 인덱스.
            store_name (str): 가게 이름.

        반환값:
            tuple: (idx, detail_url, phone)
                - idx (int): 입력받은 가게 인덱스.
                - detail_url (str): 가게 상세 페이지 URL.
                - phone (str): 가게 전화번호.

        설명:
            - 드라이버 풀에서 드라이버 가져와 사용 후 반환.
            - search_store_detail 함수를 통해 가게 상세 정보 크롤링.
            - 멀티스레드 환경에서 병렬 처리를 위한 작업 단위.
        """
        driver = get_driver()
        du, ph = search_store_detail(driver, store_name)
        return_driver(driver)
        return idx, du, ph

    for loc in ADDRESS_FILTERS:
        for cat in categories:
            path = os.path.join(data_dir, f"{loc}_{cat}.csv")
            if not os.path.exists(path):
                continue
            df = pd.read_csv(path, encoding="utf-8-sig")
            logging.info(f"{loc}-{cat}: {len(df)}개 로드")

            f1 = filter_by_address(df, loc)
            f2 = filter_by_opening_hours(f1)
            f3 = filter_by_reviews(f2)
            logging.info(f" → 필터 후: {len(f3)}개")
            if f3.empty:
                empty_cats.append(f"{loc}_{cat}")
                continue

            f3 = f3.reset_index(drop=True)
            # 검색 URL 은 따로 컬럼에 남겨두고,
            # crawl 에는 오로지 '가게명'만 전달합니다.
            f3["search_url"] = f3["name"].apply(
                lambda x: f"https://map.kakao.com/?q={loc} {x}"
            )

            urls = [""] * len(f3)
            phones = [""] * len(f3)

            with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
                future_to_idx = {
                    executor.submit(crawl, idx, row["name"]): idx
                    for idx, row in f3.iterrows()
                }
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        _, du, ph = future.result()
                        urls[idx] = du
                        phones[idx] = ph
                        logging.info(f"[{loc}-{cat}] ({idx+1}/{len(f3)}) 크롤링 완료")
                    except Exception as e:
                        logging.error(f"[{loc}-{cat}] 인덱스 {idx} 크롤링 오류: {e}")

            f3["detail_url"] = urls
            f3["phone"] = phones

            save_path = os.path.join(out_dir, f"{loc}_{cat}_filtered.csv")
            f3.fillna("-1").to_csv(save_path, index=False, encoding="utf-8-sig")
            all_filtered.append(f3)

        # '클럽' 카테고리 처리 (필요 시)
        club_path = os.path.join(data_dir, f"{loc}_클럽.csv")
        if os.path.exists(club_path):
            cdf = pd.read_csv(club_path, encoding="utf-8-sig")
            c1 = cdf[cdf["str_sub_category"].str.contains("나이트,클럽", na=False)]

            # c1 데이터 저장 (카테고리 필터만 적용)
            if not c1.empty:
                # region 필드 추가
                c1["region"] = loc

                # 검색 URL 추가
                c1["search_url"] = c1["name"].apply(
                    lambda x: f"https://map.kakao.com/?q={loc} {x}"
                )

                # detail_url과 phone 정보 크롤링 준비
                urls = [""] * len(c1)
                phones = [""] * len(c1)

                # 병렬 크롤링 실행
                with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
                    future_to_idx = {
                        executor.submit(crawl, idx, row["name"]): idx
                        for idx, row in c1.iterrows()
                    }
                    for future in as_completed(future_to_idx):
                        idx = future_to_idx[future]
                        try:
                            _, du, ph = future.result()
                            urls[idx] = du
                            phones[idx] = ph
                        except:
                            pass

                # 필드 추가
                c1["detail_url"] = urls
                c1["phone"] = phones

                # 저장
                c1_path = os.path.join(club_dir, f"{loc}_클럽_cat_only.csv")
                c1.fillna("-1").to_csv(c1_path, index=False, encoding="utf-8-sig")
                logging.info(
                    f"{loc} 클럽 카테고리 필터링만 적용된 데이터 저장: {len(c1)}개 → {c1_path}"
                )

            c2 = filter_by_address(c1, loc)
            c3 = filter_by_opening_hours(c2)
            c4 = filter_by_reviews(c3)
            if not c4.empty:
                c4 = c4.reset_index(drop=True)
                c4["search_url"] = c4["name"].apply(lambda x: f"{loc} {x}")

                urls = [""] * len(c4)
                phones = [""] * len(c4)

                with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
                    future_to_idx = {
                        executor.submit(crawl, idx, row["name"]): idx
                        for idx, row in c4.iterrows()
                    }
                    for future in as_completed(future_to_idx):
                        idx = future_to_idx[future]
                        try:
                            _, du, ph = future.result()
                            urls[idx] = du
                            phones[idx] = ph
                        except:
                            pass

                c4["detail_url"] = urls
                c4["phone"] = phones

                spath = os.path.join(club_dir, f"{loc}_클럽_filtered.csv")
                c4.fillna("-1").to_csv(spath, index=False, encoding="utf-8-sig")
                all_filtered.append(c4)
            else:
                empty_cats.append(f"{loc}_클럽")

    # 드라이버 풀 정리
    while not driver_pool.empty():
        drv = driver_pool.get()
        drv.quit()

    # 개별 파일 결측값 채우고 통합하기
    merge_and_fill_filtered_data()

    # 추가 안전장치: 메모리상 리스트로 모은 f3들도 합쳐서 저장
    if all_filtered:
        big = pd.concat(all_filtered, ignore_index=True)
        big.to_csv(
            os.path.join(all_dir, "all_filtered_data.csv"),
            index=False,
            encoding="utf-8-sig",
        )

    if empty_cats:
        logging.warning(f"필터 결과 없음: {empty_cats}")


if __name__ == "__main__":
    process_all_locations()
