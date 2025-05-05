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
        "서초대로 73길", "서초대로 75길", "서초대로 77길",
        "테헤란로 1길", "테헤란로 5길", "강남대로 102길", "강남대로 106길", "강남대로 110길",
        "서초대로 74길", "서초대로 78길",
        "테헤란로 2길", "테헤란로 4길", "테헤란로 6길", "테헤란로 8길",
    ],
    "홍대입구역": [
        "동교로 27길", "연남로3길", "연남로 5길", "월드컵북로 2길", "월드컵북로 6길",
        "동교로 30길", "동교로 32길", "동교로 34길", "동교로 36길", "동교로 38길", "동교로 46길", "연희로 1길",
        "홍익로 2길", "홍익로 4길", "와우산로 23길", "와우산로 25길", "와우산로 27길", "와우산로 29길", "와우산로 33길", "와우산로 35길",
        "신촌로 2안길", "신촌로 4길", "신촌로 6길", "신촌로 10길", "신촌로 12길", "와우산로 32길", "와우산로 37길",
    ],
    "성수역": [
        "아차산로 5길", "아차산로 7길", "성수일로 8길", "성수일로 10길", "성수일로 12길",
        "아차산로 9길", "아차산로 11길", "아차산로 13길", "아차산로 15길",
        "성수이로 7길", "성수이로 7가길", "연무장 5길", "연무장 5가길", "연무장 7길", "연무장 7가길", "연무장 9길",
        "성수이로 18길", "성수이로 20길", "연무장 13길", "연무장 15길",
    ],
    "압구정역": [
        "논현로 157길", "논현로 161길", "논현로 163길", "논현로 167길", "논현로 175길", "압구정로 20길", "압구정로 28길",
        "논현로 172길", "논현로 176길", "압구정로 30길", "압구정로 32길", "압구정로 36길", "언주로 167길",
        "논현로 152길", "논현로 158길", "논현로 164길", "도산대로 33길", "도산대로 35길", "도산대로 37길",
        "압구정로 42길", "압구정로 46길", "압구정로 48길", "선릉로 155길", "선릉로 157길", "선릉로 161길", "도산대로 49길", "도산대로 51길", "도산대로 53길",
    ],
    "이태원역": [
        "이태원로 19길", "이태원로 23길", "이태원로 27가길",
        "이태원로 16길", "이태원로 20길", "이태원로 20가길", "이태원로 26길", "보광로 59길",
        "이태원로 45길", "이태원로 49길", "이태원로 55가길", "이태원로 55나길",
        "이태원로 42길", "이태원로 54길", "이태원로 54가길", "대사관로 5길",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. 주소 정규화
def normalize_address(address: str) -> str:
    if not isinstance(address, str):
        return ""
    address = address.lower()
    address = re.sub(r"\s+", "", address)
    address = re.sub(r"[^\w\s가-힣]", "", address)
    return address

# ─────────────────────────────────────────────────────────────────────────────
# 3. 주소 필터링 + 'region' 컬럼 추가
def filter_by_address(df: pd.DataFrame, location: str) -> pd.DataFrame:
    if location not in ADDRESS_FILTERS:
        logging.warning(f"⚠️ {location}에 대한 주소 필터가 정의되어 있지 않습니다.")
        return df
    df = df.copy()
    df["_addr_norm"] = df["address"].apply(normalize_address)
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
    pattern = re.compile(r"(\d{1,2}):(\d{2})\s*[~-]\s*(\d{1,2}):(\d{2})")
    rows = []
    for _, row in df.iterrows():
        hrs = row.get("hours", "")
        if not isinstance(hrs, str):
            continue
        m = pattern.search(hrs)
        if not m:
            continue
        sh, sm, eh, em = map(int, m.groups())
        if eh == 0:
            eh = 24
        if sh < eh:
            is_night = (sh >= 21 or eh <= 9)
        else:
            is_night = True
        if is_night:
            rows.append(row)
    return pd.DataFrame(rows, columns=df.columns)

# ─────────────────────────────────────────────────────────────────────────────
# 5. 리뷰 수 필터 (>0)
def filter_by_reviews(df: pd.DataFrame) -> pd.DataFrame:
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
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # options.add_argument("--headless")  # 필요 시 해제
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()
    return driver

def initialize_driver_pool():
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())

def get_driver():
    with driver_lock:
        return driver_pool.get()

def return_driver(driver):
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
            driver.execute_script("arguments[0].click();",
                                  it.find_element(By.CSS_SELECTOR, "a[data-id='moreview']"))
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
                    driver.execute_script("arguments[0].click();",
                                          it.find_element(By.CSS_SELECTOR, "a[data-id='moreview']"))
                    found = True
                    break
        except:
            pass
    if not found:
        return "", "-1"

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
        all_df.to_csv(os.path.join(dir4, "all_filtered_data.csv"),
                      index=False, encoding="utf-8-sig")

# ─────────────────────────────────────────────────────────────────────────────
# 9. 전체 처리 및 멀티스레드 크롤링
def process_all_locations():
    data_dir = "data/1_location_categories"
    out_dir  = "data/3_filtered_location_categories"
    all_dir  = "data/4_filtered_all"
    club_dir = "data/5_filtered_clubs"
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(all_dir, exist_ok=True)
    os.makedirs(club_dir, exist_ok=True)

    categories = ["식당", "카페", "술집", "노래방", "PC방", "볼링장", "당구장"]
    initialize_driver_pool()

    all_filtered = []
    empty_cats   = []

    def crawl(idx, store_name):
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
            # crawl 에는 오로지 ‘가게명’만 전달합니다.
            f3["search_url"] = f3["name"].apply(lambda x: f"https://map.kakao.com/?q={loc} {x}")

            urls   = [""] * len(f3)
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
                        urls[idx]   = du
                        phones[idx] = ph
                        logging.info(f"[{loc}-{cat}] ({idx+1}/{len(f3)}) 크롤링 완료")
                    except Exception as e:
                        logging.error(f"[{loc}-{cat}] 인덱스 {idx} 크롤링 오류: {e}")

            f3["detail_url"] = urls
            f3["phone"]      = phones

            save_path = os.path.join(out_dir, f"{loc}_{cat}_filtered.csv")
            f3.fillna("-1").to_csv(save_path, index=False, encoding="utf-8-sig")
            all_filtered.append(f3)

        # ‘클럽’ 카테고리 처리 (필요 시)
        club_path = os.path.join(data_dir, f"{loc}_클럽.csv")
        if os.path.exists(club_path):
            cdf = pd.read_csv(club_path, encoding="utf-8-sig")
            c1 = cdf[cdf["category"].str.contains("나이트,클럽", na=False)]
            c2 = filter_by_address(c1, loc)
            c3 = filter_by_opening_hours(c2)
            c4 = filter_by_reviews(c3)
            if not c4.empty:
                c4 = c4.reset_index(drop=True)
                c4["search_url"] = c4["name"].apply(lambda x: f"{loc} {x}")

                urls   = [""] * len(c4)
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
                            urls[idx]   = du
                            phones[idx] = ph
                        except:
                            pass

                c4["detail_url"] = urls
                c4["phone"]      = phones

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
        big.to_csv(os.path.join(all_dir, "all_filtered_data.csv"),
                   index=False, encoding="utf-8-sig")

    if empty_cats:
        logging.warning(f"필터 결과 없음: {empty_cats}")

if __name__ == "__main__":
    process_all_locations()
