"""
카카오맵 기본 크롤러 모듈

- 지역+카테고리 검색  
- 매장 기본 정보(이름, 카테고리, 평점, 리뷰수, 주소, 영업시간) 추출  
- 페이지 네비게이션 및 ‘장소 더보기’ 처리  
- 1~10페이지: 모든 매장 수집  
- 11~20페이지: 리뷰 있는 매장만 수집 (리뷰 50개 모이면 즉시 종료)  
- 지역+카테고리별 CSV 저장
"""

import os
import re
import time
import logging
import pandas as pd

from queue import Queue
from threading import Lock
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─────────────────────────────────────────────────────────────────────────────
# 드라이버풀 변수
driver_pool = Queue()               # 사용 가능한 WebDriver 인스턴스 큐
DRIVER_LOCK = Lock()                # 큐 접근 동기화 락
MAX_DRIVERS = 4                     # 최대 드라이버풀 크기

# ─────────────────────────────────────────────────────────────────────────────
def setup_driver() -> webdriver.Chrome:
    """
    웹드라이버 설정 및 반환

    - ChromeOptions에 알림 비활성화 옵션 적용  
    - ChromeDriverManager를 통해 드라이버 설치  
    - 브라우저를 최소화하여 메모리 사용량 절감
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # options.add_argument("--headless")  # 필요 시 해드리스 모드 활성화
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()
    return driver

def initialize_driver_pool() -> None:
    """
    드라이버풀 초기화

    - MAX_DRIVERS 수만큼 setup_driver() 실행  
    - 생성된 WebDriver들을 driver_pool 큐에 등록
    """
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())

def get_driver() -> webdriver.Chrome:
    """
    드라이버 획득

    - driver_pool 큐에서 WebDriver 인스턴스를 안전하게 꺼내 반환
    """
    with DRIVER_LOCK:
        return driver_pool.get()

def return_driver(driver: webdriver.Chrome) -> None:
    """
    드라이버 반환

    - 세션이 살아있으면 driver_pool에 재삽입  
    - 예외 발생 시 세션을 종료하고 새로 생성 후 큐에 삽입
    """
    try:
        _ = driver.current_url
        with DRIVER_LOCK:
            driver_pool.put(driver)
    except Exception:
        try:
            driver.quit()
        except:
            pass
        with DRIVER_LOCK:
            driver_pool.put(setup_driver())

# ─────────────────────────────────────────────────────────────────────────────
def search_places(driver: webdriver.Chrome, location: str, category: str) -> None:
    """
    카카오맵에서 지역+카테고리 검색 실행

    1. 메인페이지 오픈  
    2. 검색창에 '{location} {category}' 입력  
    3. 검색 버튼 클릭(또는 ENTER)  
    4. 검색 결과 로딩 대기
    """
    logging.info(f"검색어 실행: '{location} {category}'")
    driver.get("https://map.kakao.com/")
    time.sleep(2)
    inp = driver.find_element(By.ID, "search.keyword.query")
    inp.clear()
    inp.send_keys(f"{location} {category}")
    try:
        btn = driver.find_element(By.ID, "search.keyword.submit")
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        inp.send_keys(Keys.RETURN)
    time.sleep(2)

def extract_store_info(elem) -> dict:
    """
    매장 기본 정보 추출

    - name: 가게명 텍스트  
    - category: 부가 카테고리 텍스트  
    - score_count: 평점 수량(정수)  
    - score: 평균 평점(실수, 없으면 -1.0)  
    - review_count: 리뷰 수량(정수)  
    - address: 도로명/지번 주소 텍스트  
    - hours: 영업시간 텍스트  
    """
    info = {}
    try:
        info["name"] = elem.find_element(By.CSS_SELECTOR, "a.link_name").text.strip()
    except NoSuchElementException:
        info["name"] = None
    try:
        info["category"] = elem.find_element(By.CSS_SELECTOR, "span.subcategory").text.strip()
    except NoSuchElementException:
        info["category"] = None
    try:
        sc_text = elem.find_element(By.CSS_SELECTOR, "a[data-id='numberofscore']").text
        sc_num = int(re.sub(r"[^0-9]", "", sc_text) or "0")
        info["score_count"] = sc_num
        info["score"] = float(elem.find_element(By.CSS_SELECTOR, "em[data-id='scoreNum']").text.strip()) if sc_num > 0 else -1.0
    except Exception:
        info["score_count"], info["score"] = 0, -1.0
    try:
        rv_text = elem.find_element(By.CSS_SELECTOR, "a[data-id='review'] em").text
        info["review_count"] = int(re.sub(r"[^0-9]", "", rv_text) or "0")
    except Exception:
        info["review_count"] = 0
    try:
        info["address"] = elem.find_element(By.CSS_SELECTOR, "p[data-id='address']").text.strip()
    except NoSuchElementException:
        info["address"] = None
    try:
        info["hours"] = elem.find_element(By.CSS_SELECTOR, "a[data-id='periodTxt']").text.strip()
    except NoSuchElementException:
        info["hours"] = None
    return info

def collect_all_stores(driver: webdriver.Chrome, max_pages: int = 20) -> list:
    """
    전체 매장 목록 수집 함수

    - 1~10페이지: 모든 매장 정보 수집  
    - 11~20페이지: review_count>0인 매장만 수집  
    - 리뷰 50개가 모이면 즉시 반환  
    - max_pages까지 수집 후 리뷰 50개 미만이면 경고 메시지 출력
    """
    results = []
    current_page = 1
    stores_with_reviews = 0

    # 현재 검색어 추출
    try:
        current_search = driver.find_element(By.ID, "search.keyword.query").get_attribute("value")
    except:
        current_search = "알 수 없음"

    while current_page <= max_pages:
        logging.info(f"[{current_search}] 페이지 {current_page} 수집 시작 (리뷰 있는 가게: {stores_with_reviews})")
        time.sleep(2)

        # 첫 페이지에서만 '장소 더보기' 클릭
        if current_page == 1:
            try:
                mb = driver.find_element(By.ID, "info.search.place.more")
                driver.execute_script("arguments[0].click();", mb)
                time.sleep(2)
            except NoSuchElementException:
                pass

        items = driver.find_elements(By.CSS_SELECTOR, "ul.placelist li.PlaceItem")
        if not items:
            logging.info(f"[{current_search}] 매장 정보 종료")
            break

        # 각 매장 정보 수집
        for it in items:
            info = extract_store_info(it)
            # 11~20페이지 구간에서는 리뷰 있는 매장만 수집
            if current_page > 10 and stores_with_reviews < 50:
                if info["review_count"] > 0:
                    results.append(info)
                    stores_with_reviews += 1
                    if stores_with_reviews >= 50:
                        logging.info(f"[{current_search}] 리뷰 50개 수집 완료, 즉시 종료")
                        return results
            else:
                results.append(info)
                if info["review_count"] > 0:
                    stores_with_reviews += 1

        # 10페이지까지 돌고 목표 미달 시 안내
        if current_page == 10:
            if stores_with_reviews >= 50:
                logging.info(f"[{current_search}] 10페이지 내 목표 달성, 종료")
                return results
            else:
                logging.info(f"[{current_search}] 10페이지 완료 (리뷰 {stores_with_reviews}개). 11~20페이지에서 리뷰 있는 가게만 추가 수집")

        # 페이지 이동 로직
        try:
            if current_page < max_pages:
                if current_page % 5 == 0:
                    nb = driver.find_element(By.ID, "info.search.page.next")
                    if nb.is_displayed() and "disabled" not in nb.get_attribute("class"):
                        driver.execute_script("arguments[0].click();", nb)
                        time.sleep(2)
                        current_page += 1
                        continue
                    else:
                        break
                else:
                    next_no = (current_page % 5) + 1
                    btn = driver.find_element(By.ID, f"info.search.page.no{next_no}")
                    if btn.is_displayed():
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
                        current_page += 1
                        continue
                    else:
                        break
            else:
                logging.info(f"[{current_search}] 최대 페이지({max_pages}) 도달")
                break
        except Exception as e:
            logging.error(f"[{current_search}] 페이지 이동 오류: {e}")
            break

    # max_pages까지 수집 후에도 리뷰 50개 미만일 경우 안내
    if stores_with_reviews < 50:
        logging.warning(
            f"[{current_search}] {max_pages}페이지 완료했으나 리뷰 가게 {stores_with_reviews}개 (목표 50개) → 다음 검색어로 넘어갑니다."
        )
    return results

def process_location_category(location: str, category: str) -> pd.DataFrame:
    """
    지역+카테고리 단위 데이터 수집 및 저장

    - get_driver()로 드라이버 획득  
    - search_places(), collect_all_stores() 순차 실행  
    - DataFrame 변환 및 중복 제거  
    - data/1_location_categories/{location}_{category}.csv 저장  
    """
    driver = get_driver()
    try:
        logging.info(f"== {location} - {category} 수집 시작 ==")
        search_places(driver, location, category)
        stores = collect_all_stores(driver, max_pages=20)
        df = pd.DataFrame(stores)
        if not df.empty and {"name", "address"}.issubset(df.columns):
            df = df.drop_duplicates(subset=["name", "address"])
        out_dir = "data/1_location_categories"
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{location}_{category}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logging.info(f"{location}-{category} 저장 완료 ({len(df)}개 → {path})")
        return df
    except Exception as e:
        logging.error(f"{location}-{category} 처리 오류: {e}")
        return pd.DataFrame()
    finally:
        return_driver(driver)
