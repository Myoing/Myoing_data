"""
리뷰 크롤러 모듈

- 가게별 상세 페이지에서 후기 탭 진입 후
  리뷰 내용을 스크롤하여 일정 개수 추출 기능 모듈
"""

import os
import csv
import time
import logging
import pandas as pd

from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

from crawler.detail_crawler import search_store_detail
from crawler.config import MAX_DRIVERS, DATA_DIR_4, DATA_DIR_6

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────────────────────
# 드라이버풀 변수
driver_pool = Queue()
DRIVER_LOCK = Lock()

def setup_driver() -> webdriver.Chrome:
    """
    웹드라이버 설정 및 반환

    - 알림 비활성화 옵션 설정
    - ChromeDriverManager로 드라이버 설치
    - 창 최소화
    """
    opts = webdriver.ChromeOptions()
    opts.add_argument("--disable-notifications")
    # opts.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    d = webdriver.Chrome(service=service, options=opts)
    d.minimize_window()
    return d

def initialize_driver_pool() -> None:
    """
    드라이버풀 초기화

    - MAX_DRIVERS 수만큼 setup_driver() 실행 후 등록
    """
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())

def get_driver() -> webdriver.Chrome:
    """
    드라이버 획득

    - DRIVER_LOCK으로 동시 접근 제어
    """
    with DRIVER_LOCK:
        return driver_pool.get()

def return_driver(drv: webdriver.Chrome) -> None:
    """
    드라이버 반환

    - 세션 유효 시 재등록, 예외 시 재생성 후 등록
    """
    try:
        _ = drv.current_url
        with DRIVER_LOCK:
            driver_pool.put(drv)
    except Exception:
        try:
            drv.quit()
        except:
            pass
        with DRIVER_LOCK:
            driver_pool.put(setup_driver())

def search_store_detail_for_review(driver: webdriver.Chrome, store_name: str) -> bool:
    """
    상세 페이지 후기 탭 진입

    - search_store_detail로 기본 상세 페이지 오픈
    - 후기 탭 클릭 시 True, 실패 시 False
    """
    url, _ = search_store_detail(driver, store_name)
    if not url:
        return False
    try:
        tab = driver.find_element(By.CSS_SELECTOR, "a[href*='#comment']")
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(1.5)
        return True
    except Exception as e:
        logging.error(f"[{store_name}] 후기 탭 진입 오류: {e}")
        return False

def scroll_and_collect_reviews(driver: webdriver.Chrome, store_name: str,
                               target_count: int = 50, scroll_wait: float = 1.5) -> list[dict]:
    """
    후기 스크롤 수집

    - 최대 target_count 리뷰 수집, max 5회 추가 로딩 시도
    - 평점(star count), 작성일, 내용, 리뷰어명 추출
    """
    reviews = []
    last_count = 0
    attempts = 0
    while len(reviews) < target_count and attempts < 5:
        elems = driver.find_elements(By.CSS_SELECTOR, "div.inner_review")
        if not elems:
            logging.warning(f"[{store_name}] 리뷰 컨테이너 미발견")
            break

        if len(elems) == last_count:
            attempts += 1
            logging.info(f"[{store_name}] 추가 로드 시도 {attempts}/5")
        else:
            last_count = len(elems)
            attempts = 0

        for ele in elems[len(reviews):]:
            try:
                # 내용 추출
                try:
                    cont = ele.find_element(By.CSS_SELECTOR, "p.desc_review")
                    more = cont.find_element(By.CSS_SELECTOR, "span.btn_more")
                    if more.is_displayed():
                        driver.execute_script("arguments[0].click();", more)
                        time.sleep(0.3)
                    content = cont.text.replace("더보기", "").replace("접기", "").strip()
                except Exception:
                    content = ""

                # 평점 추출
                stars = ele.find_elements(By.CSS_SELECTOR, "span.figure_star.on")
                rating = float(len(stars)) if stars else 0.0

                # 작성일 추출
                try:
                    date = ele.find_element(By.CSS_SELECTOR, "span.txt_date").text.strip()
                except NoSuchElementException:
                    date = ""

                # 리뷰어명 추출
                try:
                    user = ele.find_element(By.CSS_SELECTOR, "span.name_user").text.strip()
                except NoSuchElementException:
                    user = ""

                reviews.append({
                    "reviewer_name": user,
                    "user_rating": rating,
                    "review_date": date,
                    "review_content": content,
                })
                if len(reviews) >= target_count:
                    break
            except Exception as ex:
                logging.warning(f"[{store_name}] 리뷰 추출 오류: {ex}")
                continue

        if len(reviews) < target_count:
            driver.execute_script("arguments[0].scrollIntoView(true);", elems[-1])
            time.sleep(scroll_wait)

    if len(reviews) < target_count:
        logging.warning(f"[{store_name}] 목표 리뷰 미달 ({len(reviews)}/{target_count})")
    return reviews

def process_store_reviews(store_record: pd.Series) -> tuple[pd.DataFrame, bool]:
    """
    한 가게 리뷰 수집 및 DataFrame 반환

    - store_record: 'name', 'address' 컬럼 포함된 시리즈
    - 리뷰 50개 수집 후 CSV 칼럼 순서에 맞춰 DataFrame 생성
    """
    name = store_record["name"]
    addr = store_record.get("address", "")
    drv = None
    collected = []
    try:
        drv = get_driver()
        if not search_store_detail_for_review(drv, name):
            logging.warning(f"[{name}] 상세 페이지 진입 실패")
            return pd.DataFrame(), False

        revs = scroll_and_collect_reviews(drv, name)
        for rv in revs:
            rv["store_name"] = name
            rv["store_address"] = addr
            collected.append({
                "store_name": rv["store_name"],
                "store_address": rv["store_address"],
                "user_name": rv["reviewer_name"],
                "user_rating": rv["user_rating"],
                "review_date": rv["review_date"],
                "review_content": rv["review_content"],
            })
        df = pd.DataFrame(collected, columns=[
            "store_name", "store_address", "user_name",
            "user_rating", "review_date", "review_content"
        ])
        return df, True
    except Exception as e:
        err = str(e).lower()
        if "invalid session id" in err:
            logging.error(f"[{name}] 세션 오류: 세션 만료 또는 연결 끊김")
        else:
            logging.error(f"[{name}] 리뷰 수집 중 오류: {e}")
        return pd.DataFrame(), False
    finally:
        if drv:
            try:
                drv.close()
                if drv.window_handles:
                    drv.switch_to.window(drv.window_handles[0])
            except:
                pass
            return_driver(drv)

def main():
    """
    리뷰 크롤러 메인 실행

    1. data/4_filtered_all/all_filtered_data.csv 로드  
    2. ThreadPoolExecutor로 각 가게 process_store_reviews 병렬 실행  
    3. 전체 리뷰(all) 및 내용 있는 리뷰(filtered) CSV 저장
    """
    filtered_path = os.path.join(DATA_DIR_4, "all_filtered_data.csv")
    if not os.path.exists(filtered_path):
        logging.error(f"가게 데이터 파일 미발견: {filtered_path}")
        return

    df_all = pd.read_csv(filtered_path)
    initialize_driver_pool()

    results = []
    failed = []
    lock = Lock()

    def worker(row):
        df_rev, ok = process_store_reviews(row)
        if ok and not df_rev.empty:
            with lock:
                results.append(df_rev)
        else:
            with lock:
                failed.append(row["name"])

    with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as exe:
        for _, row in df_all.iterrows():
            exe.submit(worker, row)

    if results:
        all_rev = pd.concat(results, ignore_index=True)
        os.makedirs(DATA_DIR_6, exist_ok=True)
        # 전체 리뷰
        all_path = os.path.join(DATA_DIR_6, "kakao_map_reviews_all.csv")
        all_rev.to_csv(all_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
        logging.info(f"전체 리뷰 저장: {len(all_rev)}개 → {all_path}")
        # 내용 있는 리뷰만
        filt = all_rev[all_rev["review_content"].str.strip() != ""]
        filt_path = os.path.join(DATA_DIR_6, "kakao_map_reviews_filtered.csv")
        filt.to_csv(filt_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
        logging.info(f"내용 있는 리뷰 저장: {len(filt)}개 → {filt_path}")
        # 실패 목록
        if failed:
            fail_path = os.path.join(DATA_DIR_6, "failed_stores.txt")
            with open(fail_path, "w", encoding="utf-8") as f:
                f.write("\n".join(failed))
            logging.info(f"실패 매장 저장: {len(failed)}개 → {fail_path}")
    else:
        logging.warning("수집된 리뷰 없음")

    # 드라이버 풀 종료
    while not driver_pool.empty():
        driver_pool.get().quit()

if __name__ == "__main__":
    main()
