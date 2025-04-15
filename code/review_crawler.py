import logging
import time
import os
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
import re

# 환경 변수 로드 및 로깅 설정
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 드라이버 풀 관련 전역 변수 설정
driver_pool = Queue()
MAX_DRIVERS = 4


def setup_driver():
    """셀레니움 드라이버 설정 함수"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # headless 모드를 사용하려면 아래 주석 해제 가능
    # options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()
    return driver


def initialize_driver_pool():
    """드라이버 풀 초기화"""
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())


def get_driver():
    """드라이버 풀에서 드라이버 가져오기"""
    return driver_pool.get()


def return_driver(driver):
    """드라이버를 풀에 반환"""
    driver_pool.put(driver)


def search_store_detail(driver, store_name):
    """
    1. 카카오맵 메인페이지에서 검색 input에 store_name 입력 후 검색 버튼 클릭
    2. placelist 내의 각 결과의 link_name과 store_name을 비교하여 일치하는 결과를 찾으면
       상세보기 버튼(data-id="moreview")을 클릭합니다.
       (1페이지 내 비교를 우선하며, 없으면 "장소 더보기" 버튼을 클릭하여 추가 결과를 확인)
    """
    logging.info(f"'{store_name}' 상세정보 검색 시작...")
    driver.get("https://map.kakao.com/")
    time.sleep(2)

    try:
        # 검색창에 가게 이름 입력
        search_input = driver.find_element(By.ID, "search.keyword.query")
        search_input.clear()
        search_input.send_keys(store_name)
        # 검색 버튼 클릭
        search_button = driver.find_element(By.ID, "search.keyword.submit")
        driver.execute_script("arguments[0].click();", search_button)
        time.sleep(2)
    except Exception as e:
        logging.error(f"검색 실행 중 오류: {e}")
        return False

    matched = False
    try:
        # 첫 페이지의 placelist 결과 확인
        results = driver.find_elements(By.CSS_SELECTOR, "ul.placelist li.PlaceItem")
        for result in results:
            try:
                link_name_elem = result.find_element(By.CSS_SELECTOR, "a.link_name")
                result_name = (
                    link_name_elem.get_attribute("title") or link_name_elem.text
                ).strip()
                if result_name == store_name:
                    logging.info(f"일치하는 가게 발견: {result_name}")
                    detail_btn = result.find_element(
                        By.CSS_SELECTOR, "a[data-id='moreview']"
                    )
                    driver.execute_script("arguments[0].click();", detail_btn)
                    time.sleep(2)

                    # 새로 열린 탭으로 전환
                    windows = driver.window_handles
                    if len(windows) > 1:
                        driver.switch_to.window(
                            windows[-1]
                        )  # 가장 최근에 열린 탭으로 전환
                        logging.info("새로 열린 탭으로 전환 완료")

                    matched = True
                    break
            except NoSuchElementException:
                continue

        # 1페이지에서 일치하는 결과가 없다면 "장소 더보기" 버튼 클릭 후 다시 확인 (단, 1페이지 내에서만 비교)
        if not matched:
            try:
                more_btn = driver.find_element(By.ID, "info.search.place.more")
                if more_btn.is_displayed():
                    logging.info("장소 더보기 버튼 클릭: 추가 결과 로드")
                    driver.execute_script("arguments[0].click();", more_btn)
                    time.sleep(2)
                    results = driver.find_elements(
                        By.CSS_SELECTOR, "ul.placelist li.PlaceItem"
                    )
                    for result in results:
                        try:
                            link_name_elem = result.find_element(
                                By.CSS_SELECTOR, "a.link_name"
                            )
                            result_name = (
                                link_name_elem.get_attribute("title")
                                or link_name_elem.text
                            ).strip()
                            if result_name == store_name:
                                logging.info(f"일치하는 가게 발견: {result_name}")
                                detail_btn = result.find_element(
                                    By.CSS_SELECTOR, "a[data-id='moreview']"
                                )
                                driver.execute_script(
                                    "arguments[0].click();", detail_btn
                                )
                                time.sleep(2)

                                # 새로 열린 탭으로 전환
                                windows = driver.window_handles
                                if len(windows) > 1:
                                    driver.switch_to.window(
                                        windows[-1]
                                    )  # 가장 최근에 열린 탭으로 전환
                                    logging.info("새로 열린 탭으로 전환 완료")

                                matched = True
                                break
                        except NoSuchElementException:
                            continue
            except NoSuchElementException:
                logging.info("장소 더보기 버튼이 존재하지 않음")
        if not matched:
            logging.warning(
                f"검색 결과에서 '{store_name}'과(와) 일치하는 가게를 찾지 못함"
            )
            return False

        # 상세보기 버튼 클릭 후 후기 탭으로 이동
        time.sleep(3)  # 페이지 로딩을 위한 대기 시간 추가
        try:
            # 후기 탭 버튼 찾기 및 클릭
            review_tab = driver.find_element(By.CSS_SELECTOR, "a[href*='#comment']")
            driver.execute_script("arguments[0].click();", review_tab)
            time.sleep(3)  # 후기 탭 로딩을 위한 대기 시간
            return True
        except Exception as e:
            logging.error(f"후기 탭으로 이동 중 오류: {e}")
            return False
    except Exception as e:
        logging.error(f"가게 상세 정보 검색 중 오류: {e}")
        return False


def scroll_and_collect_reviews(driver, store_name, target_count=50, scroll_wait=1.5):
    """
    상세 페이지 내 리뷰 영역에서 리뷰들을 스크롤하며 수집합니다.
    - 수집된 리뷰 수가 target_count(50개)가 될 때까지 페이지를 아래로 스크롤합니다.
    - 만약 스크롤 후 추가 리뷰가 로드되지 않으면, 해당 가게 리뷰가 충분하지 않다는 로그를 출력합니다.
    """
    reviews = []
    last_count = 0
    max_scroll_attempts = 5
    scroll_attempt = 0

    while len(reviews) < target_count and scroll_attempt < max_scroll_attempts:
        try:
            # 리뷰 요소 찾기
            review_elements = driver.find_elements(
                By.CSS_SELECTOR, "div.wrap_review a.link_review p.desc_review"
            )

            # 새로운 리뷰가 로드되었는지 확인
            if len(review_elements) > last_count:
                for review_elem in review_elements[last_count:]:
                    try:
                        review_text = review_elem.text.strip()
                        if review_text and review_text not in reviews:
                            reviews.append(review_text)
                            logging.info(
                                f"[{store_name}] 리뷰 수집: {len(reviews)}/{target_count}"
                            )
                    except Exception as e:
                        logging.warning(
                            f"[{store_name}] 리뷰 텍스트 추출 중 오류: {str(e)}"
                        )
                        continue

                last_count = len(review_elements)
                scroll_attempt = 0  # 새로운 리뷰가 로드되면 스크롤 시도 횟수 초기화
            else:
                scroll_attempt += 1
                logging.info(
                    f"[{store_name}] 새로운 리뷰 로드 시도 {scroll_attempt}/{max_scroll_attempts}"
                )

            # 페이지 스크롤
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_wait)

        except Exception as e:
            logging.error(f"[{store_name}] 리뷰 수집 중 오류: {str(e)}")
            break

    if len(reviews) < target_count:
        logging.warning(
            f"[{store_name}] 목표 리뷰 수({target_count}개)에 도달하지 못했습니다. 수집된 리뷰: {len(reviews)}개"
        )

    return reviews


def process_store_reviews(store_record):
    """
    한 가게의 리뷰를 수집하는 함수
    store_record: DataFrame의 행(Series)로 { 'name': 가게명, 'address': 주소, ... } 포함
    """
    store_name = store_record["name"]
    store_address = store_record.get("address", "")
    driver = get_driver()
    collected_reviews = []

    try:
        if not search_store_detail(driver, store_name):
            logging.warning(
                f"[{store_name}] 상세 페이지 진입 실패: 가게 검색 또는 상세 페이지 이동 중 오류"
            )
            return pd.DataFrame(), False

        # 상세 페이지 진입 후, URL에 "#comment"가 있는 상태에서 후기 영역에서 리뷰 수집
        review_texts = scroll_and_collect_reviews(
            driver, store_name, target_count=50, scroll_wait=1.5
        )

        # 리뷰 텍스트를 딕셔너리 형태로 변환
        for review_text in review_texts:
            review_dict = {
                "store_name": store_name,
                "store_address": store_address,
                "review_text": review_text,
            }
            collected_reviews.append(review_dict)

        df = pd.DataFrame(collected_reviews)
        return df, True
    except Exception as e:
        if "invalid session id" in str(e):
            logging.error(
                f"[{store_name}] 세션 오류 발생: 브라우저 세션이 만료되었거나 연결이 끊어짐"
            )
            return pd.DataFrame(), False
        elif "no such element" in str(e).lower():
            logging.error(f"[{store_name}] 요소를 찾을 수 없음: {str(e)}")
            return pd.DataFrame(), False
        elif "timeout" in str(e).lower():
            logging.error(f"[{store_name}] 페이지 로딩 시간 초과: {str(e)}")
            return pd.DataFrame(), False
        elif "stale element" in str(e).lower():
            logging.error(f"[{store_name}] 페이지가 새로고침되거나 변경됨: {str(e)}")
            return pd.DataFrame(), False
        else:
            logging.error(
                f"[{store_name}] 리뷰 수집 중 예상치 못한 오류 발생: {str(e)}"
            )
            return pd.DataFrame(), False
    finally:
        return_driver(driver)


def main():
    """
    메인 함수:
    1. data/4_filtered_all/all_filtered_data.csv 파일을 로드하여 각 가게에 대해 리뷰 50개씩을 수집
    2. 수집한 리뷰 데이터에 가게의 name, address 정보를 매핑하고
    3. data/6_reviews_about_4 폴더에 CSV 파일로 저장
    """
    filtered_data_path = "data/4_filtered_all/all_filtered_data.csv"
    if not os.path.exists(filtered_data_path):
        logging.error(f"가게 데이터 파일을 찾을 수 없습니다: {filtered_data_path}")
        return

    all_filtered_data = pd.read_csv(filtered_data_path)
    review_dfs = []
    failed_stores = []

    initialize_driver_pool()

    for idx, store_row in all_filtered_data.iterrows():
        store_name = store_row["name"]
        logging.info(f"=== '{store_name}' 리뷰 수집 시작 ===")
        df_reviews, success = process_store_reviews(store_row)

        if not success:
            failed_stores.append(store_name)
            continue

        if df_reviews.empty:
            logging.warning(f"[{store_name}] 리뷰 수집 결과 없음")
            failed_stores.append(store_name)
        else:
            logging.info(f"[{store_name}] {len(df_reviews)}개의 리뷰 수집")
            review_dfs.append(df_reviews)

    if review_dfs:
        all_reviews_df = pd.concat(review_dfs, ignore_index=True)
        output_dir = "data/6_reviews_about_4"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "kakao_map_reviews.csv")
        all_reviews_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logging.info(
            f"전체 리뷰 저장 완료: {len(all_reviews_df)}개의 리뷰, 파일: {output_path}"
        )

        if failed_stores:
            failed_stores_path = os.path.join(output_dir, "failed_stores.txt")
            with open(failed_stores_path, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_stores))
            logging.info(
                f"실패한 매장 목록 저장 완료: {len(failed_stores)}개, 파일: {failed_stores_path}"
            )
    else:
        logging.warning("수집된 리뷰가 없습니다.")

    while not driver_pool.empty():
        driver = driver_pool.get()
        driver.quit()


if __name__ == "__main__":
    main()
