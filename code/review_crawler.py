import logging
import time
import os
import csv
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

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
driver_lock = Lock()  # 드라이버 풀 접근용 락


def setup_driver():
    """셀레니움 드라이버 설정 함수"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # headless 모드 사용 시 아래 주석 해제
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
    """드라이버 풀에서 드라이버 가져오기 (락 적용)"""
    with driver_lock:
        return driver_pool.get()


def return_driver(driver):
    """드라이버를 풀에 반환 (유효성 체크 후)"""
    try:
        driver.current_url
        with driver_lock:
            driver_pool.put(driver)
    except Exception:
        try:
            driver.quit()
        except Exception:
            pass
        with driver_lock:
            driver_pool.put(setup_driver())


def search_store_detail(driver, store_name):
    """
    1. 카카오맵 메인페이지에서 검색창에 store_name 입력 후 검색 버튼 클릭
    2. placelist 내 각 결과의 link_name(클래스 "a.link_name", title 속성 또는 내부 텍스트)과 store_name을 비교하여
       일치하는 경우 상세보기 버튼(data-id="moreview")을 클릭해 상세 페이지(후기 탭 포함)로 이동
    """
    logging.info(f"'{store_name}' 상세정보 검색 시작...")
    driver.get("https://map.kakao.com/")
    time.sleep(2)

    try:
        search_input = driver.find_element(By.ID, "search.keyword.query")
        search_input.clear()
        search_input.send_keys(store_name)
        search_button = driver.find_element(By.ID, "search.keyword.submit")
        driver.execute_script("arguments[0].click();", search_button)
        time.sleep(2)
    except Exception as e:
        logging.error(f"검색 실행 중 오류: {e}")
        return False

    matched = False
    try:
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
                    windows = driver.window_handles
                    if len(windows) > 1:
                        driver.switch_to.window(windows[-1])
                        logging.info("새로 열린 탭으로 전환 완료")
                    matched = True
                    break
            except NoSuchElementException:
                continue
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
                                windows = driver.window_handles
                                if len(windows) > 1:
                                    driver.switch_to.window(windows[-1])
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

        time.sleep(3)
        try:
            review_tab = driver.find_element(By.CSS_SELECTOR, "a[href*='#comment']")
            driver.execute_script("arguments[0].click();", review_tab)
            time.sleep(3)
            return True
        except Exception as e:
            logging.error(f"후기 탭으로 이동 중 오류: {e}")
            return False
    except Exception as e:
        logging.error(f"가게 상세 정보 검색 중 오류: {e}")
        return False


def scroll_and_collect_reviews(driver, store_name, target_count=50, scroll_wait=2.0):
    """
    상세 페이지 내 후기 영역에서 리뷰 항목(div.inner_review)을 반복 처리하여
    사용자 평점(별점은 <span class="figure_star on"> 개수로 계산), 작성일, 리뷰 내용, 그리고 사용자(리뷰어) 이름 등을 추출합니다.
    반환되는 각 리뷰 항목은 다음 딕셔너리 형태입니다.
      {
        "reviewer_name": <텍스트 또는 빈 문자열>,
        "user_rating": <평점 (float)>,
        "review_date": <작성일 (str)>,
        "review_content": <리뷰 전체 텍스트 (str)>
      }
    """
    reviews = []
    last_count = 0
    max_scroll_attempts = 5
    scroll_attempt = 0

    while len(reviews) < target_count and scroll_attempt < max_scroll_attempts:
        try:
            review_containers = driver.find_elements(
                By.CSS_SELECTOR, "div.inner_review"
            )
            if not review_containers:
                logging.warning(f"[{store_name}] 리뷰 컨테이너를 찾을 수 없습니다.")
                break

            if len(review_containers) == last_count:
                scroll_attempt += 1
                logging.info(
                    f"[{store_name}] 새로운 리뷰 로드 시도 {scroll_attempt}/{max_scroll_attempts}"
                )
            else:
                scroll_attempt = 0
                last_count = len(review_containers)

            for container in review_containers[len(reviews) :]:
                try:
                    # 리뷰 내용 추출: p.desc_review에서 더보기 버튼 클릭 후 0.5초 대기
                    try:
                        content_elem = container.find_element(
                            By.CSS_SELECTOR,
                            "div.review_detail div.wrap_review a.link_review p.desc_review",
                        )
                        try:
                            btn_more = content_elem.find_element(
                                By.CSS_SELECTOR, "span.btn_more"
                            )
                            if btn_more.is_displayed():
                                driver.execute_script("arguments[0].click();", btn_more)
                                time.sleep(0.5)
                        except NoSuchElementException:
                            pass
                        review_content = (
                            content_elem.text.strip()
                            .replace("더보기", "")
                            .replace("접기", "")
                            .replace("\n", " ")
                            .strip()
                        )
                    except Exception as ce:
                        review_content = ""

                    # 평점 추출: <span class="figure_star on"> 개수로 계산
                    try:
                        star_elements = container.find_elements(
                            By.CSS_SELECTOR,
                            "div.review_detail div.info_grade span.starred_grade span.wrap_grade span.figure_star.on",
                        )
                        user_rating = float(len(star_elements))
                    except Exception:
                        user_rating = 0.0

                    # 작성일 추출
                    try:
                        date_elem = container.find_element(
                            By.CSS_SELECTOR,
                            "div.review_detail div.info_grade span.txt_date",
                        )
                        review_date = date_elem.text.strip()
                    except NoSuchElementException:
                        review_date = ""

                    # 리뷰어 이름 추출: 구체적인 선택자 사용 (div.info_user > div.wrap_user > a.link_user > span.name_user)
                    try:
                        reviewer_elem = container.find_element(
                            By.CSS_SELECTOR,
                            "div.info_user > div.wrap_user > a.link_user > span.name_user",
                        )
                        reviewer_name = reviewer_elem.text.strip()
                    except NoSuchElementException:
                        reviewer_name = ""

                    reviews.append(
                        {
                            "reviewer_name": reviewer_name,
                            "user_rating": user_rating,
                            "review_date": review_date,
                            "review_content": review_content,
                        }
                    )
                    logging.info(
                        f"[{store_name}] 리뷰 수집: {len(reviews)}/{target_count}"
                    )
                    if len(reviews) >= target_count:
                        break
                except Exception as inner_e:
                    logging.warning(f"[{store_name}] 리뷰 정보 추출 중 오류: {inner_e}")
                    continue

            if len(reviews) >= target_count:
                break

            driver.execute_script(
                "arguments[0].scrollIntoView(true);", review_containers[-1]
            )
            time.sleep(scroll_wait)
        except Exception as e:
            logging.error(f"[{store_name}] 리뷰 수집 중 오류: {e}")
            break

    if len(reviews) < target_count:
        logging.warning(
            f"[{store_name}] 목표 리뷰 수({target_count}개)에 도달하지 못했습니다. (수집된 리뷰: {len(reviews)}개)"
        )
    return reviews


def process_store_reviews(store_record):
    """
    한 가게의 리뷰를 수집하는 함수
    store_record: DataFrame의 행(Series)로 { 'name': 가게명, 'address': 주소, ... } 포함
    반환: 리뷰 DataFrame, 성공 여부 (True/False)
    """
    store_name = store_record["name"]
    store_address = store_record.get("address", "")
    driver = None
    collected_reviews = []

    try:
        driver = get_driver()
        if not search_store_detail(driver, store_name):
            logging.warning(
                f"[{store_name}] 상세 페이지 진입 실패: 가게 검색 또는 상세 페이지 이동 중 오류"
            )
            return pd.DataFrame(), False

        reviews = scroll_and_collect_reviews(
            driver, store_name, target_count=50, scroll_wait=2.0
        )

        # 각 리뷰에 가게 정보 추가 및 CSV 칼럼 순서 맞춤
        for review in reviews:
            review["store_name"] = store_name
            review["store_address"] = store_address
            collected_reviews.append(
                {
                    "store_name": review["store_name"],
                    "store_address": review["store_address"],
                    "user_name": review.get("reviewer_name", ""),
                    "user_rating": review.get("user_rating", 0.0),
                    "review_date": review.get("review_date", ""),
                    "review_content": review.get("review_content", ""),
                }
            )

        df = pd.DataFrame(
            collected_reviews,
            columns=[
                "store_name",
                "store_address",
                "user_name",
                "user_rating",
                "review_date",
                "review_content",
            ],
        )
        return df, True
    except Exception as e:
        err_str = str(e).lower()
        if "invalid session id" in err_str:
            logging.error(
                f"[{store_name}] 세션 오류 발생: 브라우저 세션이 만료되었거나 연결이 끊어짐"
            )
        elif "no such element" in err_str:
            logging.error(f"[{store_name}] 요소를 찾을 수 없음: {e}")
        elif "timeout" in err_str:
            logging.error(f"[{store_name}] 페이지 로딩 시간 초과: {e}")
        elif "stale element" in err_str:
            logging.error(f"[{store_name}] 페이지 변경됨: {e}")
        else:
            logging.error(f"[{store_name}] 리뷰 수집 중 예상치 못한 오류 발생: {e}")
        return pd.DataFrame(), False
    finally:
        if driver:
            try:
                driver.close()
                if driver.window_handles:
                    driver.switch_to.window(driver.window_handles[0])
            except Exception as e:
                logging.error(f"[{store_name}] 탭 닫기 중 오류 발생: {e}")
            return_driver(driver)


def main():
    """
    메인 함수:
    1. data/4_filtered_all/all_filtered_data.csv 파일을 로드하여 각 가게에 대해 리뷰 50개씩 수집
    2. 수집된 리뷰에 가게 정보(name, address) 추가 후 data/6_reviews_about_4 폴더에 두 개의 CSV 파일로 저장
       - 모든 리뷰 데이터 파일 (empty review_content 포함): kakao_map_reviews_all.csv
       - 리뷰 내용이 비어있지 않은 행만 필터링한 파일: kakao_map_reviews_filtered.csv
       CSV 칼럼 순서는 store_name, store_address, user_name, user_rating, review_date, review_content 입니다.

    ※ 전체 데이터에 대해 실행합니다.
    """
    filtered_data_path = "data/4_filtered_all/all_filtered_data.csv"
    if not os.path.exists(filtered_data_path):
        logging.error(f"가게 데이터 파일을 찾을 수 없습니다: {filtered_data_path}")
        return

    # 전체 데이터를 대상으로 실행
    all_filtered_data = pd.read_csv(filtered_data_path)
    stores_data = all_filtered_data

    review_dfs = []
    failed_stores = []
    review_lock = Lock()

    initialize_driver_pool()

    def process_store_with_lock(store_row):
        store_name = store_row["name"]
        logging.info(f"=== '{store_name}' 리뷰 수집 시작 ===")
        df_reviews, success = process_store_reviews(store_row)
        if not success or df_reviews.empty:
            with review_lock:
                failed_stores.append(store_name)
            return None
        else:
            logging.info(f"[{store_name}] {len(df_reviews)}개의 리뷰 수집")
            return df_reviews

    with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
        future_to_store = {
            executor.submit(process_store_with_lock, store_row): store_row["name"]
            for _, store_row in stores_data.iterrows()
        }
        for future in as_completed(future_to_store):
            store_name = future_to_store[future]
            try:
                df = future.result()
                if df is not None:
                    with review_lock:
                        review_dfs.append(df)
            except Exception as e:
                logging.error(f"[{store_name}] 처리 중 오류 발생: {e}")
                with review_lock:
                    failed_stores.append(store_name)

    if review_dfs:
        all_reviews_df = pd.concat(review_dfs, ignore_index=True)
        output_dir = "data/6_reviews_about_4"
        os.makedirs(output_dir, exist_ok=True)

        # 전체 데이터를 저장하는 파일
        output_path_all = os.path.join(output_dir, "kakao_map_reviews_all.csv")
        all_reviews_df.to_csv(
            output_path_all, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL
        )
        logging.info(
            f"전체 리뷰 저장 완료: {len(all_reviews_df)}개의 리뷰, 파일: {output_path_all}"
        )

        # 리뷰 내용(review_content)이 비어있는 행은 제거한 파일 (빈 리뷰도 포함시킨 전체 파일과 별도로 저장)
        filtered_df = all_reviews_df[all_reviews_df["review_content"].str.strip() != ""]
        output_path_filtered = os.path.join(
            output_dir, "kakao_map_reviews_filtered.csv"
        )
        filtered_df.to_csv(
            output_path_filtered,
            index=False,
            encoding="utf-8-sig",
            quoting=csv.QUOTE_ALL,
        )
        logging.info(
            f"리뷰 내용이 있는 리뷰 저장 완료: {len(filtered_df)}개의 리뷰, 파일: {output_path_filtered}"
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
