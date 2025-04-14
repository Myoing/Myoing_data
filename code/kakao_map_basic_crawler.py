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
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import re

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 전역 변수로 드라이버 풀 관리
driver_pool = Queue()
MAX_DRIVERS = 4


def setup_driver():
    """셀레니움 드라이버를 설정하는 함수"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # 필요한 경우 headless 모드 사용 가능
    # options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()  # 창 최소화
    return driver


def initialize_driver_pool():
    """드라이버 풀 초기화"""
    for _ in range(MAX_DRIVERS):
        driver = setup_driver()
        driver_pool.put(driver)


def get_driver():
    """드라이버 풀에서 드라이버 가져오기"""
    return driver_pool.get()


def return_driver(driver):
    """드라이버를 풀에 반환"""
    driver_pool.put(driver)


def search_places(driver, location, category):
    """카카오맵에서 특정 지역과 카테고리로 검색하는 함수"""
    logging.info(f"'{location} {category}' 검색 중...")
    driver.get("https://map.kakao.com/")
    time.sleep(2)  # 기본 페이지 로딩 대기

    try:
        # 검색 입력창 및 검색 버튼 선택
        search_input = driver.find_element(By.ID, "search.keyword.query")
        search_input.clear()
        search_input.send_keys(f"{location} {category}")
        search_button = driver.find_element(By.ID, "search.keyword.submit")
        driver.execute_script("arguments[0].click();", search_button)
        time.sleep(2)  # 검색 결과 로딩 대기
    except Exception as e:
        logging.error(f"검색 중 오류 발생: {e}")
        try:
            search_input = driver.find_element(By.ID, "search.keyword.query")
            search_input.clear()
            search_input.send_keys(f"{location} {category}")
            search_input.send_keys(Keys.RETURN)
            time.sleep(2)
        except Exception as e2:
            logging.error(f"대체 검색 방법도 실패: {e2}")
            raise


def extract_store_info(store_element):
    """가게 요소에서 기본 정보를 추출하는 함수"""
    store_info = {}

    try:
        # 가게 이름
        name_element = store_element.find_element(By.CSS_SELECTOR, "a.link_name")
        store_info["name"] = name_element.text.strip()
    except NoSuchElementException:
        store_info["name"] = None
        logging.warning("가게 이름을 찾을 수 없습니다.")

    try:
        # 카테고리
        category_element = store_element.find_element(
            By.CSS_SELECTOR, "span.subcategory"
        )
        store_info["category"] = category_element.text.strip()
    except NoSuchElementException:
        store_info["category"] = None
        logging.warning("카테고리를 찾을 수 없습니다.")

    try:
        # 별점 정보
        score_count_element = store_element.find_element(
            By.CSS_SELECTOR, "a[data-id='numberofscore']"
        )
        score_count_text = score_count_element.text.strip()
        # "0건" 형식에서 숫자만 추출
        score_count_text = re.sub(r"[^0-9]", "", score_count_text)
        # 빈 문자열인 경우 0으로 처리
        score_count = int(score_count_text) if score_count_text else 0
        store_info["score_count"] = score_count

        # 별점이 있는 경우에만 별점 정보 추출
        if score_count > 0:
            score_element = store_element.find_element(
                By.CSS_SELECTOR, "em[data-id='scoreNum']"
            )
            score_text = score_element.text.strip()
            store_info["score"] = float(score_text)
        else:
            store_info["score"] = None
    except NoSuchElementException:
        store_info["score_count"] = 0
        store_info["score"] = None
        logging.warning("별점 정보를 찾을 수 없습니다.")
    except (ValueError, TypeError) as e:
        store_info["score_count"] = 0
        store_info["score"] = None
        logging.warning(f"별점 정보 처리 중 오류 발생: {e}")

    try:
        # 리뷰 갯수
        review_element = store_element.find_element(
            By.CSS_SELECTOR, "a[data-id='review'] em[data-id='numberofreview']"
        )
        review_count_text = review_element.text.strip()
        # 숫자만 추출
        review_count_text = re.sub(r"[^0-9]", "", review_count_text)
        # 빈 문자열인 경우 0으로 처리
        review_count = int(review_count_text) if review_count_text else 0
        store_info["review_count"] = review_count
    except NoSuchElementException:
        store_info["review_count"] = 0
        logging.warning("리뷰 갯수 정보를 찾을 수 없습니다.")
    except (ValueError, TypeError) as e:
        store_info["review_count"] = 0
        logging.warning(f"리뷰 갯수 정보 처리 중 오류 발생: {e}")

    try:
        # 주소
        address_element = store_element.find_element(
            By.CSS_SELECTOR, "p[data-id='address']"
        )
        store_info["address"] = address_element.text.strip()
    except NoSuchElementException:
        store_info["address"] = None
        logging.warning("주소를 찾을 수 없습니다.")

    try:
        # 영업시간
        hours_element = store_element.find_element(
            By.CSS_SELECTOR, "a[data-id='periodTxt']"
        )
        store_info["hours"] = hours_element.text.strip()
    except NoSuchElementException:
        store_info["hours"] = None
        logging.warning("영업시간을 찾을 수 없습니다.")

    return store_info


def collect_all_stores(driver, max_pages=20):
    """'장소 더보기' 버튼과 페이지 번호를 활용하여 최대 max_pages까지의 가게 정보를 수집하는 함수"""
    all_stores = []
    current_page = 1
    stores_with_reviews = 0

    # 현재 검색어 가져오기
    try:
        search_input = driver.find_element(By.ID, "search.keyword.query")
        current_search = search_input.get_attribute("value")
    except:
        current_search = "알 수 없음"

    while current_page <= max_pages:
        logging.info(
            f"[{current_search}] 페이지 {current_page} 수집 중... (현재 리뷰 있는 가게: {stores_with_reviews}개)"
        )
        time.sleep(2)

        try:
            # 첫 페이지에서만 '장소 더보기' 버튼 클릭
            if current_page == 1:
                try:
                    more_button = driver.find_element(By.ID, "info.search.place.more")
                    if more_button.is_displayed():
                        logging.info(
                            f"[{current_search}] 장소 더보기 버튼 클릭하여 추가 정보 로드"
                        )
                        driver.execute_script("arguments[0].click();", more_button)
                        time.sleep(2)
                except NoSuchElementException:
                    logging.info(f"[{current_search}] 장소 더보기 버튼이 없습니다.")
                except Exception as e:
                    logging.error(
                        f"[{current_search}] 장소 더보기 버튼 클릭 중 오류 발생: {e}"
                    )

            # 현재 페이지의 모든 가게 정보 수집
            store_elements = driver.find_elements(
                By.CSS_SELECTOR, ".placelist > li.PlaceItem"
            )

            if not store_elements:
                logging.info(f"[{current_search}] 더 이상 가게 정보가 없습니다.")
                break

            # 각 가게의 정보를 수집합니다
            for store_element in store_elements:
                store_info = extract_store_info(store_element)

                # 10페이지 이후에는 리뷰가 있는 가게만 수집 (50개 미만일 때만)
                if current_page > 10 and stores_with_reviews < 50:
                    if store_info["review_count"] > 0:
                        all_stores.append(store_info)
                        stores_with_reviews += 1
                        if stores_with_reviews >= 50:
                            logging.info(
                                f"[{current_search}] 리뷰가 있는 가게 50개 수집 완료"
                            )
                            return all_stores
                else:
                    all_stores.append(store_info)
                    if store_info["review_count"] > 0:
                        stores_with_reviews += 1

            # 10페이지까지 수집 후 리뷰가 있는 가게가 50개 미만이면 계속 진행
            if current_page == 10:
                if stores_with_reviews >= 50:
                    logging.info(
                        f"[{current_search}] 10페이지까지 수집 완료. 리뷰가 있는 가게 {stores_with_reviews}개 (목표: 50개)"
                    )
                    logging.info(
                        f"[{current_search}] 목표 달성! 다음 검색어로 넘어갑니다."
                    )
                    logging.info(f"[{current_search}] {'='*50}")
                    return all_stores
                else:
                    logging.info(
                        f"[{current_search}] 10페이지까지 수집 완료. 리뷰가 있는 가게 {stores_with_reviews}개 (목표: 50개)"
                    )
                    logging.info(
                        f"[{current_search}] 11-20페이지에서 리뷰가 있는 가게만 추가 수집 시작"
                    )

            # 다음 페이지로 이동
            if current_page < max_pages:
                try:
                    if current_page % 5 == 0:
                        # 5페이지 단위로 '다음' 버튼 클릭
                        next_button = driver.find_element(
                            By.ID, "info.search.page.next"
                        )
                        if (
                            next_button.is_displayed()
                            and "disabled" not in next_button.get_attribute("class")
                        ):
                            logging.info(
                                f"[{current_search}] 다음 페이지 버튼 클릭 (현재 {current_page}페이지 수집 완료)"
                            )
                            driver.execute_script("arguments[0].click();", next_button)
                            current_page += 1
                            time.sleep(2)
                        else:
                            logging.info(
                                f"[{current_search}] 다음 페이지 버튼을 찾을 수 없거나 비활성화됨 (현재 {current_page}페이지 수집 완료)"
                            )
                            break
                    else:
                        # 다음 페이지 번호 클릭 (1-5 사이의 페이지 번호)
                        next_page_num = (current_page % 5) + 1
                        next_page_button = driver.find_element(
                            By.ID, f"info.search.page.no{next_page_num}"
                        )
                        if next_page_button.is_displayed():
                            logging.info(
                                f"[{current_search}] 페이지 {current_page + 1} 번호 클릭 (현재 {current_page}페이지 수집 완료)"
                            )
                            driver.execute_script(
                                "arguments[0].click();", next_page_button
                            )
                            current_page += 1
                            time.sleep(2)
                        else:
                            # 페이지 번호가 5개 미만인 경우 검색 결과가 부족함을 알림
                            page_wrap = driver.find_element(
                                By.CSS_SELECTOR, "div.pageWrap"
                            )
                            page_buttons = page_wrap.find_elements(
                                By.CSS_SELECTOR, "a[id^='info.search.page.no']"
                            )
                            if len(page_buttons) < 5:
                                logging.warning(
                                    f"[{current_search}] 검색 결과가 부족합니다. (현재 {current_page}페이지 수집 완료)"
                                )
                                break
                            logging.info(
                                f"[{current_search}] 다음 페이지 버튼을 찾을 수 없음 (현재 {current_page}페이지 수집 완료)"
                            )
                            break
                except NoSuchElementException:
                    logging.info(
                        f"[{current_search}] 마지막 페이지에 도달 (현재 {current_page}페이지 수집 완료)"
                    )
                    break
                except Exception as e:
                    logging.error(
                        f"[{current_search}] 페이지 이동 중 오류 발생: {e} (현재 {current_page}페이지 수집 완료)"
                    )
                    break
            else:
                logging.info(
                    f"[{current_search}] 최대 페이지({max_pages}페이지) 수집 완료"
                )
                break

        except Exception as e:
            logging.error(
                f"[{current_search}] 페이지 {current_page} 수집 중 오류 발생: {e}"
            )
            break

    # 20페이지까지 수집 후에도 리뷰가 있는 가게가 50개 미만인 경우에만 무한 크롤링 방지 메시지 출력
    if current_page == max_pages and stores_with_reviews < 50:
        logging.warning(
            f"[{current_search}] {current_page}페이지까지 수집 완료했으나 리뷰가 있는 가게가 50개 미만입니다. (현재: {stores_with_reviews}개)"
        )
        logging.warning(
            f"[{current_search}] 무한 크롤링 방지를 위해 다음 검색어로 넘어갑니다."
        )
        logging.warning(f"[{current_search}] {'='*50}")

    return all_stores


def process_location_category(args):
    """지역과 카테고리 조합에 대한 데이터 수집을 처리하는 함수"""
    location, category = args
    driver = get_driver()
    try:
        logging.info(f"== {location} {category} 검색 시작 ==")
        search_places(driver, location, category)
        stores = collect_all_stores(driver, max_pages=20)
        df = pd.DataFrame(stores)

        # 중복 제거 (가게 이름 + 주소 기준)
        df = df.drop_duplicates(subset=["name", "address"])

        # 데이터 저장 경로 설정
        data_dir = "data/1_location_categories"
        os.makedirs(data_dir, exist_ok=True)

        # CSV 파일 저장
        csv_filename = os.path.join(data_dir, f"{location}_{category}.csv")
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")

        # 리뷰가 있는 가게 수 계산
        stores_with_reviews = len(df[df["review_count"] > 0])

        if stores_with_reviews < 50:
            logging.warning(f"\n{'='*50}")
            logging.warning(f"데이터 부족 알림: {location} {category}")
            logging.warning(f"- 전체 수집된 가게: {len(df)}개")
            logging.warning(f"- 리뷰가 있는 가게: {stores_with_reviews}개")
            logging.warning(f"- 목표 대비 달성률: {(stores_with_reviews/50)*100:.1f}%")
            logging.warning(f"{'='*50}\n")
        else:
            logging.info(
                f"{location} {category} 데이터 수집 완료: {len(df)}개 가게 (저장 파일: {csv_filename})"
            )
        return df
    except Exception as e:
        logging.error(f"{location} {category} 처리 중 오류 발생: {e}")
        return pd.DataFrame()
    finally:
        return_driver(driver)


def main():
    """메인 함수: 여러 지역 및 카테고리 조합에 대해 데이터 수집"""
    # combined_dir 디렉토리 생성
    combined_dir = "data/2_combined_location_categories"
    os.makedirs(combined_dir, exist_ok=True)

    locations = ["강남역", "홍대입구역", "성수역", "이태원역", "압구정역"]
    categories = ["식당", "카페", "술집", "노래방", "PC방", "클럽", "볼링장", "당구장"]

    # 드라이버 풀 초기화
    initialize_driver_pool()

    try:
        # 모든 지역과 카테고리 조합 생성
        tasks = [(loc, cat) for loc in locations for cat in categories]

        # ThreadPoolExecutor를 사용하여 병렬 처리
        all_results = []
        with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
            results = list(executor.map(process_location_category, tasks))
            # 빈 DataFrame 필터링
            all_results = [df for df in results if not df.empty and len(df.columns) > 0]

        # 모든 데이터를 하나의 파일로 저장
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            combined_file_path = os.path.join(combined_dir, "kakao_map_all_data.csv")
            combined_df.to_csv(combined_file_path, index=False, encoding="utf-8-sig")
            print(f"\n전체 데이터 저장 완료: {len(combined_df)}개 가게")

    finally:
        # 드라이버 풀 정리
        while not driver_pool.empty():
            driver = driver_pool.get()
            driver.quit()


if __name__ == "__main__":
    main()
