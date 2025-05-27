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
    """
    Selenium 웹 드라이버 설정 및 초기화 함수.

    입력값:
        없음

    반환값:
        webdriver.Chrome: 설정이 완료된 Chrome 웹 드라이버 객체.

    설명:
        - Chrome 브라우저의 알림 비활성화 옵션 적용.
        - 필요시 headless 모드 사용 가능(주석 처리됨).
        - 생성된 브라우저 창을 최소화하여 시스템 자원 절약.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    # 필요한 경우 headless 모드 사용 가능
    # options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.minimize_window()  # 창 최소화
    return driver


def initialize_driver_pool():
    """
    드라이버 풀 초기화 함수.

    입력값:
        없음

    반환값:
        없음

    설명:
        - 전역 변수 MAX_DRIVERS에 지정된 수만큼 웹 드라이버 인스턴스 생성.
        - 생성된 드라이버를 전역 driver_pool 큐에 추가하여 병렬 처리 준비.
    """
    for _ in range(MAX_DRIVERS):
        driver = setup_driver()
        driver_pool.put(driver)


def get_driver():
    """
    드라이버 풀에서 사용 가능한 드라이버 가져오기 함수.

    입력값:
        없음

    반환값:
        webdriver.Chrome: 드라이버 풀에서 가져온 Chrome 웹 드라이버 객체.

    설명:
        - driver_pool 큐에서 사용 가능한 드라이버를 가져옴.
        - 풀이 비어있는 경우 큐가 비워질 때까지 대기.
    """
    return driver_pool.get()


def return_driver(driver):
    """
    사용 완료된 드라이버를 풀에 반환하는 함수.

    입력값:
        driver (webdriver.Chrome): 반환할 Chrome 웹 드라이버 객체.

    반환값:
        없음

    설명:
        - 작업이 완료된 드라이버를 driver_pool 큐에 다시 추가하여 재사용 가능하게 함.
    """
    driver_pool.put(driver)


def search_places(driver, location, category):
    """
    카카오맵에서 특정 지역과 카테고리 조합으로 검색을 수행하는 함수.

    입력값:
        driver (webdriver.Chrome): 사용할 Chrome 웹 드라이버 객체.
        location (str): 검색할 지역명(예: '강남역', '홍대입구역').
        category (str): 검색할 카테고리명(예: '식당', '카페', '클럽').

    반환값:
        tuple: (location, category) 검색에 사용된 지역명과 카테고리명

    설명:
        - 카카오맵 웹사이트 접속 후 검색창에 지역명과 카테고리를 입력하여 검색.
        - 검색 버튼 클릭에 실패할 경우 Enter 키 입력을 통한 대체 검색 시도.
    """
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

    return location, category


def extract_store_info(store_element):
    """
    검색 결과의 가게 요소에서 상세 정보를 추출하는 함수.

    입력값:
        store_element (WebElement): 카카오맵 검색 결과의 가게 정보를 포함하는 웹 요소.

    반환값:
        dict: 다음 키를 포함하는 가게 정보 딕셔너리.
            - str_name (str): 가게 이름
            - str_sub_category (str): 가게 카테고리
            - i_star_point_count (int): 별점 평가 수
            - f_star_point (float): 평균 별점(-1은 별점 없음을 의미)
            - i_review_count (int): 리뷰 수
            - str_address (str): 가게 주소
            - run_day (str): 영업 요일
            - run_time_start (str): 영업 시작 시간
            - run_time_end (str): 영업 종료 시간

    설명:
        - 각 정보 추출 시 요소가 없는 경우 None 또는 기본값(-1, 0 등) 반환.
        - 별점과 리뷰 정보는 정규식을 통해 숫자만 추출하여 처리.
    """
    store_info = {}

    try:
        # 가게 이름
        name_element = store_element.find_element(By.CSS_SELECTOR, "a.link_name")
        store_info["str_name"] = name_element.text.strip()
    except NoSuchElementException:
        store_info["str_name"] = None
        logging.warning("가게 이름을 찾을 수 없습니다.")

    try:
        # 카테고리
        category_element = store_element.find_element(
            By.CSS_SELECTOR, "span.subcategory"
        )
        store_info["str_sub_category"] = category_element.text.strip()
    except NoSuchElementException:
        store_info["str_sub_category"] = None
        logging.warning("카테고리를 찾을 수 없습니다.")

    try:
        # 별점 정보
        score_count_element = store_element.find_element(
            By.CSS_SELECTOR, "a[data-id='numberofscore']"
        )
        score_count_text = score_count_element.text.strip()
        # "0건" 형식에서 숫자만 추출
        score_count_text = re.sub(r"[^0-9]", "", score_count_text)
        # 빈 문자열인 경우 None으로 처리
        score_count = int(score_count_text) if score_count_text else None
        store_info["i_star_point_count"] = score_count

        # 별점이 있는 경우에만 별점 정보 추출
        if score_count and score_count > 0:
            score_element = store_element.find_element(
                By.CSS_SELECTOR, "em[data-id='scoreNum']"
            )
            score_text = score_element.text.strip()
            store_info["f_star_point"] = float(score_text)
        else:
            store_info["f_star_point"] = None
    except NoSuchElementException:
        store_info["i_star_point_count"] = None
        store_info["f_star_point"] = None
        logging.warning("별점 정보를 찾을 수 없습니다.")
    except (ValueError, TypeError) as e:
        store_info["i_star_point_count"] = None
        store_info["f_star_point"] = None
        logging.warning(f"별점 정보 처리 중 오류 발생: {e}")

    try:
        # 리뷰 갯수
        review_element = store_element.find_element(
            By.CSS_SELECTOR, "a[data-id='review'] em[data-id='numberofreview']"
        )
        review_count_text = review_element.text.strip()
        # 숫자만 추출
        review_count_text = re.sub(r"[^0-9]", "", review_count_text)
        # 빈 문자열인 경우 None으로 처리
        review_count = int(review_count_text) if review_count_text else None
        store_info["i_review_count"] = review_count
    except NoSuchElementException:
        store_info["i_review_count"] = None
        logging.warning("리뷰 갯수 정보를 찾을 수 없습니다.")
    except (ValueError, TypeError) as e:
        store_info["i_review_count"] = None
        logging.warning(f"리뷰 갯수 정보 처리 중 오류 발생: {e}")

    try:
        # 주소
        address_element = store_element.find_element(
            By.CSS_SELECTOR, "p[data-id='address']"
        )
        store_info["str_address"] = address_element.text.strip()
    except NoSuchElementException:
        store_info["str_address"] = None
        logging.warning("주소를 찾을 수 없습니다.")

    try:
        # 영업시간
        hours_element = store_element.find_element(
            By.CSS_SELECTOR, "a[data-id='periodTxt']"
        )
        hours_text = hours_element.text.strip()

        # 휴게시간 정보 제거
        if "·" in hours_text:
            hours_text = hours_text.split("·")[0].strip()

        # 요일 정보 파싱
        day_pattern = (
            r"(?:영업시간\s+)?(?:매일|(?:[월화수목금토일][,~][월화수목금토일]+))"
        )
        day_match = re.search(day_pattern, hours_text)

        if day_match:
            run_day = day_match.group(0)
            if "영업시간" in run_day:
                run_day = run_day.replace("영업시간", "").strip()
            if run_day == "매일":
                run_day = "월,화,수,목,금,토,일"
        else:
            run_day = None

        # 시간 정보 파싱
        time_pattern = r"(\d{2}:\d{2})\s*~\s*(\d{2}:\d{2})"
        time_match = re.search(time_pattern, hours_text)

        if time_match:
            run_time_start = time_match.group(1)
            run_time_end = time_match.group(2)
        else:
            run_time_start = None
            run_time_end = None

        # 새로운 칼럼에 정보 저장
        store_info["run_day"] = run_day
        store_info["run_time_start"] = run_time_start
        store_info["run_time_end"] = run_time_end

    except NoSuchElementException:
        store_info["run_day"] = None
        store_info["run_time_start"] = None
        store_info["run_time_end"] = None
        logging.warning("영업시간을 찾을 수 없습니다.")

    try:
        # 전화번호
        phone_element = store_element.find_element(By.CSS_SELECTOR, "span.phone")
        store_info["str_telephone"] = phone_element.text.strip()
    except NoSuchElementException:
        store_info["str_telephone"] = -1
        logging.warning("전화번호를 찾을 수 없습니다.")

    return store_info


def collect_all_stores(driver, max_pages=50, search_info=None):
    """
    카카오맵 검색 결과에서 가게 정보를 페이지별로 수집하는 함수.

    입력값:
        driver (webdriver.Chrome): 사용할 Chrome 웹 드라이버 객체.
        max_pages (int, 기본값=50): 수집할 최대 페이지 수.
        search_info (tuple, 기본값=None): (location, category) 검색에 사용된 지역명과 카테고리명

    반환값:
        list: 각 가게 정보를 담은 딕셔너리 객체들의 리스트.

    설명:
        - 최대 max_pages까지 페이지를 이동하며 가게 정보 수집.
        - 첫 페이지에서는 '장소 더보기' 버튼을 클릭하여 더 많은 결과 로드.
        - 리뷰가 있는 가게가 500개에 도달하면 조기 종료(목표 달성).
        - 페이지 이동 시 5페이지 단위로 '다음' 버튼, 그 외에는 페이지 번호 클릭.
    """
    all_stores = []
    current_page = 1
    stores_with_reviews = 0
    unique_stores = set()  # 중복 제거를 위한 set

    # 현재 검색어 가져오기
    try:
        search_input = driver.find_element(By.ID, "search.keyword.query")
        current_search = search_input.get_attribute("value")
    except:
        current_search = "알 수 없음"

    # 검색 정보 설정
    location, category = search_info if search_info else ("", "")

    while current_page <= max_pages:
        logging.info(
            f"[{current_search}] 페이지 {current_page} 수집 중... (현재 리뷰 있는 가게: {stores_with_reviews}개, 현재 수집된 가게: {len(all_stores)}개)"
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

                # 검색 키워드 정보 추가
                store_info["str_location_keyword"] = location
                store_info["str_main_category"] = category
                store_info["str_url"] = (
                    f"https://map.kakao.com/?q={location} {store_info['str_name']}"
                )

                # 중복 체크 (가게 이름 + 주소로 유니크한 식별)
                store_key = f"{store_info['str_name']}_{store_info['str_address']}"
                if store_key not in unique_stores:
                    unique_stores.add(store_key)
                    all_stores.append(store_info)
                    if store_info["i_review_count"] > 0:
                        stores_with_reviews += 1
                        if stores_with_reviews >= 500:
                            logging.info(
                                f"[{current_search}] 리뷰가 있는 가게 500개 수집 완료"
                            )
                            return all_stores

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

    # 50페이지까지 수집 후에도 리뷰가 있는 가게가 500개 미만인 경우에만 무한 크롤링 방지 메시지 출력
    if current_page == max_pages and stores_with_reviews < 500:
        logging.warning(
            f"[{current_search}] {current_page}페이지까지 수집 완료했으나 리뷰가 있는 가게가 500개 미만입니다. (현재: {stores_with_reviews}개)"
        )
        logging.warning(
            f"[{current_search}] 무한 크롤링 방지를 위해 다음 검색어로 넘어갑니다."
        )
        logging.warning(f"[{current_search}] {'='*50}")

    return all_stores


def process_location_category(args):
    """
    지역과 카테고리 조합에 대한 가게 데이터 수집 및 저장 처리 함수.

    입력값:
        args (tuple): (location, category) 형식의 튜플.
            - location (str): 검색할 지역명(예: '강남역').
            - category (str): 검색할 카테고리명(예: '식당').

    반환값:
        pandas.DataFrame: 수집된 가게 정보가 담긴 데이터프레임. 오류 발생 시 빈 데이터프레임 반환.

    설명:
        - 드라이버 풀에서 드라이버를 가져와 검색 및 정보 수집 수행.
        - 수집된 가게 정보를 데이터프레임으로 변환하고 중복 제거.
        - CSV 파일 형식으로 지역별 카테고리 데이터 저장.
        - 리뷰가 있는 가게가 목표치(50개)보다 적을 경우 경고 메시지 출력.
        - 작업 완료 후 드라이버를 풀에 반환.
    """
    location, category = args
    driver = get_driver()
    try:
        logging.info(f"== {location} {category} 검색 시작 ==")
        search_info = search_places(driver, location, category)
        stores = collect_all_stores(driver, max_pages=50, search_info=search_info)
        df = pd.DataFrame(stores)

        # 중복 제거 (가게 이름 + 주소 기준)
        df = df.drop_duplicates(subset=["str_name", "str_address"])

        # 칼럼 순서 지정
        column_order = [
            "str_name",
            "str_address",
            "str_location_keyword",
            "str_main_category",
            "str_sub_category",
            "i_star_point_count",
            "f_star_point",
            "i_review_count",
            "run_day",
            "run_time_start",
            "run_time_end",
            "str_url",
            "str_telephone",
        ]
        df = df[column_order]

        # 데이터 저장 경로 설정
        data_dir = "data/1_location_categories"
        os.makedirs(data_dir, exist_ok=True)

        # CSV 파일 저장
        csv_filename = os.path.join(data_dir, f"{location}_{category}.csv")
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")

        # 리뷰가 있는 가게 수 계산
        stores_with_reviews = len(df[df["i_review_count"] > 0])

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
    """
    카카오맵 크롤링 메인 실행 함수.

    입력값:
        없음

    반환값:
        없음

    설명:
        - 지정된 지역 및 카테고리 조합에 대한 데이터 수집 작업 수행.
        - 병렬 처리를 위한 ThreadPoolExecutor 사용하여 효율적인 크롤링 수행.
        - 수집된 모든 데이터를 통합하여 하나의 CSV 파일로 저장.
        - 작업 완료 후 드라이버 풀의 모든 드라이버 종료 및 정리.
        - 주요 처리 단계:
          1. 데이터 저장 디렉토리 생성
          2. 드라이버 풀 초기화
          3. 지역과 카테고리 조합 생성
          4. 병렬 처리로 각 조합에 대한 데이터 수집
          5. 결과 통합 및 저장
          6. 드라이버 정리
    """
    start_time = time.time()
    logging.info("크롤링 시작")

    # combined_dir 디렉토리 생성
    combined_dir = "data/2_combined_location_categories"
    os.makedirs(combined_dir, exist_ok=True)

    # 지역 및 카테고리 설정
    location_keyword = ["강남역", "홍대입구역", "성수역", "이태원역", "압구정역"]
    main_category = [
        "식당",
        "카페",
        "술집",
        "노래방",
        "PC방",
        "클럽",
        "볼링장",
        "당구장",
    ]

    # 드라이버 풀 초기화
    initialize_driver_pool()

    try:
        # 모든 지역과 카테고리 조합 생성
        tasks = [(loc, cat) for loc in location_keyword for cat in main_category]
        total_tasks = len(tasks)
        completed_tasks = 0

        # ThreadPoolExecutor를 사용하여 병렬 처리
        all_results = []
        with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
            results = list(executor.map(process_location_category, tasks))
            # 빈 DataFrame 필터링
            all_results = [df for df in results if not df.empty and len(df.columns) > 0]
            completed_tasks = len(all_results)

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

    end_time = time.time()
    execution_time = end_time - start_time
    hours = int(execution_time // 3600)
    minutes = int((execution_time % 3600) // 60)
    seconds = int(execution_time % 60)

    logging.info(f"크롤링 완료")
    logging.info(f"총 실행 시간: {hours}시간 {minutes}분 {seconds}초")
    logging.info(f"총 작업 수: {total_tasks}개")
    logging.info(f"성공한 작업 수: {completed_tasks}개")
    logging.info(f"실패한 작업 수: {total_tasks - completed_tasks}개")
    logging.info(f"평균 처리 시간: {execution_time/total_tasks:.2f}초/작업")


if __name__ == "__main__":
    main()
