import logging
import time
import re
import os
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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


def search_places(driver, location, category):
    """카카오맵에서 특정 지역과 카테고리로 검색하는 함수"""
    logging.info(f"'{location} {category}' 검색 중...")
    driver.get("https://map.kakao.com/")
    time.sleep(2)  # 기본 페이지 로딩 대기 시간 단축

    try:
        # 검색 입력창 및 검색 버튼 선택
        search_input = driver.find_element(By.ID, "search.keyword.query")
        search_input.clear()
        search_input.send_keys(f"{location} {category}")
        search_button = driver.find_element(By.ID, "search.keyword.submit")
        driver.execute_script("arguments[0].click();", search_button)
        time.sleep(3)  # 검색 결과 로딩 대기 시간 단축
    except Exception as e:
        logging.error(f"검색 중 오류 발생: {e}")
        try:
            search_input = driver.find_element(By.ID, "search.keyword.query")
            search_input.clear()
            search_input.send_keys(f"{location} {category}")
            search_input.send_keys(Keys.RETURN)
            time.sleep(3)
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


def collect_all_stores(driver, max_pages=10):
    """'장소 더보기' 버튼과 페이지 번호를 활용하여 최대 max_pages까지의 가게 정보를 수집하는 함수"""
    all_stores = []
    current_page = 1

    while current_page <= max_pages:
        logging.info(f"페이지 {current_page} 수집 중...")
        time.sleep(1)  # 페이지 전환 대기 시간 단축

        try:
            # 첫 페이지에서만 '장소 더보기' 버튼 클릭
            if current_page == 1:
                try:
                    more_button = driver.find_element(By.ID, "info.search.place.more")
                    if more_button.is_displayed():
                        driver.execute_script("arguments[0].click();", more_button)
                        time.sleep(2)  # 장소 더보기 로딩 대기 시간 단축
                except NoSuchElementException:
                    pass
                except Exception as e:
                    logging.error(f"장소 더보기 버튼 클릭 중 오류 발생: {e}")

            # 현재 페이지의 모든 가게 정보 수집
            store_elements = driver.find_elements(
                By.CSS_SELECTOR, ".placelist > li.PlaceItem"
            )

            if not store_elements:
                logging.info("더 이상 가게 정보가 없습니다.")
                break

            # 각 가게의 정보를 수집합니다
            for store_element in store_elements:
                store_info = extract_store_info(store_element)
                all_stores.append(store_info)

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
                            driver.execute_script("arguments[0].click();", next_button)
                            current_page += 1
                            time.sleep(1)  # 페이지 전환 대기 시간 단축
                        else:
                            logging.info("다음 페이지 버튼을 찾을 수 없거나 비활성화됨")
                            break
                    else:
                        # 다음 페이지 번호 클릭 (1-5 사이의 페이지 번호)
                        next_page_num = (current_page % 5) + 1
                        next_page_button = driver.find_element(
                            By.ID, f"info.search.page.no{next_page_num}"
                        )
                        if next_page_button.is_displayed():
                            driver.execute_script(
                                "arguments[0].click();", next_page_button
                            )
                            current_page += 1
                            time.sleep(1)  # 페이지 전환 대기 시간 단축
                        else:
                            logging.info("다음 페이지 버튼을 찾을 수 없음")
                            break
                except NoSuchElementException:
                    logging.info("마지막 페이지에 도달")
                    break
                except Exception as e:
                    logging.error(f"페이지 이동 중 오류 발생: {e}")
                    break
            else:
                break

        except Exception as e:
            logging.error(f"페이지 {current_page} 수집 중 오류 발생: {e}")
            break

    return all_stores


def filter_by_address(df, target_addresses):
    """특정 주소에 해당하는 가게만 필터링하는 함수"""
    filtered_df = df[df["address"].str.contains("|".join(target_addresses), na=False)]
    return filtered_df


def main():
    """메인 함수: 여러 지역 및 카테고리 조합에 대해 데이터 수집"""
    locations = ["강남역", "홍대입구역", "성수역", "이태원역", "압구정역"]
    categories = ["식당", "카페", "술집", "노래방", "PC방", "클럽", "볼링장", "당구장"]

    all_results = pd.DataFrame()
    driver = setup_driver()

    try:
        for location in locations:
            for category in categories:
                logging.info(f"== {location} {category} 검색 시작 ==")
                search_places(driver, location, category)
                stores = collect_all_stores(driver, max_pages=10)
                df = pd.DataFrame(stores)

                # 중복 제거 (가게 이름 + 주소 기준)
                df = df.drop_duplicates(subset=["name", "address"])

                # 결과 저장
                all_results = pd.concat([all_results, df], ignore_index=True)
                csv_filename = f"kakao_map_data_{location}_{category}.csv"
                df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
                logging.info(
                    f"{location} {category} 데이터 수집 완료: {len(df)}개 가게 (저장 파일: {csv_filename})"
                )

        # 전체 결과 저장
        all_results.to_csv("kakao_map_all_data.csv", index=False, encoding="utf-8-sig")
        logging.info(f"전체 데이터 수집 완료: {len(all_results)}개 가게")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
