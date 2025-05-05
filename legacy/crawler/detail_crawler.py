"""
가게 상세 정보 크롤러 모듈

- 검색어로 카카오맵에서 가게 상세 페이지 진입 후
  og:url과 전화번호를 추출하는 기능 모듈
"""

import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

def search_store_detail(driver, store_name: str) -> tuple[str, str]:
    """
    검색어 기반 상세 페이지 URL 및 전화번호 추출 기능

    매개변수
    ----------
    driver : webdriver.Chrome
        셀레니움 크롬 드라이버 인스턴스
    store_name : str
        크롤링 대상 가게명

    반환값
    ----------
    detail_url : str
        meta[property="og:url"]에서 추출된 상세 페이지 URL (없으면 빈 문자열)
    phone : str
        사업자 정보에서 추출된 전화번호 또는 '-1'
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
        name_el = it.find_element(By.CSS_SELECTOR, "a.link_name")
        nm = (name_el.get_attribute("title") or name_el.text).strip()
        if nm == store_name:
            # 상세보기 버튼 클릭
            btn_more = it.find_element(By.CSS_SELECTOR, "a[data-id='moreview']")
            driver.execute_script("arguments[0].click();", btn_more)
            found = True
            break
    if not found:
        # '장소 더보기' 클릭 후 재탐색
        try:
            mb = driver.find_element(By.ID, "info.search.place.more")
            driver.execute_script("arguments[0].click();", mb)
            time.sleep(1.5)
            items = driver.find_elements(By.CSS_SELECTOR, "ul.placelist li.PlaceItem")
            for it in items:
                name_el = it.find_element(By.CSS_SELECTOR, "a.link_name")
                if (name_el.get_attribute("title") or name_el.text).strip() == store_name:
                    btn_more = it.find_element(By.CSS_SELECTOR, "a[data-id='moreview']")
                    driver.execute_script("arguments[0].click();", btn_more)
                    found = True
                    break
        except:
            pass
    if not found:
        return "", "-1"

    time.sleep(1.5)
    # 새 창으로 전환
    handles = driver.window_handles
    if len(handles) > 1:
        driver.switch_to.window(handles[-1])
    time.sleep(1.5)

    # og:url과 전화번호 추출
    soup = BeautifulSoup(driver.page_source, "html.parser")
    meta = soup.find("meta", {"property": "og:url"})
    detail_url = meta["content"] if meta else ""
    phone = "-1"
    info_section = soup.select_one("div.section_comm.section_defaultinfo")
    if info_section:
        for unit in info_section.select("div.unit_default"):
            if unit.select_one("span.ico_mapdesc.ico_call2"):
                txt = unit.select_one("span.txt_detail")
                if txt:
                    phone = txt.get_text(strip=True)
                break

    return detail_url, phone
