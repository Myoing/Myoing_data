from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import logging
import os
import time
import uuid
import json
from datetime import datetime
import uvicorn

# 크롤링 모듈 임포트
import code.kakao_map_basic_crawler as basic_crawler
import code.filters as filters
import code.review_crawler as review_crawler

# 웹드라이버 관련 임포트
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
from threading import Lock

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# API 앱 초기화
app = FastAPI(
    title="Myoing Data API",
    description="카카오맵 데이터 수집 및 필터링 API",
    version="1.0.0",
)

# 디렉토리 경로 상수 정의
DATA_DIRS = {
    "basic": "data/1_location_categories",
    "combined": "data/2_combined_location_categories",
    "filtered": "data/3_filtered_location_categories_hour_club",
    "all_filtered": "data/4_filtered_all_hour_club",
    "review_filtered": "data/5_filtered_all_hour_club_reviewcount",
    "reviews": "data/6_reviews_about_5",
}

# 공통으로 사용할 드라이버 풀 관련 변수
driver_pool = Queue()
MAX_DRIVERS = 4
driver_lock = Lock()

# 작업 상태 관리
tasks = {}


# 모델 정의
class CrawlingRequest(BaseModel):
    str_location_keywords: List[str] = [
        "강남역",
        "홍대입구역",
        "성수역",
        "압구정역",
        "이태원역",
    ]
    str_main_categories: List[str] = [
        "식당",
        "카페",
        "술집",
        "노래방",
        "PC방",
        "볼링장",
        "당구장",
        "클럽",
    ]
    max_pages: int = 20
    max_reviews: int = 50


class TaskStatus(BaseModel):
    task_id: str
    status: str  # 'pending', 'running', 'completed', 'failed'
    start_time: str
    end_time: Optional[str] = None
    progress: float = 0.0
    current_step: str = ""
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


# 유틸리티 함수
def setup_driver():
    """셀레니움 드라이버 설정 함수"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    options.add_argument("--headless=new")  # 새로운 headless 모드 사용
    options.add_argument("--no-sandbox")  # EC2 환경에서 필요한 보안 설정
    options.add_argument("--disable-dev-shm-usage")  # 공유 메모리 사용 비활성화
    options.add_argument("--disable-gpu")  # GPU 사용 비활성화
    options.add_argument("--disable-software-rasterizer")  # 소프트웨어 렌더링 비활성화
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def initialize_driver_pool():
    """드라이버 풀 초기화"""
    # global driver_pool - 참조만 하는 경우 global 선언 불필요

    # 기존 드라이버 풀 정리
    cleanup_driver_pool()

    # 새 드라이버 생성
    logging.info(f"{MAX_DRIVERS}개의 웹 드라이버를 초기화합니다...")
    for _ in range(MAX_DRIVERS):
        driver_pool.put(setup_driver())
    logging.info("드라이버 풀 초기화 완료")


def cleanup_driver_pool():
    """드라이버 풀의 모든 드라이버 종료 및 정리"""
    global driver_pool  # 값을 할당하는 경우 global 선언 필요

    logging.info("드라이버 풀 정리 중...")
    temp_pool = Queue()

    # 기존 드라이버 풀에서 모든 드라이버를 가져와 종료
    while not driver_pool.empty():
        try:
            driver = driver_pool.get(block=False)
            try:
                driver.quit()
            except Exception as e:
                logging.warning(f"드라이버 종료 중 오류 발생: {e}")
        except Exception:
            break

    # 빈 큐로 초기화
    driver_pool = Queue()  # 값 할당


# 백그라운드 작업 함수
async def run_basic_crawler(task_id: str, params: CrawlingRequest):
    """카카오맵 기본 크롤러 실행"""
    try:
        # 상태 업데이트
        tasks[task_id].status = "running"
        tasks[task_id].current_step = "basic_crawler"

        # 드라이버 풀 초기화
        initialize_driver_pool()

        # 크롤러 모듈 설정
        basic_crawler.driver_pool = driver_pool
        basic_crawler.MAX_DRIVERS = MAX_DRIVERS
        basic_crawler.driver_lock = driver_lock
        basic_crawler.initialize_driver_pool = lambda: None

        # 크롤링 실행
        str_location_keywords = params.str_location_keywords
        str_main_categories = params.str_main_categories

        total_tasks = len(str_location_keywords) * len(str_main_categories)
        completed_tasks = 0

        # 병렬 처리를 위한 후처리 함수
        def update_progress(future):
            nonlocal completed_tasks
            completed_tasks += 1
            tasks[task_id].progress = (
                completed_tasks / total_tasks
            ) * 33.3  # 전체 작업의 1/3

        # 크롤링 실행
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=MAX_DRIVERS) as executor:
            futures = []
            for str_location_keyword in str_location_keywords:
                for str_main_category in str_main_categories:
                    future = executor.submit(
                        basic_crawler.process_location_category,
                        (str_location_keyword, str_main_category),
                    )
                    future.add_done_callback(update_progress)
                    futures.append(future)

            # 모든 작업이 완료될 때까지 대기
            for future in futures:
                future.result()

        # 드라이버 풀 정리
        cleanup_driver_pool()

        # 1단계 완료 상태 업데이트
        tasks[task_id].current_step = "basic_crawler_completed"
        return True
    except Exception as e:
        logging.error(f"기본 크롤러 실행 중 오류 발생: {e}")
        tasks[task_id].status = "failed"
        tasks[task_id].error = str(e)
        cleanup_driver_pool()
        return False


async def run_filters(task_id: str):
    """필터링 모듈 실행"""
    try:
        # 상태 업데이트
        tasks[task_id].current_step = "filters"

        # 드라이버 풀 초기화
        initialize_driver_pool()

        # 필터링 모듈 설정
        filters.driver_pool = driver_pool
        filters.MAX_DRIVERS = MAX_DRIVERS
        filters.driver_lock = driver_lock
        filters.initialize_driver_pool = lambda: None

        # 필터링 실행
        filters.process_all_locations()

        # 드라이버 풀 정리
        cleanup_driver_pool()

        # 진행률 업데이트
        tasks[task_id].progress = 66.6  # 전체 작업의 2/3
        tasks[task_id].current_step = "filters_completed"
        return True
    except Exception as e:
        logging.error(f"필터링 모듈 실행 중 오류 발생: {e}")
        tasks[task_id].status = "failed"
        tasks[task_id].error = str(e)
        cleanup_driver_pool()
        return False


async def run_review_crawler(task_id: str, params: CrawlingRequest):
    """리뷰 크롤러 실행"""
    try:
        # 상태 업데이트
        tasks[task_id].current_step = "review_crawler"

        # 드라이버 풀 초기화
        initialize_driver_pool()

        # 리뷰 크롤러 모듈 설정
        review_crawler.driver_pool = driver_pool
        review_crawler.MAX_DRIVERS = MAX_DRIVERS
        review_crawler.driver_lock = driver_lock
        review_crawler.initialize_driver_pool = lambda: None

        # 리뷰 크롤링 실행
        review_crawler.main()

        # 드라이버 풀 정리
        cleanup_driver_pool()

        # 작업 완료 상태 업데이트
        tasks[task_id].progress = 100.0
        tasks[task_id].status = "completed"
        tasks[task_id].current_step = "completed"
        tasks[task_id].end_time = datetime.now().isoformat()

        # 결과 정보 설정
        data_stats = {
            "basic_data": len(os.listdir(DATA_DIRS["basic"])),
            "filtered_data": len(os.listdir(DATA_DIRS["filtered"])),
            "review_data": os.path.exists(
                os.path.join(DATA_DIRS["reviews"], "kakao_map_reviews_filtered.csv")
            ),
        }
        tasks[task_id].result = data_stats

        return True
    except Exception as e:
        logging.error(f"리뷰 크롤러 실행 중 오류 발생: {e}")
        tasks[task_id].status = "failed"
        tasks[task_id].error = str(e)
        cleanup_driver_pool()
        return False


async def run_full_pipeline(task_id: str, params: CrawlingRequest):
    """전체 크롤링 파이프라인 실행"""
    # 기본 크롤러 실행
    if not await run_basic_crawler(task_id, params):
        return

    # 필터링 모듈 실행
    if not await run_filters(task_id):
        return

    # 리뷰 크롤러 실행
    await run_review_crawler(task_id, params)


# API 엔드포인트
@app.get("/")
async def root():
    """API 상태 확인"""
    return {"status": "online", "message": "Myoing Data API is running"}


@app.post("/myoing_data/crawler/all", status_code=status.HTTP_202_ACCEPTED)
async def start_full_crawling(
    request: CrawlingRequest, background_tasks: BackgroundTasks
):
    """전체 크롤링 파이프라인 시작"""
    task_id = str(uuid.uuid4())

    # 작업 상태 초기화
    tasks[task_id] = TaskStatus(
        task_id=task_id, status="pending", start_time=datetime.now().isoformat()
    )

    # 백그라운드 작업 시작
    background_tasks.add_task(run_full_pipeline, task_id, request)

    return {
        "task_id": task_id,
        "status": "accepted",
        "message": "크롤링 작업이 시작되었습니다. 작업 상태를 확인하려면 /myoing_data/crawler/status/{task_id} 엔드포인트를 사용하세요.",
    }


@app.post("/myoing_data/crawler/basic", status_code=status.HTTP_202_ACCEPTED)
async def start_basic_crawling(
    request: CrawlingRequest, background_tasks: BackgroundTasks
):
    """기본 크롤러만 실행"""
    task_id = str(uuid.uuid4())

    # 작업 상태 초기화
    tasks[task_id] = TaskStatus(
        task_id=task_id, status="pending", start_time=datetime.now().isoformat()
    )

    # 백그라운드 작업 시작
    background_tasks.add_task(run_basic_crawler, task_id, request)

    return {
        "task_id": task_id,
        "status": "accepted",
        "message": "기본 크롤링 작업이 시작되었습니다. 작업 상태를 확인하려면 /myoing_data/crawler/status/{task_id} 엔드포인트를 사용하세요.",
    }


@app.post("/myoing_data/crawler/filter", status_code=status.HTTP_202_ACCEPTED)
async def start_filtering(background_tasks: BackgroundTasks):
    """필터링 모듈만 실행"""
    task_id = str(uuid.uuid4())

    # 작업 상태 초기화
    tasks[task_id] = TaskStatus(
        task_id=task_id, status="pending", start_time=datetime.now().isoformat()
    )

    # 백그라운드 작업 시작
    background_tasks.add_task(run_filters, task_id)

    return {
        "task_id": task_id,
        "status": "accepted",
        "message": "필터링 작업이 시작되었습니다. 작업 상태를 확인하려면 /myoing_data/crawler/status/{task_id} 엔드포인트를 사용하세요.",
    }


@app.post("/myoing_data/crawler/reviews", status_code=status.HTTP_202_ACCEPTED)
async def start_review_crawling(
    request: CrawlingRequest, background_tasks: BackgroundTasks
):
    """리뷰 크롤러만 실행"""
    task_id = str(uuid.uuid4())

    # 작업 상태 초기화
    tasks[task_id] = TaskStatus(
        task_id=task_id, status="pending", start_time=datetime.now().isoformat()
    )

    # 백그라운드 작업 시작
    background_tasks.add_task(run_review_crawler, task_id, request)

    return {
        "task_id": task_id,
        "status": "accepted",
        "message": "리뷰 크롤링 작업이 시작되었습니다. 작업 상태를 확인하려면 /myoing_data/crawler/status/{task_id} 엔드포인트를 사용하세요.",
    }


@app.get("/myoing_data/crawler/status/{task_id}")
async def check_crawling_status(task_id: str):
    """크롤링 작업 상태 확인"""
    if task_id not in tasks:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"작업 ID {task_id}를 찾을 수 없습니다.",
        )

    return tasks[task_id]


@app.get("/data/{data_type}")
async def list_data_files(data_type: str):
    """
    지정된 데이터 타입의 파일 목록을 반환합니다.

    매개변수:
        data_type (str): 데이터 타입 ("basic", "filtered", "all_filtered", "reviews" 중 하나)

    반환값:
        dict: 파일 목록과 메시지를 포함하는 딕셔너리
    """
    if data_type not in DATA_DIRS:
        return {
            "error": f"잘못된 데이터 타입입니다. 가능한 값: {list(DATA_DIRS.keys())}"
        }

    directory = DATA_DIRS[data_type]
    try:
        files = [f for f in os.listdir(directory) if f.endswith(".csv")]
        return {
            "message": f"{data_type} 데이터 파일 목록",
            "files": files,
            "count": len(files),
        }
    except Exception as e:
        return {"error": f"파일 목록 조회 중 오류 발생: {str(e)}"}


@app.get("/data/{data_type}/{filename}")
async def get_data_file(data_type: str, filename: str):
    """
    지정된 데이터 파일의 내용을 반환합니다.

    매개변수:
        data_type (str): 데이터 타입 ("basic", "filtered", "all_filtered", "reviews" 중 하나)
        filename (str): 파일명

    반환값:
        dict: 파일 내용과 메시지를 포함하는 딕셔너리
    """
    if data_type not in DATA_DIRS:
        return {
            "error": f"잘못된 데이터 타입입니다. 가능한 값: {list(DATA_DIRS.keys())}"
        }

    file_path = os.path.join(DATA_DIRS[data_type], filename)
    if not os.path.exists(file_path):
        return {"error": f"파일을 찾을 수 없습니다: {filename}"}

    try:
        import pandas as pd

        df = pd.read_csv(file_path)
        return {
            "message": f"{data_type} 데이터 파일 내용",
            "filename": filename,
            "row_count": len(df),
            "columns": df.columns.tolist(),
            "data": df.head(10).to_dict(orient="records"),
        }
    except Exception as e:
        return {"error": f"파일 읽기 중 오류 발생: {str(e)}"}


# 서버 시작 시 필요한 디렉토리 생성
@app.on_event("startup")
async def startup_event():
    # 데이터 디렉토리 생성
    for dir_path in DATA_DIRS.values():
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f"디렉토리 생성/확인: {dir_path}")

    logging.info("API 서버가 시작되었습니다.")


# 서버 종료 시 드라이버 정리
@app.on_event("shutdown")
async def shutdown_event():
    cleanup_driver_pool()
    logging.info("API 서버가 종료되었습니다.")


# 직접 실행 시 서버 시작
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=7070, reload=True)
