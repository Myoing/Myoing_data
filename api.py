"""
Myoing Data FastAPI 서버

이 서버는 카카오맵 기반 데이터 크롤링, 필터링, 리뷰 수집, DB 마이그레이션 등
데이터 파이프라인의 각 단계를 API 엔드포인트로 제공합니다.

주요 엔드포인트:
- POST /myoing_data/crawler/all      : 전체 파이프라인 실행 (크롤링→필터→리뷰→DB)
- POST /myoing_data/crawler/basic    : 기본 크롤러만 실행
- POST /myoing_data/crawler/filter   : 필터링만 실행
- POST /myoing_data/crawler/reviews  : 리뷰 크롤링만 실행
- POST /myoing_data/crawler/migrate  : DB 마이그레이션만 실행
- GET  /data/{data_type}             : 데이터 파일 목록 조회
- GET  /data/{data_type}/{filename}  : 데이터 파일 다운로드

실행 방법:
- python api.py
  → 0.0.0.0:7070에서 FastAPI 서버 실행

각 엔드포인트는 내부적으로 해당 단계의 main/update_data 함수를 직접 호출하여
CLI와 동일한 실행 흐름을 재현합니다.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import uvicorn
import os
from code.kakao_map_basic_crawler import main as crawl_main
from code.filters import main as filter_main
from code.review_crawler import main as review_main
from DB_code.data_updater import update_data

app = FastAPI(
    title="Myoing Data API",
    description="카카오맵 데이터 수집 및 필터링 API",
    version="2.0.0",
)

DATA_DIRS = {
    "basic": "data/1_location_categories",
    "combined": "data/2_combined_location_categories",
    "filtered": "data/3_filtered_location_categories_hour_club",
    "all_filtered": "data/4_filtered_all_hour_club",
    "review_filtered": "data/5_filtered_all_hour_club_reviewcount",
    "reviews": "data/6_reviews_about_5",
}


@app.post("/myoing_data/crawler/all")
async def run_all():
    crawl_main()
    filter_main()
    review_main()
    update_data()
    return {"result": "전체 파이프라인 완료"}


@app.post("/myoing_data/crawler/basic")
async def run_basic():
    crawl_main()
    return {"result": "기본 크롤링 완료"}


@app.post("/myoing_data/crawler/filter")
async def run_filter():
    filter_main()
    return {"result": "필터링 완료"}


@app.post("/myoing_data/crawler/reviews")
async def run_reviews():
    review_main()
    return {"result": "리뷰 크롤링 완료"}


@app.post("/myoing_data/crawler/migrate")
async def run_migrate():
    update_data()
    return {"result": "DB 마이그레이션 완료"}


@app.get("/data/{data_type}")
async def list_data_files(data_type: str):
    dir_path = DATA_DIRS.get(data_type)
    if not dir_path or not os.path.exists(dir_path):
        raise HTTPException(status_code=404, detail="데이터 타입 또는 디렉터리 없음")
    files = [
        f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))
    ]
    return {"files": files}


@app.get("/data/{data_type}/{filename}")
async def get_data_file(data_type: str, filename: str):
    dir_path = DATA_DIRS.get(data_type)
    if not dir_path:
        raise HTTPException(status_code=404, detail="데이터 타입 없음")
    file_path = os.path.join(dir_path, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="파일 없음")
    return FileResponse(file_path)


# 직접 실행 시 서버 시작
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=7070, reload=True)
