# 📦 Myoing_data: 카카오맵 데이터 파이프라인

---

**Myoing_data**는 카카오맵을 기반으로 다양한 장소 정보 및 사용자 리뷰를 수집하고 정제하는 데이터 파이프라인 프로젝트입니다. 🚀

이 프로젝트는 특정 지역과 업종 카테고리를 기준으로 장소 데이터를 수집한 후, 다양한 필터링 과정과 상세 정보 추가(enrichment)를 통해 정제된 데이터셋을 생성합니다.

## 🔄 데이터 파이프라인 흐름

본 프로젝트는 다음과 같은 단계로 구성된 데이터 파이프라인을 통해 운영됩니다:

| 단계             | 설명                                                     | 주요 코드                                     | 출력 데이터 경로                                 |
| :--------------- | :------------------------------------------------------- | :-------------------------------------------- | :----------------------------------------------- |
| 1️⃣ **수집**      | 지역 × 업종 기반으로 카카오맵에서 가게 기본 정보 크롤링  | `code/kakao_map_basic_crawler.py`             | `data/1_location_categories/`                    |
| 2️⃣ **통합**      | 수집된 지역별/카테고리별 데이터를 단일 파일로 통합       | `code/kakao_map_basic_crawler.py` (내부 로직) | `data/2_combined_location_categories/`           |
| 3️⃣ **정제**      | 영업시간, 클럽 카테고리 필터링 적용 및 데이터 정제       | `code/filters.py`                             | `data/3_filtered_location_categories_hour_club/` |
| 4️⃣ **재통합**    | 정제된 지역별/카테고리별 데이터를 단일 파일로 통합       | `code/filters.py` (내부 로직)                 | `data/4_filtered_all_hour_club/`                 |
| 5️⃣ **추가 정제** | 리뷰 개수 기반 필터링 적용                               | `code/filters.py` (내부 로직)                 | `data/5_filtered_all_hour_club_reviewcount/`     |
| 6️⃣ **상세 수집** | 필터링된 가게 대상 상세 페이지에서 리뷰 데이터 추가 수집 | `code/review_crawler.py`                      | `data/6_reviews_about_5/`                        |
| 7️⃣ **DB 저장**   | 최종 정제된 가게 및 리뷰 데이터를 DB에 업데이트          | `DB_code/data_updater.py`                     | DB 내 `store_table`, `review_table`              |
| 8️⃣ **활용**      | 정제된 데이터는 외부 서비스/모델 학습 등에 활용 예정     | ❌ (외부 프로젝트)                            | 외부 서비스 또는 모델 학습 파이프라인            |

---

## 🚀 실행 방법

### 1. CLI 방식 (스크립트 직접 실행)

`main.py` 파일을 직접 실행하여 전체 데이터 파이프라인을 순차적으로 진행할 수 있습니다.

```bash
python main.py
```

내부적으로 다음 모듈들이 정의된 순서대로 실행됩니다:

- **1단계**: `code/kakao_map_basic_crawler.py` → 지역별 장소 기본 정보 및 리뷰 수집, 통합 데이터 생성
- **2단계**: `code/filters.py` → 영업시간/클럽 카테고리 필터링, 리뷰 수 필터링 및 통합 데이터 생성
- **3단계**: `code/review_crawler.py` → 상세 리뷰 정보 수집
- **4단계**: `DB_code/data_updater.py` → 수집 및 정제된 데이터를 데이터베이스에 업데이트

### 2. API 방식 (웹 서버 실행)

`api.py` 파일을 실행하여 FastAPI 기반의 웹 서버를 구동할 수 있습니다.

```bash
python api.py
```

서버는 기본적으로 `0.0.0.0:7070`에서 실행됩니다.

API 서버는 다음과 같은 엔드포인트를 제공하며, 각 엔드포인트는 해당 단계의 `main` 또는 `update_data` 함수를 직접 호출하여 CLI 실행과 동일한 흐름을 따릅니다:

- `POST /myoing_data/crawler/all`: 전체 크롤링 파이프라인 실행 (수집 → 필터링 → 리뷰 수집 → DB 업데이트)
- `POST /myoing_data/crawler/basic`: 기본 크롤러만 실행 (1단계 + 2단계 일부)
- `POST /myoing_data/crawler/filter`: 필터링 작업만 실행 (2단계 일부)
- `POST /myoing_data/crawler/reviews`: 리뷰 크롤링만 실행 (3단계)
- `POST /myoing_data/crawler/migrate`: DB 마이그레이션(데이터 업데이트)만 실행 (4단계)
- `GET /data/{data_type}`: 지정된 타입의 데이터 파일 목록 조회
- `GET /data/{data_type}/{filename}`: 지정된 데이터 파일 다운로드

### 3. Docker 방식 (컨테이너 실행)

`docker-compose.yaml` 파일을 사용하여 Docker 컨테이너 환경에서 서비스를 실행할 수 있습니다.

```bash
docker-compose up
```

이 명령어를 통해 API 서버 및 필요한 서비스(예: MySQL DB)가 함께 실행되며, 포트 `7070`으로 API 서버에 접근할 수 있습니다.

---
