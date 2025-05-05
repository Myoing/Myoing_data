"""
Myoing Data 크롤링 패키지.

설명:
    카카오맵 데이터 수집 및 분석을 위한 모듈 모음.
    각 모듈은 데이터 수집, 필터링, 리뷰 분석 등의 기능 수행.

구성 모듈:
    - kakao_map_basic_crawler: 카카오맵 기본 정보 크롤링 모듈
    - filters: 수집 데이터 필터링 및 가공 모듈
    - review_crawler: 가게 리뷰 정보 크롤링 모듈

사용법:
    import code.kakao_map_basic_crawler as basic_crawler
    import code.filters as filters
    import code.review_crawler as review_crawler

입력값:
    없음 (패키지 임포트 시)

반환값:
    없음 (패키지 초기화 파일)

작업 흐름:
    1. kakao_map_basic_crawler: 지역별, 카테고리별 데이터 수집
    2. filters: 수집된 데이터 필터링 및 상세 정보 보강
    3. review_crawler: 필터링된 가게의 리뷰 데이터 수집

데이터 저장 구조:
    - data/1_location_categories/: 1단계 기본 크롤링 데이터
    - data/2_combined_location_categories/: 통합 데이터
    - data/3_filtered_location_categories/: 필터링된 데이터
    - data/4_filtered_all/: 전체 필터링 데이터
    - data/5_filtered_clubs/: 클럽 관련 필터링 데이터
    - data/6_reviews_about_4/: 리뷰 데이터
"""
