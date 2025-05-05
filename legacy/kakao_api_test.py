import requests
import json
import os
from dotenv import load_dotenv

# .env 파일에 저장된 환경 변수
load_dotenv()

# 환경 변수에서 API 키 가져오기
api_key = os.getenv('KAKAO_API_KEY')
if not api_key:
    raise ValueError("API 키가 환경 변수에 설정되지 않았습니다. .env 파일을 확인하세요.")

# 카카오맵 키워드 검색 API URL
url = "https://dapi.kakao.com/v2/local/search/keyword.json"

# 검색할 키워드 (예: 강남 카페)
params = {
    'query': '강남 카페',
    'page': 45,
    'size': 15  # 한 페이지에 최대 15개까지 반환
}

# API 요청 헤더 설정
headers = {
    "Authorization": f"KakaoAK {api_key}"
}

# API 호출
response = requests.get(url, headers=headers, params=params)

# 응답 결과 확인
if response.status_code == 200:
    result = response.json()
    print(json.dumps(result, ensure_ascii=False, indent=4))
else:
    print("API 호출 실패. 상태 코드:", response.status_code)
