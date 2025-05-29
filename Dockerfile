FROM python:3.10-slim

# 작업 디렉토리 설정
WORKDIR /app

# 크롬 설치 (보안 개선 방식으로 변경)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ChromeDriver는 런타임에 webdriver-manager가 자동으로 관리함

# 필요한 디렉토리 구조 생성
RUN mkdir -p /app/data/1_location_categories \
    /app/data/2_merged_location_categories \
    /app/data/3_filtered_location_categories \
    /app/data/4_filtered_all \
    /app/data/5_club_filter_results \
    /app/data/6_reviews_about_4

# 필요한 파이썬 패키지 설치를 위한 requirements.txt 복사 및 설치
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 전체 소스 복사
COPY . .

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# 포트 노출
EXPOSE 7070

# 애플리케이션 실행
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "7070"] 