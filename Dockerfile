FROM python:3.10-slim

# 작업 디렉토리 설정
WORKDIR /app

# 크롬 및 관련 종속성 설치
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 크롬드라이버 설치 - 수정된 방식
RUN apt-get update && apt-get install -y unzip \
    && CHROME_MAJOR_VERSION=$(google-chrome-stable --version | sed 's/Google Chrome \([0-9]*\).*/\1/') \
    && CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_MAJOR_VERSION}") \
    && wget -q "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip" \
    && unzip chromedriver_linux64.zip \
    && mv chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm chromedriver_linux64.zip

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

# 애플리케이션 코드 복사
COPY api.py /app/
COPY code/ /app/code/

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# 포트 노출
EXPOSE 8000

# 애플리케이션 실행
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"] 