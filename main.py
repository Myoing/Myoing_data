import logging
import subprocess

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crawler.log"),
    ],
)


def main():
    try:
        # 1단계: 기본 크롤링 (kakao_map_basic_crawler.py)
        logging.info("====== 1단계: 기본 크롤링 작업 시작 ======")
        subprocess.run(["python", "code/kakao_map_basic_crawler.py"])
        logging.info("====== 1단계: 기본 크롤링 작업 완료 ======")

        # 2단계: 필터링 (filters.py)
        logging.info("====== 2단계: 필터링 작업 시작 ======")
        subprocess.run(["python", "code/filters.py"])
        logging.info("====== 2단계: 필터링 작업 완료 ======")

        # 3단계: 리뷰 크롤링 (review_crawler.py)
        logging.info("====== 3단계: 리뷰 크롤링 시작 ======")
        subprocess.run(["python", "code/review_crawler.py"])
        logging.info("====== 3단계: 리뷰 크롤링 완료 ======")

        # 4단계: 데이터베이스 업데이트
        logging.info("====== 4단계: 데이터베이스 업데이트 시작 ======")
        subprocess.run(["python", "DB_code/data_updater.py"])
        logging.info("====== 4단계: 데이터베이스 업데이트 완료 ======")

        logging.info("모든 작업이 완료되었습니다!")

    except Exception as e:
        logging.error(f"실행 중 오류 발생: {e}")


if __name__ == "__main__":
    main()
