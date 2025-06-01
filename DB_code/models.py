from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    Time,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# [1] Store 테이블 정의
class Store(Base):
    __tablename__ = "store_table"

    # 복합 기본키 구성
    str_name = Column(String(255))  # 가게 이름
    str_address = Column(String(255))  # 가게 주소

    # PrimaryKeyConstraint는 __table_args__로 명시
    __table_args__ = (PrimaryKeyConstraint("str_name", "str_address"),)

    str_location_keyword = Column(String(100))  # 위치 키워드
    str_main_category = Column(String(100))  # 주요 카테고리
    str_sub_category = Column(String(100))  # 하위 카테고리
    i_star_point_count = Column(Integer)  # 별점 수
    f_star_point = Column(Float)  # 평균 별점
    i_review_count = Column(Integer)  # 리뷰 수
    run_day = Column(String(50))  # 영업 요일
    run_time_start = Column(Time)  # 영업 시작 시간
    run_time_end = Column(Time)  # 영업 종료 시간
    str_url = Column(String(255))  # 가게 URL
    str_telephone = Column(String(50))  # 전화번호

    # 관계 설정 (1:N)
    reviews = relationship("Review", back_populates="store")


# [2] Review 테이블 정의
class Review(Base):
    __tablename__ = "review_table"

    reviewer_name = Column(String(100))  # 리뷰어 이름
    review_date = Column(DateTime)  # 리뷰 날짜

    str_name = Column(String(255))  # 가게 이름 (외래키)
    str_address = Column(String(255))  # 가게 주소 (외래키)

    str_location_keyword = Column(String(100))  # 위치 키워드
    str_main_category = Column(String(100))  # 주요 카테고리
    reviewer_score = Column(Float)  # 리뷰어가 준 점수
    review_content = Column(Text)  # 긴 텍스트 대응을 위한 Text 타입 사용

    # 관계 설정 (N:1)
    store = relationship("Store", back_populates="reviews")

    # 복합 기본키 + 복합 외래키 정의
    __table_args__ = (
        PrimaryKeyConstraint("reviewer_name", "review_date"),
        ForeignKeyConstraint(
            ["str_name", "str_address"],
            ["store_table.str_name", "store_table.str_address"],
        ),
    )

# [3] UserFeedback 테이블 정의
class UserFeedback(Base):
    __tablename__ = "user_feedback"

    id = Column(Integer, primary_key=True)  # PK: id
    name = Column(String(150))  # 사용자 이름
    satisfaction_score = Column(Integer)  # 만족도 점수
    review = Column(Text)  # 설문 리뷰
