from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    Time,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# [1] Store 테이블 정의
class Store(Base):
    __tablename__ = "store_table"

    # 복합 기본키 구성
    str_name = Column(String(255))
    str_address = Column(String(255))

    # PrimaryKeyConstraint는 __table_args__로 명시
    __table_args__ = (
        PrimaryKeyConstraint("str_name", "str_address"),
    )

    str_location_keyword = Column(String(100))
    str_main_category = Column(String(100))
    str_sub_category = Column(String(100))
    i_star_point_count = Column(Integer)
    f_star_point = Column(Float)
    i_review_count = Column(Integer)
    run_day = Column(String(50))
    run_time_start = Column(Time)
    run_time_end = Column(Time)
    str_url = Column(String(255))
    str_telephone = Column(String(50))

    # 관계 설정
    reviews = relationship("Review", back_populates="store")


# [2] Review 테이블 정의
class Review(Base):
    __tablename__ = "review_table"

    reviewer_name = Column(String(100))
    review_date = Column(DateTime)

    str_name = Column(String(255))
    str_address = Column(String(255))

    str_location_keyword = Column(String(100))
    str_main_category = Column(String(100))
    reviewer_score = Column(Float)
    review_content = Column(String(1000))

    # 관계 설정
    store = relationship("Store", back_populates="reviews")

    # 복합 기본키 + 복합 외래키 정의
    __table_args__ = (
        PrimaryKeyConstraint("reviewer_name", "review_date"),
        ForeignKeyConstraint(
            ["str_name", "str_address"],
            ["store_table.str_name", "store_table.str_address"]
        ),
    )