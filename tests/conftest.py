import enum
from datetime import timedelta
from random import randrange

from sqlalchemy import String, Column, Integer, func, ForeignKey, select, Enum
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import declarative_base, column_property, relationship
from sqlalchemy_utils import ArrowType
from sqlalchemy_utils.types.arrow import arrow

Base = declarative_base()


def randtime():
    return arrow.now() - timedelta(seconds=randrange(86400))


class MyInteger(float):
    pass


class Colour(enum.Enum):
    red = 0
    green = 1
    blue = 2


class Light(Base):
    __tablename__ = "lights"
    id = Column(Integer, primary_key=True)
    intensity = Column(Integer, nullable=False)
    colour = Column(Enum(Colour), nullable=False)


class Book(Base):
    __tablename__ = "books"
    id = Column("book_id", Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    a = Column(Integer)
    b = Column(Integer, nullable=False)
    c = Column(Integer, nullable=False)
    d = Column(Integer, nullable=False)
    author_id = Column(Integer, ForeignKey("authors.id"))
    prequel_id = Column(Integer, ForeignKey(id), nullable=True)
    prequel = relationship("Book", remote_side=[id], backref="sequel", uselist=False)
    published_at = Column(ArrowType, default=randtime, nullable=False)

    popularity = column_property(b + c * d)

    @hybrid_property
    def score(self):
        return self.b * self.c - self.d


class Author(Base):
    __tablename__ = "authors"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    books = relationship("Book", backref="author")
    info = Column(String(255), nullable=False)

    @hybrid_property
    def book_count(self) -> int:
        return len(self.books)

    @book_count.expression
    def book_count(cls):
        return (
            select(func.count(Book.id))
                .where(Book.author_id == cls.id)
                .label("book_count")
        )
