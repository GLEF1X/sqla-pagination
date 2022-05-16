from sqlalchemy import select, create_engine
from sqlalchemy.orm import sessionmaker

from examples.rest_api import Message
from sqlalchemy_pagination import LimitOffsetPaginator

engine = create_engine(
    "postgresql+psycopg2://glibgaranin:postgres@localhost:5432/test",
    echo=True
)
pool = sessionmaker(engine)

paginator = LimitOffsetPaginator(select(Message).order_by(Message.id))

with pool.begin() as session:
    stmt = paginator.get_modified_sql_statement()
    result = session.execute(stmt).all()
    page = paginator.parse_result(result)

    with paginator.bookmarked(page.next):
        stmt = paginator.get_modified_sql_statement()
        result = session.execute(stmt).all()
        new_page = paginator.parse_result(result)
