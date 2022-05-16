# SQLAlchemy pagination for humans

<div>
    <img src="assets/sqlalchemy.png" alt="sql-alchemy" height="60" />
</div>

## Features

* Support both async and sync sqlalchemy approaches without corrupting and duplicating API
* Include variety of different pagination strategies such as `keyset`, `limit-offset` and others
* Support PEP 484 (typehints) and consuconsequently static type checking using `mypy`, `pyright` or other tool
* Transparent page abstraction
* Highly tested(not yet)

Quick example with sync `sqlalchemy`:

 ```python
from sqlalchemy import select, create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy_pagination import LimitOffsetPaginator

engine = create_engine("connection url")
pool = sessionmaker(engine)

paginator = LimitOffsetPaginator(select(Book).order_by(Book.id, Book.title))

with pool.begin() as session:
    stmt = paginator.get_modified_sql_statement()
    result = session.execute(stmt).all()
    page = paginator.parse_result(result)

    with paginator.bookmarked(page.next):
        stmt = paginator.get_modified_sql_statement()
        result = session.execute(stmt).all()
        new_page = paginator.parse_result(result)

```
