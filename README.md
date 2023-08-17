<div>
    <img src="assets/sqlalchemy.png" alt="sql-alchemy" height="60" /> pagination for humans
</div>

## Features

* Support both async and sync sqlalchemy approaches without corrupting and duplicating API
* Include variety of different pagination strategies such as `keyset(infinite scrolling)`, `limit-offset` and others
* Support PEP 484 (typehints) and consequentially static type checking using `mypy`, `pyright` or other tool
* Transparent page abstraction

### Getting started

 ```python
from sqlalchemy import select, create_engine
from sqlalchemy.orm import sessionmaker

from sqlapagination import LimitOffsetPaginator

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

### What do bookmarks look like?

Bookmark is a plain dict, but for different pagination strategies
a dict's payload differ from each other

#### Keyset:

```python
{
    "keyset_pairs": {
        "id": (1,)
    },
    "direction": "forward",
}
```

#### Limit-offset:
```python
{
    "offset": 10000,
}
```

#### Limitations:

* _Golden Rule_: Always ensure your keysets are unique per row. If you violate this condition you risk skipped rows and other nasty problems. The simplest way to do this is to always include your primary key column(s) at the end of your ordering columns.
* Any rows containing null values in their keysets will be omitted from the results, so your ordering columns should be NOT NULL. (This is a consequence of the fact that comparisons against NULL are always false in SQL.) This may change in the future if we work out an alternative implementation; but for now we recommend using coalesce as a workaround if you need to sort by nullable columns:
