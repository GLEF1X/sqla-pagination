from sqlalchemy import select, asc, desc

from sqlalchemy_pagination import KeySetPaginator, DEFAULT_PAGE_SIZE
from tests.conftest import Author, Book


class TestGetModifiedSqlStatement:

    def test_get_modified_sql_statement(self):
        paginator = KeySetPaginator(select(Author).order_by(Author.id))

        compiled_paginators_query = paginator.get_modified_sql_statement().compile().string
        compiled_expected_query = select(Author).order_by(asc(Author.id)).limit(
            DEFAULT_PAGE_SIZE
        ).compile().string

        assert compiled_paginators_query == compiled_expected_query

    def test_get_modified_sql_statement_with_forward_bookmark(self):
        paginator = KeySetPaginator(
            select(Author).order_by(Author.id),
            bookmark={
                "keyset_pairs": {
                    "id": 10,
                },
                "direction": "forward",
            }
        )

        compiled_paginators_query = paginator.get_modified_sql_statement().compile().string
        compiled_expected_query = select(Author).order_by(asc(Author.id)).limit(DEFAULT_PAGE_SIZE).where(
            Author.id > 10
        ).compile().string

        assert compiled_paginators_query == compiled_expected_query

    def test_get_modified_sql_statement_with_backward_bookmark(self):
        paginator = KeySetPaginator(
            select(Author).order_by(Author.id),
            bookmark={
                "keyset_pairs": {
                    "id": 10,
                },
                "direction": "backward",
            }
        )

        compiled_paginators_query = paginator.get_modified_sql_statement().compile().string
        compiled_expected_query = select(Author).order_by(desc(Author.id)).limit(DEFAULT_PAGE_SIZE).where(
            Author.id < 10
        ).compile().string

        assert compiled_paginators_query == compiled_expected_query

    def test_get_modified_sql_statement_with_join(self):
        paginator = KeySetPaginator(
            select(Author, Book).order_by(Author.id, Book.id,
                                          Book.published_at).outerjoin(
                Author.books
            )
        )

        compiled_pagination_query = paginator.get_modified_sql_statement().compile().string
        compiled_expected_query = select(Author, Book).order_by(
            asc(Author.id), asc(Book.id), asc(Book.published_at)
        ).outerjoin(Author.books).limit(DEFAULT_PAGE_SIZE).compile().string
        assert compiled_pagination_query == compiled_expected_query

    def test_with_column_property(self):
        paginator = KeySetPaginator(
            select(Author, Book).outerjoin(
                Author.books
            ).order_by(Book.popularity)
        )

        compiled_pagination_query = paginator.get_modified_sql_statement().compile().string
        compiled_expected_query = select(Author, Book).outerjoin(
            Author.books
        ).order_by(Book.popularity.element).limit(DEFAULT_PAGE_SIZE).compile().string

        assert compiled_pagination_query == compiled_expected_query
