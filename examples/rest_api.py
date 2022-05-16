from datetime import datetime
from typing import cast, List, Dict, Any

import uvicorn
from fastapi import FastAPI, Depends, Query
from pydantic import BaseModel
from sqlalchemy import BIGINT, Column, VARCHAR, func, TEXT, TIMESTAMP, select
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy_pagination import KeySetPaginator, DEFAULT_PAGE_SIZE, LimitOffsetPaginator

app = FastAPI()

Base = declarative_base()

GENERATE_SHA_1_HASH = func.digest(cast(cast(func.random(), TEXT), BYTEA), 'sha1')


class Message(Base):
    __tablename__ = 'messages'
    id = Column(BIGINT, primary_key=True)
    message_hash = Column(VARCHAR(64), server_default=GENERATE_SHA_1_HASH)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class MessageSchema(BaseModel):
    id: int
    message_hash: str
    created_at: datetime


class PaginatedResult(BaseModel):
    results: List[MessageSchema]
    paging: Dict[str, Any]


engine = create_async_engine(
    "postgresql+asyncpg://glibgaranin:postgres@localhost:5432/test",
    echo=True
)
pool = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class SessionDependencyMarker:
    pass


app.dependency_overrides[SessionDependencyMarker] = lambda: pool()


@app.get("/messages/keyset", response_model=PaginatedResult)
async def read_messages_keyset(
        session: AsyncSession = Depends(SessionDependencyMarker),
        page_size: int = Query(default=DEFAULT_PAGE_SIZE),
        from_id: int = Query(default=None),
):
    bookmark = None
    if from_id is not None:
        bookmark = {"keyset_pairs": {
            "id": from_id
        }}

    paginator = KeySetPaginator(
        query_or_select=select(Message).order_by(Message.id),
        page_size=page_size,
        bookmark=bookmark
    )
    statement = paginator.get_modified_sql_statement()
    result = (await session.execute(statement)).all()
    page = paginator.parse_result(result)
    await session.commit()
    await session.close()

    return PaginatedResult(
        results=[
            MessageSchema(id=msg.id, message_hash=msg.message_hash, created_at=msg.created_at)
            for msg in page
        ],
        paging={
            "next": page.next,
            "previous": page.previous
        }
    )


@app.get("/messages/limit-offset", response_model=PaginatedResult)
async def read_messages_limit_offset(
        session: AsyncSession = Depends(SessionDependencyMarker),
        page_size: int = Query(default=DEFAULT_PAGE_SIZE),
        offset: int = Query(default=None),
):
    bookmark = {}
    if offset is not None:
        bookmark = {
            "offset": offset
        }

    paginator = LimitOffsetPaginator(
        query_or_select=select(Message).order_by(Message.id),
        page_size=page_size,
        bookmark=bookmark
    )
    statement = paginator.get_modified_sql_statement()
    result = (await session.execute(statement)).all()
    page = paginator.parse_result(result)
    await session.commit()
    await session.close()

    return PaginatedResult(
        results=[
            MessageSchema(id=msg.id, message_hash=msg.message_hash, created_at=msg.created_at)
            for msg in page
        ],
        paging={
            "next": page.next,
            "previous": page.previous,
            "current_page_number": page.current_page_number,
            "total_pages_count": page.total_pages_count,
            "has_next": page.has_next,
            "has_previous": page.has_previous,
        }
    )


if __name__ == '__main__':
    uvicorn.run(app, port=8080)
