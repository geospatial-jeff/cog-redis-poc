import asyncio
import json
import logging

import aiohttp
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from starlette import status

from app.cog import extract_byte_ranges, is_bigtiff
from app.io import (
    do_head_request,
    do_range_request,
    get_redis_client,
    get_redis_key,
    request_and_load_into_redis
)

app = FastAPI()
logger = logging.getLogger(__name__)


def parse_range_header(request: Request) -> tuple[int, int]:
    range_header = request.headers.get("Range")
    if not range_header:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must include `Range` header",
        )
    _, byte_range = range_header.split("=")
    start, end = byte_range.split("-")
    logger.debug("Parsed range header", extra={"start": start, "end": end})
    return int(start), int(end)


@app.post("/")
async def create_cog(url: str = Query(...)):
    redis_client = get_redis_client()

    async with aiohttp.ClientSession() as session:
        file_header = await do_range_request(url, 0, 16383, session)

    if is_bigtiff(file_header):
        return Response(
            "BIGTIFF is not supported", status_code=status.HTTP_400_BAD_REQUEST
        )

    # Store TIFF header in redis
    await redis_client.set(get_redis_key(url, 0), file_header)

    # Extract byte ranges for each block, store in redis.
    byte_ranges = extract_byte_ranges(file_header)
    await asyncio.gather(
        *[
            request_and_load_into_redis(
                url,
                offset,
                offset + byte_count,
                session,
                redis_client,
            )
            for (offset, byte_count) in byte_ranges
        ]
    )

    # Store some metadata about the file in redis.
    metadata = await do_head_request(url, session)
    await redis_client.set(get_redis_key(url), json.dumps(metadata))


@app.head("/{path:path}")
async def head_cog(path: str):
    redis_client = get_redis_client()
    headers = await redis_client.get(path)
    if not headers:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    return Response(headers=json.loads(headers), status_code=status.HTTP_200_OK)


@app.get("/{path:path}")
async def read_cog(
    path: str, range_header: tuple[int, int] = Depends(parse_range_header)
):
    redis_client = get_redis_client()
    content = await redis_client.get(f"{path}@{range_header[0]}")
    if not content:
        return Response(status_code=status.HTTP_404_NOT_FOUND)

    return Response(
        content,
        status_code=status.HTTP_206_PARTIAL_CONTENT,
        media_type="binary/octet-stream"
    )
