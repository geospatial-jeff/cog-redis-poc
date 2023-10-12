import logging
from urllib.parse import urlsplit

import aiohttp
from redis import asyncio as aioredis

from app.config import get_settings

logger = logging.getLogger(__name__)


def get_redis_key(url: str, start: int | None = None) -> str:
    splits = urlsplit(url)
    key = f"{splits.netloc}{splits.path}"
    if start is not None:
        key += f"@{start}"
    return key


async def do_head_request(url: str, session: aiohttp.ClientSession) -> dict[str, str]:
    r = await session.head(url)
    return {
        "Content-Type": r.headers["Content-Type"],
        "Content-Length": r.headers["Content-Length"],
    }


async def do_range_request(
    url: str, start: int, end: int, session: aiohttp.ClientSession
) -> bytes:
    r = await session.get(url, headers={"Range": f"bytes={start}-{end}"})
    r.raise_for_status()
    content = await r.content.read()
    return content


async def request_and_load_into_redis(
    url: str,
    start: int,
    end: int,
    session: aiohttp.ClientSession,
    redis_client: aioredis.Redis,
):
    content = await do_range_request(url, start, end, session)
    redis_key = get_redis_key(url, start)
    await redis_client.set(redis_key, content)
    logger.debug("Cached byte range", extra={"url": url, "start": start, "end": end})


REDIS_CLIENT: aioredis.Redis | None = None


def get_redis_client() -> aioredis.Redis:
    global REDIS_CLIENT
    if not REDIS_CLIENT:
        logger.debug("Creating new redis client")
        redis_uri = get_settings().redis_uri
        REDIS_CLIENT = aioredis.from_url(redis_uri)
    return REDIS_CLIENT
