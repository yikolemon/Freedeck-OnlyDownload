"""SteamGridDB API v2 client helpers for artwork fetching."""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional
from urllib.parse import quote

import aiohttp

import config

SGDB_API_BASE = "https://www.steamgriddb.com/api/v2"
SGDB_ENV_API_KEY = "FREDECK_STEAMGRIDDB_API_KEY"
# NOTE: 该 Key 会被打包进插件文件中，任何安装插件的人都可以提取并滥用。
# 如果你计划分发插件，强烈建议改用环境变量 FREDECK_STEAMGRIDDB_API_KEY。
SGDB_DEFAULT_API_KEY = "3d057a658242e107af945bff275c9406"


def _normalize_steamgriddb_api_key(raw: str) -> str:
    token = str(raw or "").strip()
    if not token:
        return ""
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if token.lower().startswith("key") and re.fullmatch(r"[0-9a-fA-F]{32}", token[3:] or ""):
        token = token[3:].strip()
    return token


def resolve_steamgriddb_api_key(stored: str = "") -> str:
    """Resolve SteamGridDB API key.

    Priority:
    1) env `FREDECK_STEAMGRIDDB_API_KEY`
    2) stored settings value
    3) plugin default key
    """
    override = _normalize_steamgriddb_api_key(os.environ.get(SGDB_ENV_API_KEY, ""))
    if override:
        return override
    candidate = _normalize_steamgriddb_api_key(stored)
    if candidate:
        return candidate
    return _normalize_steamgriddb_api_key(SGDB_DEFAULT_API_KEY)


def _coerce_url(value: Any) -> str:
    url = str(value or "").strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return ""


async def _sgdb_get_json(
    *,
    session: aiohttp.ClientSession,
    api_key: str,
    path: str,
    params: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    if not api_key:
        return {}
    url = f"{SGDB_API_BASE}{path}"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
        "Referer": "https://www.steamgriddb.com/",
    }
    try:
        async with session.get(url, headers=headers, params=params or {}, ssl=False) as resp:
            if int(resp.status) != 200:
                return {"_http_status": int(resp.status)}
            payload = await resp.json(content_type=None)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _pick_top_url(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict) or not bool(payload.get("success", False)):
        return ""
    items = payload.get("data")
    if not isinstance(items, list) or not items:
        return ""
    first = items[0] if isinstance(items[0], dict) else {}
    return _coerce_url(first.get("url"))


async def search_steamgriddb_autocomplete(
    *,
    api_key: str,
    term: str,
) -> Dict[str, Any]:
    """Search SteamGridDB games by keyword (autocomplete).

    Returns a dict with keys:
    - ok (bool)
    - message (str)
    - games (list[dict])
    """
    key = str(api_key or "").strip()
    keyword = str(term or "").strip()
    if not key or not keyword:
        return {"ok": False, "message": "SteamGridDB API key 缺失或关键词为空", "games": []}

    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/search/autocomplete/{quote(keyword, safe='')}",
            params={},
        )

    if not isinstance(payload, dict) or not bool(payload.get("success", False)):
        http_status = payload.get("_http_status") if isinstance(payload, dict) else None
        suffix = f" (HTTP {int(http_status)})" if http_status else ""
        return {
            "ok": False,
            "message": f"SteamGridDB 搜索失败{suffix}",
            "games": [],
            "http_status": int(http_status or 0),
        }

    items = payload.get("data")
    if not isinstance(items, list) or not items:
        return {"ok": False, "message": "SteamGridDB 未返回匹配结果", "games": [], "http_status": 0}
    games = [item for item in items if isinstance(item, dict)]
    return {"ok": True, "message": "", "games": games, "http_status": 0}


async def resolve_steamgriddb_artwork(
    *,
    api_key: str,
    steam_app_id: int,
) -> Dict[str, Any]:
    """Resolve Steam artwork URLs from SteamGridDB by Steam app id.

    Returns a dict with keys:
    - ok (bool)
    - landscape / portrait / hero / logo / icon (urls)
    """
    key = str(api_key or "").strip()
    app = int(steam_app_id or 0)
    if not key or app <= 0:
        return {
            "ok": False,
            "message": "SteamGridDB API key 缺失或 Steam AppID 无效",
            "landscape": "",
            "portrait": "",
            "hero": "",
            "logo": "",
            "icon": "",
        }

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        base_filters = {
            "types": "static",
            "nsfw": "false",
            "humor": "false",
            "epilepsy": "false",
            "limit": "1",
        }

        statuses = []
        landscape_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/grids/steam/{app}",
            params={
                **base_filters,
                "dimensions": "920x430,460x215",
                "mimes": "image/png,image/jpeg",
            },
        )
        if isinstance(landscape_payload, dict) and landscape_payload.get("_http_status"):
            statuses.append(int(landscape_payload.get("_http_status") or 0))
        landscape = _pick_top_url(landscape_payload)

        portrait_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/grids/steam/{app}",
            params={
                **base_filters,
                "dimensions": "600x900,660x930,342x482",
                "mimes": "image/png,image/jpeg",
            },
        )
        if isinstance(portrait_payload, dict) and portrait_payload.get("_http_status"):
            statuses.append(int(portrait_payload.get("_http_status") or 0))
        portrait = _pick_top_url(portrait_payload)

        hero_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/heroes/steam/{app}",
            params={
                **base_filters,
                "dimensions": "1920x620,3840x1240,1600x650",
                "mimes": "image/png,image/jpeg",
            },
        )
        if isinstance(hero_payload, dict) and hero_payload.get("_http_status"):
            statuses.append(int(hero_payload.get("_http_status") or 0))
        hero = _pick_top_url(hero_payload)

        logo_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/logos/steam/{app}",
            params={**base_filters, "styles": "official,white,black,custom", "mimes": "image/png"},
        )
        if isinstance(logo_payload, dict) and logo_payload.get("_http_status"):
            statuses.append(int(logo_payload.get("_http_status") or 0))
        logo = _pick_top_url(logo_payload)

        icon_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/icons/steam/{app}",
            params={
                **base_filters,
                "styles": "official,custom",
                "dimensions": "256,512,128",
                "mimes": "image/png",
            },
        )
        if isinstance(icon_payload, dict) and icon_payload.get("_http_status"):
            statuses.append(int(icon_payload.get("_http_status") or 0))
        icon = _pick_top_url(icon_payload)

    ok = bool(landscape or portrait or hero or logo or icon)
    if not ok:
        config.logger.info("SteamGridDB artwork not found for app=%s", app)

    http_status = max(statuses) if statuses else 0
    return {
        "ok": ok,
        "message": "" if ok else "未从 SteamGridDB 获取到素材",
        "landscape": landscape,
        "portrait": portrait,
        "hero": hero,
        "logo": logo,
        "icon": icon,
        "http_status": int(http_status or 0),
    }


async def resolve_steamgriddb_artwork_by_game_id(
    *,
    api_key: str,
    game_id: int,
) -> Dict[str, Any]:
    """Resolve Steam artwork URLs from SteamGridDB by SteamGridDB game id.

    Returns a dict with keys:
    - ok (bool)
    - landscape / portrait / hero / logo / icon (urls)
    """
    key = str(api_key or "").strip()
    gid = int(game_id or 0)
    if not key or gid <= 0:
        return {
            "ok": False,
            "message": "SteamGridDB API key 缺失或 game_id 无效",
            "landscape": "",
            "portrait": "",
            "hero": "",
            "logo": "",
            "icon": "",
        }

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        base_filters = {
            "types": "static",
            "nsfw": "false",
            "humor": "false",
            "epilepsy": "false",
            "limit": "1",
        }

        statuses = []
        landscape_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/grids/game/{gid}",
            params={
                **base_filters,
                "dimensions": "920x430,460x215",
                "mimes": "image/png,image/jpeg",
            },
        )
        if isinstance(landscape_payload, dict) and landscape_payload.get("_http_status"):
            statuses.append(int(landscape_payload.get("_http_status") or 0))
        landscape = _pick_top_url(landscape_payload)

        portrait_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/grids/game/{gid}",
            params={
                **base_filters,
                "dimensions": "600x900,660x930,342x482",
                "mimes": "image/png,image/jpeg",
            },
        )
        if isinstance(portrait_payload, dict) and portrait_payload.get("_http_status"):
            statuses.append(int(portrait_payload.get("_http_status") or 0))
        portrait = _pick_top_url(portrait_payload)

        hero_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/heroes/game/{gid}",
            params={
                **base_filters,
                "dimensions": "1920x620,3840x1240,1600x650",
                "mimes": "image/png,image/jpeg",
            },
        )
        if isinstance(hero_payload, dict) and hero_payload.get("_http_status"):
            statuses.append(int(hero_payload.get("_http_status") or 0))
        hero = _pick_top_url(hero_payload)

        logo_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/logos/game/{gid}",
            params={**base_filters, "styles": "official,white,black,custom", "mimes": "image/png"},
        )
        if isinstance(logo_payload, dict) and logo_payload.get("_http_status"):
            statuses.append(int(logo_payload.get("_http_status") or 0))
        logo = _pick_top_url(logo_payload)

        icon_payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/icons/game/{gid}",
            params={
                **base_filters,
                "styles": "official,custom",
                "dimensions": "256,512,128",
                "mimes": "image/png",
            },
        )
        if isinstance(icon_payload, dict) and icon_payload.get("_http_status"):
            statuses.append(int(icon_payload.get("_http_status") or 0))
        icon = _pick_top_url(icon_payload)

    ok = bool(landscape or portrait or hero or logo or icon)
    if not ok:
        config.logger.info("SteamGridDB artwork not found for game=%s", gid)

    http_status = max(statuses) if statuses else 0
    return {
        "ok": ok,
        "message": "" if ok else "未从 SteamGridDB 获取到素材",
        "landscape": landscape,
        "portrait": portrait,
        "hero": hero,
        "logo": logo,
        "icon": icon,
        "http_status": int(http_status or 0),
    }


async def resolve_steamgriddb_portrait_grid(
    *,
    api_key: str,
    steam_app_id: int,
) -> Dict[str, Any]:
    """Resolve Steam portrait grid URL from SteamGridDB by Steam app id.

    Compared to `resolve_steamgriddb_artwork`, this only fetches portrait grid
    and is designed for high-frequency list rendering.
    """
    key = str(api_key or "").strip()
    app = int(steam_app_id or 0)
    if not key or app <= 0:
        return {
            "ok": False,
            "message": "SteamGridDB API key 缺失或 Steam AppID 无效",
            "portrait": "",
        }

    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/grids/steam/{app}",
            params={
                "types": "static",
                "nsfw": "false",
                "humor": "false",
                "epilepsy": "false",
                "limit": "1",
                "dimensions": "600x900,660x930,342x482",
                "mimes": "image/png,image/jpeg",
            },
        )
        portrait = _pick_top_url(payload)
        http_status = int(payload.get("_http_status") or 0) if isinstance(payload, dict) else 0

    ok = bool(portrait)
    if not ok:
        config.logger.info("SteamGridDB portrait grid not found for app=%s", app)

    return {
        "ok": ok,
        "message": "" if ok else "未从 SteamGridDB 获取到竖版封面",
        "portrait": portrait,
        "http_status": int(http_status or 0),
    }


async def resolve_steamgriddb_portrait_grid_by_game_id(
    *,
    api_key: str,
    game_id: int,
) -> Dict[str, Any]:
    """Resolve Steam portrait grid URL from SteamGridDB by SteamGridDB game id."""
    key = str(api_key or "").strip()
    gid = int(game_id or 0)
    if not key or gid <= 0:
        return {
            "ok": False,
            "message": "SteamGridDB API key 缺失或 game_id 无效",
            "portrait": "",
        }

    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        payload = await _sgdb_get_json(
            session=session,
            api_key=key,
            path=f"/grids/game/{gid}",
            params={
                "types": "static",
                "nsfw": "false",
                "humor": "false",
                "epilepsy": "false",
                "limit": "1",
                "dimensions": "600x900,660x930,342x482",
                "mimes": "image/png,image/jpeg",
            },
        )
        portrait = _pick_top_url(payload)
        http_status = int(payload.get("_http_status") or 0) if isinstance(payload, dict) else 0

    ok = bool(portrait)
    if not ok:
        config.logger.info("SteamGridDB portrait grid not found for game=%s", gid)

    return {
        "ok": ok,
        "message": "" if ok else "未从 SteamGridDB 获取到竖版封面",
        "portrait": portrait,
        "http_status": int(http_status or 0),
    }
