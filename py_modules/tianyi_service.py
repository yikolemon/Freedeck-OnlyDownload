# tianyi_service.py - 天翼下载业务编排
#
# 该模块串联目录、登录态、直链与下载任务。

from __future__ import annotations

import asyncio
import base64
import csv
import configparser
import copy
import json
import os
import pwd
import re
import shlex
import shutil
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, quote, urlparse

import aiohttp
import decky
from yarl import URL

import config
from aria2_manager import Aria2Error, Aria2Manager
from gba_catalog import GbaCatalog, resolve_default_gba_catalog_path
from game_catalog import GameCatalog, resolve_default_catalog_path
from seven_zip_manager import SevenZipCancelledError, SevenZipError, SevenZipManager
from steamgriddb_client import (
    resolve_steamgriddb_api_key,
    resolve_steamgriddb_artwork,
    resolve_steamgriddb_artwork_by_game_id,
    resolve_steamgriddb_portrait_grid,
    resolve_steamgriddb_portrait_grid_by_game_id,
    search_steamgriddb_autocomplete,
)
from steam_shortcuts import (
    add_or_update_tianyi_shortcut,
    list_tianyi_shortcuts_sync,
    migrate_tianyi_shortcut_tokens_sync,
    remove_tianyi_shortcut,
    resolve_tianyi_shortcut_sync,
)
from baidu_client import BaiduApiError, fetch_baidu_download_url, resolve_baidu_share
from ctfile_client import CtfileApiError, fetch_ctfile_download_url, resolve_ctfile_file_infos, resolve_ctfile_share
from tianyi_client import (
    TianyiApiError,
    download_cloud_archive,
    fetch_access_token,
    fetch_download_url,
    get_user_account,
    list_cloud_archives,
    resolve_share,
    upload_archive_to_cloud,
)
from tianyi_store import TianyiInstalledGame, TianyiStateStore, TianyiTaskRecord


class InstallCancelledError(RuntimeError):
    """安装流程被用户取消。"""


TASK_RETENTION_SECONDS = 7 * 24 * 3600
PANEL_TASK_REFRESH_TIMEOUT_SECONDS = 2.0
LOCAL_WEB_READY_TIMEOUT_SECONDS = 3.0
LOCAL_WEB_PROBE_TIMEOUT_SECONDS = 1.2
LOGIN_STATE_CACHE_SECONDS = 300
SHARE_RESOLVE_CACHE_SECONDS = 600
CAPTURE_DEFAULT_TIMEOUT_SECONDS = 240
CAPTURE_MIN_TIMEOUT_SECONDS = 30
CAPTURE_MAX_TIMEOUT_SECONDS = 600
CDP_ENDPOINT_PORTS = (8080, 9222)
TIANYI_HOST_KEYWORDS = ("cloud.189.cn", "h5.cloud.189.cn", "open.e.189.cn", "189.cn")
BAIDU_HOST_KEYWORDS = ("pan.baidu.com", "passport.baidu.com", "baidu.com")
CAPTURE_LOOP_WINDOW = 8
CAPTURE_LOOP_CORE_HOSTS = ("cloud.189.cn", "h5.cloud.189.cn")
COOKIE_CAPTURE_SOURCES = ("cdp", "cookie_db")
COOKIE_DB_MAX_ROWS = 600
QR_LOGIN_SESSION_TIMEOUT_SECONDS = 300
QR_LOGIN_HTTP_TIMEOUT_SECONDS = 20
CATALOG_COVER_CACHE_TTL_SECONDS = 7 * 24 * 3600
CATALOG_COVER_NEGATIVE_TTL_SECONDS = 1800
CATALOG_COVER_HTTP_TIMEOUT_SECONDS = 6.0
CATALOG_COVER_SEARCH_LIMIT = 14
CATALOG_COVER_MIN_SCORE = 25
CATALOG_COVER_STRICT_MIN_SCORE = 115
CATALOG_COVER_STRICT_KEY_COVERAGE = 0.85
CATALOG_COVER_VALIDATE_MAX_CANDIDATES = 10
STEAM_APPDETAILS_HTTP_TIMEOUT_SECONDS = 4.0
STEAMGRIDDB_DISABLE_SECONDS = 600
CATALOG_DEFAULT_DATE = "2026-02-23"
CATALOG_UPDATE_HTTP_TIMEOUT_SECONDS = 12.0
CATALOG_UPDATE_MAX_BYTES = 64 * 1024 * 1024
CATALOG_UPDATE_GITHUB_API_URL = "https://api.github.com/repos/panyiwei-home/Freedeck/contents/gl"
CATALOG_UPDATE_USER_AGENT = "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)"
CATALOG_UPDATE_DOWNLOAD_RETRIES = 3
CATALOG_UPDATE_RETRY_BACKOFF_SECONDS = 1.0
CATALOG_UPDATE_TRANSIENT_STATUSES = {502, 503, 504, 522, 524}
CATALOG_LOCAL_FILENAME = "freedeck_catalog.csv"
PROTONDB_HTTP_TIMEOUT_SECONDS = 4.5
HLTB_CACHE_TTL_SECONDS = 7 * 24 * 3600
HLTB_NEGATIVE_TTL_SECONDS = 20
HLTB_HTTP_TIMEOUT_SECONDS = 4.0
HLTB_SEARCH_LIMIT = 12
HLTB_TOKEN_CACHE_SECONDS = 30 * 60
HLTB_TOKEN_URL = "https://howlongtobeat.com/api/finder/init"
HLTB_SEARCH_URL = "https://howlongtobeat.com/api/finder"
HLTB_LEGACY_SEARCH_URL = "https://howlongtobeat.com/api/search"
PANEL_POLL_MODE_ACTIVE = "active"
PANEL_POLL_MODE_IDLE = "idle"
PANEL_POLL_MODE_BACKGROUND = "background"
PANEL_TASK_REFRESH_ACTIVE_SECONDS = 1.0
PANEL_TASK_REFRESH_IDLE_SECONDS = 10.0
PANEL_TASK_REFRESH_BACKGROUND_SECONDS = 30.0
PANEL_INSTALLED_REFRESH_ACTIVE_SECONDS = 20.0
PANEL_INSTALLED_REFRESH_IDLE_SECONDS = 60.0
SWITCH_EMULATOR_DISPLAY_NAME = "Eden"
EDEN_CONFIG_DIR = os.path.realpath(os.path.expanduser("~/.config/eden"))
EDEN_QT_CONFIG_PATH = os.path.join(EDEN_CONFIG_DIR, "qt-config.ini")
EDEN_DATA_DIR = os.path.realpath(os.path.expanduser("~/.local/share/eden"))
EDEN_KEYS_DIR = os.path.join(EDEN_DATA_DIR, "keys")
EDEN_UI_LANGUAGE = "zh_CN"
EDEN_SYSTEM_LANGUAGE_INDEX = "15"
EDEN_USER_DIRNAME = "user"
EDEN_SAVE_RELATIVE_PATH = os.path.join("nand", "user", "save")
PANEL_INSTALLED_REFRESH_BACKGROUND_SECONDS = 120.0

QR_STATUS_SUCCESS = 0
QR_STATUS_WAITING = {-106}
QR_STATUS_SCANNED_WAIT_CONFIRM = {-11002}
QR_STATUS_EXPIRED = {-11001, -20099}
QR_STATUS_NEED_EXTRA_VERIFY = {-134}
QR_CA_CANDIDATE_FILES = (
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/cert.pem",
    "/etc/openssl/certs/ca-certificates.crt",
)
ARCHIVE_SUFFIXES = {
    ".zip",
    ".tar",
    ".tgz",
    ".tar.gz",
    ".tbz",
    ".tbz2",
    ".tar.bz2",
    ".txz",
    ".tar.xz",
    ".7z",
    ".rar",
}
MULTIPART_NUMBERED_ARCHIVE_RE = re.compile(r"(?i)^(?P<base>.+\.(?:7z|zip|rar))\.(?P<idx>\d{2,4})$")
MULTIPART_RAR_PART_RE = re.compile(r"(?i)^(?P<prefix>.+?)\.part(?P<idx>\d{1,4})\.rar$")
MULTIPART_ZIP_Z_RE = re.compile(r"(?i)^(?P<prefix>.+?)\.z(?P<idx>\d{2})$")
MULTIPART_RAR_R_RE = re.compile(r"(?i)^(?P<prefix>.+?)\.r(?P<idx>\d{2})$")
CLOUD_SAVE_TASK_STAGES = {
    "idle",
    "scanning",
    "packaging",
    "uploading",
    "completed",
    "failed",
}
CLOUD_SAVE_RESTORE_TASK_STAGES = {
    "idle",
    "listing",
    "planning",
    "ready",
    "applying",
    "completed",
    "failed",
}
RUNTIME_REPAIR_TASK_STAGES = {
    "idle",
    "running",
    "completed",
    "failed",
}
RUNTIME_REPAIR_STEP_TIMEOUT_SECONDS = 30 * 60
RUNTIME_REPAIR_LOG_MAX_LINES = 40
CLOUD_SAVE_DATE_FORMAT = "%Y%m%d_%H%M%S"
CLOUD_SAVE_UPLOAD_ROOT = "FreedeckCloudSaves"
CLOUD_SAVE_PROTON_BASE_DIRS = (
    ("Documents", "My Games"),
    ("Saved Games",),
    ("AppData", "Roaming"),
    ("AppData", "Local"),
    ("AppData", "LocalLow"),
)
CLOUD_SAVE_INSTALL_FALLBACK_DIRS = (
    "save",
    "saves",
    "saved",
    "userdata",
    "profiles",
)
CLOUD_SAVE_SCAN_MAX_DEPTH = 6
CLOUD_SAVE_SCAN_MAX_MATCHES = 32
CLOUD_SAVE_MAX_SOURCE_PATHS = 24
CLOUD_SAVE_RESTORE_CONFLICT_SAMPLES = 16
SWITCH_ROM_EXTENSIONS = {".xci", ".nsp", ".nsz", ".xcz"}
SWITCH_TITLE_ID_RE = re.compile(r"(?i)(0100[0-9a-f]{12})")
PLAYTIME_SESSION_MAX_SECONDS = 12 * 3600
PLAYTIME_STALE_SESSION_SECONDS = 3 * 24 * 3600
STEAM_CONSOLE_LOG_POLL_SECONDS = 2.5
STEAM_CONSOLE_LOG_TAIL_BYTES = 256 * 1024
STEAM_CONSOLE_LOG_READ_CHUNK_BYTES = 64 * 1024
STEAM_CONSOLE_LOG_MAX_BYTES_PER_TICK = 512 * 1024
STEAM_CONSOLE_LOG_PATTERN = re.compile(
    r"Game process\s+(added|removed|updated)\s*:?\s*AppID\s+(-?\d+)",
    re.IGNORECASE,
)
STEAM_CONSOLE_LOG_STEAMLAUNCH_PATTERN = re.compile(
    r"SteamLaunch\s+AppId=(\d+)",
    re.IGNORECASE,
)
STEAM_COMPAT_LOG_REQUEST_PATTERN = re.compile(
    r'Requesting mapping AppID\s+(\d+)\s+from appinfo\s+to tool\s+"([^"\r\n]+)"',
    re.IGNORECASE,
)
STEAM_COMPAT_LOG_RECORD_PATTERN = re.compile(
    r'Recording non-user mapping for\s+(\d+)\s+at priority\s+\d+\s+to tool\s+"?([^"\r\n]+)"?',
    re.IGNORECASE,
)
WINDOWS_LAUNCH_SUFFIXES = (".exe", ".bat", ".cmd")

def _now_wall_ts() -> int:
    """返回当前 wall-clock 秒级时间戳。"""
    return int(time.time())


def _freedeck_base_home_dir() -> str:
    """返回用于落盘数据的基础 home 目录。

    SteamOS 上通常为 /home/deck；某些运行环境中可能以 root 启动插件，因此做兜底。
    """
    try:
        if os.path.isdir("/home/deck"):
            return "/home/deck"
    except Exception:
        pass
    try:
        return str(Path.home())
    except Exception:
        return "/home/deck"


def _freedeck_default_download_dir() -> str:
    """默认下载目录（兜底）：/home/deck/Game"""
    return os.path.join(_freedeck_base_home_dir(), "Game")


def _freedeck_default_install_dir(download_dir: str) -> str:
    """默认安装目录：<download_dir>/installed"""
    return os.path.join(str(download_dir or "").strip(), "installed")


def _normalize_dir_path(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    return os.path.realpath(os.path.expanduser(value))


def _dir_exists_writable(path: str) -> bool:
    if not path:
        return False
    try:
        if not os.path.isdir(path):
            return False
        return bool(os.access(path, os.W_OK | os.X_OK))
    except Exception:
        return False


def _safe_int(value: Any, default: int = 0) -> int:
    """安全解析整数。"""
    try:
        return int(value)
    except Exception:
        return default


def _choose_aria2_split(*, size_bytes: int, preferred_split: int) -> int:
    """根据文件大小选择更稳定的 aria2 split 数。

    split 过大会显著增加连接数，容易触发网盘限速/临时错误；这里做一个启发式降级：
    - 小文件：不分片
    - 中等文件：少量分片
    - 大文件：按用户设置，但仍限制上限
    """
    preferred = max(1, min(64, int(preferred_split or 1)))
    size = max(0, int(size_bytes or 0))
    if size <= 0:
        return preferred

    mib = 1024 * 1024
    if size < 128 * mib:
        return 1
    if size < 512 * mib:
        return min(preferred, 4)
    if size < 2 * 1024 * mib:
        return min(preferred, 8)
    return preferred


def _ctfile_direct_url_limit_hint(direct_url: str) -> Dict[str, Any]:
    """从 CTFile 直链 query 中提取可能的限速信息。

    CTFile 的 downurl 里常见参数：
    - limit=1
    - spd/spd2: 可能的“每连接速度”（单位通常为 B/s）
    - threshold: 达到一定下载量后切换到 spd2
    """
    url_text = str(direct_url or "").strip()
    if not url_text:
        return {"limited": False}
    try:
        parsed = urlparse(url_text)
    except Exception:
        return {"limited": False}

    host = str(parsed.hostname or parsed.netloc or "").strip().lower()
    try:
        qs = parse_qs(parsed.query or "")
    except Exception:
        qs = {}

    def pick_int(key: str) -> int:
        values = qs.get(key) or []
        if not values:
            return 0
        return max(0, _safe_int(values[0], 0))

    limit = pick_int("limit")
    spd = pick_int("spd")
    spd2 = pick_int("spd2")
    threshold = pick_int("threshold")
    limited = bool(limit or spd or spd2)
    return {
        "limited": limited,
        "host": host,
        "limit": limit,
        "spd": spd,
        "spd2": spd2,
        "threshold": threshold,
    }


def _is_transient_download_error(message: str) -> bool:
    """判断 aria2 的错误信息是否更可能是临时网络/直链过期导致。"""
    text = str(message or "").strip().lower()
    if not text:
        return False

    # 常见 HTTP 临时错误/直链过期
    if any(code in text for code in (" 401", " 403", " 408", " 429", " 500", " 502", " 503", " 504")):
        return True
    if any(code in text for code in ("401 ", "403 ", "408 ", "429 ", "500 ", "502 ", "503 ", "504 ")):
        return True
    if any(word in text for word in ("forbidden", "unauthorized", "too many requests", "bad gateway", "service unavailable", "gateway timeout")):
        return True

    # 常见网络波动
    if any(
        word in text
        for word in (
            "timeout",
            "timed out",
            "connection reset",
            "connection refused",
            "connection aborted",
            "network is unreachable",
            "temporary failure",
            "name resolution",
            "could not resolve",
            "tls",
            "ssl",
        )
    ):
        return True

    return False


def _load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _save_json_file(path: str, payload: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _to_uint32_app_id(value: Any, default: int = 0) -> int:
    """将可能为 int32 的 Steam AppID 规范为 uint32。

    Steam shortcuts.vdf 中的非 Steam 游戏 appid 是带符号 int32（高位为 1 时为负数），
    而前端/后端内部统一使用无符号 appid（appid_unsigned）来做映射与缓存键。
    """
    try:
        signed = int(value)
    except Exception:
        return default
    if signed == 0:
        return default
    unsigned = int(signed) & 0xFFFFFFFF
    return unsigned if unsigned > 0 else default


def _format_size_bytes(size_bytes: int) -> str:
    """将字节数格式化为易读文本。"""
    value = float(max(0, int(size_bytes or 0)))
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"


def _format_playtime_seconds(total_seconds: int) -> str:
    """将累计游玩秒数格式化为可读文本。"""
    seconds = max(0, int(total_seconds or 0))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours} 小时 {minutes} 分钟"
    if minutes > 0:
        return f"{minutes} 分钟"
    return "0 分钟"


def _format_hours_value(hours_value: Any) -> str:
    """将小时数格式化为可读文本。"""
    try:
        value = float(hours_value)
    except Exception:
        value = 0.0
    if value <= 0:
        return "-"
    rounded = round(value, 1)
    if abs(rounded - round(rounded)) < 0.05:
        return f"{int(round(rounded))} 小时"
    return f"{rounded:.1f} 小时"


def _mask_secret(raw: str, *, head: int = 4, tail: int = 4) -> str:
    """脱敏显示敏感字符串（token/cookie 等）。"""
    text = str(raw or "").strip()
    if not text:
        return ""
    if head <= 0 and tail <= 0:
        return "…"
    if len(text) <= head + tail + 2:
        keep = max(1, min(len(text), head or 1))
        return text[:keep] + "…"
    return f"{text[:head]}…{text[-tail:]}"


def _disk_free_bytes(path: str) -> int:
    """获取目标目录所在分区的可用空间。"""
    target = os.path.realpath(os.path.expanduser(str(path or "").strip()))
    if not target:
        raise ValueError("目录无效")
    os.makedirs(target, exist_ok=True)
    usage = shutil.disk_usage(target)
    return int(usage.free)


def _detect_share_provider(share_url: str) -> str:
    """根据分享链接判断 provider。"""
    url = str(share_url or "").strip()
    if not url:
        return ""
    try:
        host = str(urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    if not host:
        return ""
    if "pan.baidu.com" in host or host.endswith(".baidu.com") or host == "baidu.com":
        return "baidu"
    if host == "ctfile.com" or host.endswith(".ctfile.com") or host.endswith("ctfile.com"):
        return "ctfile"
    if "189.cn" in host:
        return "tianyi"
    return ""


def _extract_ctfile_token_from_share_url(share_url: str) -> str:
    """从 CTFile 分享链接 query 中提取 token/session_id（仅用于本次解析，不持久化）。"""
    url = str(share_url or "").strip()
    if not url or "://" not in url or "?" not in url:
        return ""
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query or "")
    except Exception:
        return ""
    for key in ("token", "session_id"):
        values = query.get(key) or []
        token = str(values[0] if values else "").strip()
        if token:
            return token
    return ""


def _compact_task_share_ctx(provider: str, share_ctx: Any, file_id: str) -> Dict[str, Any]:
    """将解析上下文压缩后写入任务记录，避免 state.json 体积膨胀。

    目前仅百度网盘需要：用于慢速切线路/重取直链时复用必要参数与 transfer 缓存。
    """
    prov = str(provider or "").strip().lower()
    if not isinstance(share_ctx, dict):
        return {}

    if prov == "ctfile":
        keep_keys = (
            "provider",
            "client_rev",
            "share_url",
            "canonical_url",
            "fileid",
            "pwd",
        )
        compact: Dict[str, Any] = {}
        for key in keep_keys:
            if key in share_ctx:
                compact[key] = share_ctx.get(key)
        return compact

    if prov != "baidu":
        return {}

    keep_keys = (
        "provider",
        "client_rev",
        "share_url",
        "canonical_url",
        "surl",
        "share_id",
        "uk",
        "pwd",
        "randsk",
        "sekey",
    )
    compact = {}
    for key in keep_keys:
        if key in share_ctx:
            compact[key] = share_ctx.get(key)

    fid = str(file_id or "").strip()
    if not fid:
        return compact

    transfer_cache = share_ctx.get("_transfer_cache")
    if isinstance(transfer_cache, dict) and fid in transfer_cache:
        compact["_transfer_cache"] = {fid: transfer_cache.get(fid)}
    transfer_cache_meta = share_ctx.get("_transfer_cache_meta")
    if isinstance(transfer_cache_meta, dict) and fid in transfer_cache_meta:
        compact["_transfer_cache_meta"] = {fid: transfer_cache_meta.get(fid)}

    return compact


def _task_to_view(task: TianyiTaskRecord) -> Dict[str, Any]:
    """转换任务展示结构。"""
    return {
        "task_id": task.task_id,
        "game_id": task.game_id,
        "game_title": task.game_title,
        "provider": str(getattr(task, "provider", "tianyi") or "tianyi"),
        "file_name": task.file_name,
        "status": task.status,
        "progress": round(float(task.progress), 2),
        "speed": int(task.speed),
        "error_reason": task.error_reason,
        "notice": str(getattr(task, "notice", "") or ""),
        "install_status": task.install_status,
        "install_progress": round(float(getattr(task, "install_progress", 0.0) or 0.0), 2),
        "install_message": task.install_message,
        "installed_path": task.installed_path,
        "steam_import_status": str(getattr(task, "steam_import_status", "") or ""),
        "steam_exe_candidates": list(getattr(task, "steam_exe_candidates", []) or []),
        "steam_exe_selected": str(getattr(task, "steam_exe_selected", "") or ""),
        "updated_at": task.updated_at,
    }


def _is_terminal(status: str) -> bool:
    """判断任务是否终态。"""
    return status in {"complete", "error", "removed"}


class LocalWebNotReadyError(RuntimeError):
    """本地网页未就绪异常，附带结构化诊断信息。"""

    def __init__(self, message: str, *, reason: str, diagnostics: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.reason = str(reason or "local_web_not_ready")
        self.diagnostics = diagnostics or {}


class TianyiService:
    """天翼下载业务入口。"""

    def __init__(self, plugin: Any):
        self.plugin = plugin
        plugin_dir = str(getattr(decky, "DECKY_PLUGIN_DIR", Path.cwd()))
        state_root = config.DECKY_SEND_DIR
        try:
            os.makedirs(state_root, exist_ok=True)
        except Exception:
            # 某些开发环境中家目录不可写，回退到插件目录下的临时目录。
            state_root = os.path.join(plugin_dir, ".tmp", "Decky-send")

        state_dir = os.path.join(state_root, "tianyi")
        state_file = os.path.join(state_dir, "state.json")
        self.store = TianyiStateStore(state_file)
        self.catalog = GameCatalog(resolve_default_catalog_path())
        self.gba_catalog = GbaCatalog(resolve_default_gba_catalog_path())
        self.aria2 = Aria2Manager(plugin_dir=plugin_dir, work_dir=os.path.join(state_dir, "aria2"))
        self.seven_zip = SevenZipManager(plugin_dir=plugin_dir)
        self._lock = asyncio.Lock()
        self._post_process_jobs: Dict[str, asyncio.Task] = {}
        self._install_cancel_events: Dict[str, threading.Event] = {}
        self._download_retry_state: Dict[str, Dict[str, Any]] = {}
        self._download_slow_state: Dict[str, Dict[str, Any]] = {}

        # 登录采集状态机（内存态）。
        self._capture_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "next_action": "",
            "user_account": "",
            "updated_at": _now_wall_ts(),
            "diagnostics": {},
            "source_attempts": [],
            "success_source": "",
            "source_diagnostics": {},
        }
        self._capture_task: Optional[asyncio.Task] = None
        self._capture_lock = asyncio.Lock()

        # 百度网盘登录采集状态机（内存态）。
        self._baidu_capture_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "next_action": "",
            "user_account": "",
            "updated_at": _now_wall_ts(),
            "diagnostics": {},
            "source_attempts": [],
            "success_source": "",
            "source_diagnostics": {},
        }
        self._baidu_capture_task: Optional[asyncio.Task] = None
        self._baidu_capture_lock = asyncio.Lock()
        self._qr_login_lock = asyncio.Lock()
        self._qr_login_state: Dict[str, Any] = {
            "session_id": "",
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "next_action": "",
            "user_account": "",
            "image_url": "",
            "expires_at": 0,
            "updated_at": _now_wall_ts(),
            "diagnostics": {},
        }
        self._qr_login_context: Optional[Dict[str, Any]] = None
        self._catalog_cover_cache: Dict[str, Dict[str, Any]] = {}
        self._catalog_cover_lock = asyncio.Lock()
        self._steamgriddb_disabled_until = 0
        self._hltb_cache: Dict[str, Dict[str, Any]] = {}
        self._hltb_lock = asyncio.Lock()
        self._hltb_token = ""
        self._hltb_token_expires_at = 0
        self._hltb_token_lock = asyncio.Lock()
        self._hltb_hint_cache: Dict[str, Dict[str, Any]] = {}
        self._hltb_hint_lock = asyncio.Lock()
        self._panel_cache_lock = asyncio.Lock()
        self._panel_tasks_cache: List[Dict[str, Any]] = []
        self._panel_tasks_cache_at = 0.0
        self._panel_installed_cache: Dict[str, Any] = {"total": 0, "preview": []}
        self._panel_installed_cache_at = 0.0
        self._panel_last_expensive_refresh_at = 0.0
        self._panel_last_mode = PANEL_POLL_MODE_IDLE
        self._panel_last_active_tasks = 0
        self._cloud_save_lock = asyncio.Lock()
        self._cloud_save_task: Optional[asyncio.Task] = None
        self._cloud_save_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "current_game": "",
            "total_games": 0,
            "processed_games": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": {},
        }
        self._cloud_save_restore_lock = asyncio.Lock()
        self._cloud_save_restore_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "target_game_id": "",
            "target_game_title": "",
            "target_game_key": "",
            "target_version": "",
            "selected_entry_ids": [],
            "selected_target_dir": "",
            "requires_confirmation": False,
            "conflict_count": 0,
            "conflict_samples": [],
            "restored_files": 0,
            "restored_entries": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": {},
        }
        self._share_resolve_cache_lock = asyncio.Lock()
        self._share_resolve_cache: Dict[str, Dict[str, Any]] = {}
        self._share_resolve_inflight: Dict[str, asyncio.Task] = {}
        self._cloud_save_restore_plan: Dict[str, Any] = {}
        self._runtime_repair_lock = asyncio.Lock()
        self._runtime_repair_task: Optional[asyncio.Task] = None
        self._runtime_repair_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "total_games": 0,
            "processed_games": 0,
            "total_steps": 0,
            "completed_steps": 0,
            "succeeded_steps": 0,
            "skipped_steps": 0,
            "failed_steps": 0,
            "current_game_id": "",
            "current_game_title": "",
            "current_package_id": "",
            "current_package_label": "",
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": {},
        }
        self._playtime_lock = asyncio.Lock()
        self._playtime_sessions: Dict[str, Dict[str, Any]] = {}
        self._steam_console_log_task: Optional[asyncio.Task] = None
        self._steam_console_log_path = ""
        self._steam_console_log_inode = 0
        self._steam_console_log_offset = 0
        self._steam_console_log_buffer = ""
        self._steam_console_log_running_appids: Set[int] = set()
        self._steam_console_log_last_missing_at = 0
        self._steam_compat_log_cache: Dict[str, Any] = {
            "path": "",
            "inode": 0,
            "mtime_ns": 0,
            "size": 0,
            "mappings": {},
        }
        self._catalog_update_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """初始化状态与目录。"""
        os.makedirs(os.path.dirname(self.store.state_file), exist_ok=True)
        await asyncio.to_thread(self.store.load)
        if bool(getattr(self.store, "settings_migration_needed", False)):
            try:
                await asyncio.to_thread(self.store.save)
                config.logger.info("State settings migrated: added missing cloud_save_auto_upload field")
            except Exception as exc:
                config.logger.warning("State settings migration failed: %s", exc)
        try:
            meta_path = self._catalog_meta_file()
            if os.path.isfile(meta_path):
                payload = await asyncio.to_thread(_load_json_file, meta_path)
                if isinstance(payload, dict):
                    csv_path = str(payload.get("csv_path", "") or "").strip()
                    if csv_path:
                        resolved = os.path.realpath(os.path.expanduser(csv_path))
                        if resolved and os.path.isfile(resolved):
                            self.catalog.csv_path = resolved
        except Exception:
            pass
        try:
            preferred_catalog_path = await asyncio.to_thread(
                self._ensure_catalog_csv_storage_path,
                str(self.catalog.csv_path or "").strip(),
            )
            if preferred_catalog_path:
                self.catalog.csv_path = preferred_catalog_path
        except Exception:
            pass
        await asyncio.to_thread(self.catalog.load)
        await asyncio.to_thread(self.gba_catalog.load)

        # 目录容错：用户曾选择的下载目录被外部删除/移动/卸载时，避免初始化直接失败导致插件不可用。
        # 策略：
        # - 如果下载目录不存在/不可写，重置为 /home/deck/Game（并创建）
        # - install_dir 不存在/不可写时，重置为 <download_dir>/installed（并创建）
        async def _ensure_storage_dirs() -> None:
            settings = self.store.settings
            base_home = _freedeck_base_home_dir()
            fallback_download = _normalize_dir_path(_freedeck_default_download_dir())
            fallback_install = _normalize_dir_path(_freedeck_default_install_dir(fallback_download))

            plugin_download = _normalize_dir_path(getattr(self.plugin, "downloads_dir", "") or "")
            stored_download = _normalize_dir_path(getattr(settings, "download_dir", "") or "")
            stored_install = _normalize_dir_path(getattr(settings, "install_dir", "") or "")

            download_dir = stored_download or plugin_download or fallback_download

            download_reason = ""
            if not download_dir:
                download_reason = "download_dir 为空"
            else:
                try:
                    if os.path.exists(download_dir) and not os.path.isdir(download_dir):
                        download_reason = "download_dir 不是文件夹"
                    elif not os.path.exists(download_dir):
                        # 用户配置路径缺失（可能被删除/移动/卸载），此时不尝试原地重建，直接回退到默认目录。
                        download_reason = "download_dir 不存在"
                    if not download_reason and not _dir_exists_writable(download_dir):
                        download_reason = "download_dir 不可写"
                except Exception as exc:
                    download_reason = f"download_dir 检查失败: {exc}"

            reset_download = bool(download_reason)
            if reset_download:
                config.logger.warning(
                    "Download dir invalid, fallback to default: old=%s reason=%s fallback=%s",
                    stored_download or plugin_download,
                    download_reason,
                    fallback_download,
                )
                download_dir = fallback_download

            # 确保最终 download_dir 存在（此处始终允许创建兜底目录）。
            try:
                os.makedirs(download_dir, exist_ok=True)
            except Exception as exc:
                # 兜底目录也无法创建时，最后回退到 config.DOWNLOADS_DIR 或 /tmp，保证插件可用。
                config.logger.warning("Ensure download_dir failed: %s (%s)", download_dir, exc)
                secondary = _normalize_dir_path(os.path.join(base_home, "Downloads"))
                tmp_fallback = _normalize_dir_path(os.path.join("/tmp", "freedeck", "downloads"))
                for candidate in (secondary, tmp_fallback):
                    try:
                        if not candidate:
                            continue
                        os.makedirs(candidate, exist_ok=True)
                        download_dir = candidate
                        break
                    except Exception:
                        continue

            # install_dir：优先保留用户配置，缺失/不可写时回退到 <download_dir>/installed
            install_dir = stored_install
            install_reason = ""
            if install_dir:
                try:
                    if os.path.exists(install_dir) and not os.path.isdir(install_dir):
                        install_reason = "install_dir 不是文件夹"
                    elif not os.path.exists(install_dir):
                        install_reason = "install_dir 不存在"
                    elif not _dir_exists_writable(install_dir):
                        install_reason = "install_dir 不可写"
                except Exception as exc:
                    install_reason = f"install_dir 检查失败: {exc}"
            if not install_dir or install_reason:
                install_dir = _normalize_dir_path(_freedeck_default_install_dir(download_dir))
                if install_reason:
                    config.logger.warning(
                        "Install dir invalid, reset to default: old=%s reason=%s new=%s",
                        stored_install,
                        install_reason,
                        install_dir,
                    )

            try:
                os.makedirs(install_dir, exist_ok=True)
            except Exception as exc:
                config.logger.warning("Ensure install_dir failed: %s (%s)", install_dir, exc)
                candidates = [
                    _normalize_dir_path(os.path.join(download_dir, "installed")),
                    _normalize_dir_path(os.path.join("/tmp", "freedeck", "installed")),
                ]
                last_error = ""
                for candidate in candidates:
                    if not candidate:
                        continue
                    try:
                        os.makedirs(candidate, exist_ok=True)
                        install_dir = candidate
                        last_error = ""
                        break
                    except Exception as inner_exc:
                        last_error = str(inner_exc)
                        continue
                if last_error:
                    config.logger.error("Ensure install_dir failed permanently: %s", last_error)
                    install_dir = _normalize_dir_path(download_dir) or install_dir

            needs_update = False
            if _normalize_dir_path(settings.download_dir) != _normalize_dir_path(download_dir):
                needs_update = True
            if _normalize_dir_path(settings.install_dir) != _normalize_dir_path(install_dir):
                needs_update = True

            if needs_update:
                self.store.set_settings(download_dir=download_dir, install_dir=install_dir)
            else:
                # 即便未变更，也确保 plugin download_dir 与实际一致，避免 UI 与后端路径分裂。
                download_dir = _normalize_dir_path(settings.download_dir) or download_dir

            # 同步插件级下载目录（Decky settings），避免本地服务/UI 读取到旧路径。
            try:
                if hasattr(self.plugin, "set_download_dir"):
                    await self.plugin.set_download_dir(download_dir)
                else:
                    setattr(self.plugin, "downloads_dir", download_dir)
            except Exception as exc:
                config.logger.warning("Sync plugin download_dir failed: %s", exc)
                try:
                    setattr(self.plugin, "downloads_dir", download_dir)
                except Exception:
                    pass

        await _ensure_storage_dirs()

        # 自动安装能力固定开启，避免 UI 配置分叉造成行为不一致。
        if not bool(self.store.settings.auto_install):
            self.store.set_settings(auto_install=True)
        self._cloud_save_state["last_result"] = dict(self.store.cloud_save_last_result or {})
        self._cloud_save_restore_state["last_result"] = dict(self.store.cloud_save_restore_last_result or {})
        self._runtime_repair_state["last_result"] = dict(self.store.runtime_repair_last_result or {})
        await self._recover_playtime_sessions_from_store()
        try:
            migration = await asyncio.to_thread(migrate_tianyi_shortcut_tokens_sync)
            if isinstance(migration, dict) and bool(migration.get("changed")):
                config.logger.info(
                    "Steam shortcuts token migrated: migrated=%s cleaned=%s",
                    migration.get("migrated", 0),
                    migration.get("cleaned", 0),
                )
        except Exception as exc:
            config.logger.warning("Steam shortcuts token migration skipped: %s", exc)
        self._ensure_steam_console_log_watcher()

    async def shutdown(self) -> None:
        """关闭后台资源。"""
        if self._steam_console_log_task and not self._steam_console_log_task.done():
            self._steam_console_log_task.cancel()
            try:
                await self._steam_console_log_task
            except BaseException:
                pass
        self._steam_console_log_task = None
        async with self._capture_lock:
            if self._capture_task and not self._capture_task.done():
                self._capture_task.cancel()
                try:
                    await self._capture_task
                except BaseException:
                    pass
            self._capture_task = None
        async with self._qr_login_lock:
            await self._close_qr_login_context_locked()
        await self._finalize_active_playtime_sessions(reason="service_shutdown")
        await self._cancel_cloud_save_task()
        await self._clear_cloud_save_restore_plan()
        jobs = list(self._post_process_jobs.values())
        for job in jobs:
            if not job.done():
                job.cancel()
        for job in jobs:
            try:
                await job
            except BaseException:
                pass
        self._post_process_jobs.clear()
        await asyncio.to_thread(self.aria2.stop)

    def _ensure_steam_console_log_watcher(self) -> None:
        if self._steam_console_log_task and not self._steam_console_log_task.done():
            return
        self._steam_console_log_task = asyncio.create_task(
            self._steam_console_log_loop(),
            name="freedeck_steam_console_log",
        )

    def _catalog_meta_file(self) -> str:
        state_dir = os.path.dirname(self.store.state_file)
        return os.path.join(state_dir, "catalog_meta.json")

    def _preferred_catalog_csv_path(self) -> str:
        """返回 Freedeck 使用的目录 CSV 落盘路径。"""
        state_dir = os.path.dirname(self.store.state_file)
        return os.path.join(state_dir, CATALOG_LOCAL_FILENAME)

    def _ensure_catalog_csv_storage_path(self, current_path: str = "") -> str:
        """确保目录 CSV 使用 Freedeck 命名，并优先落在可写状态目录。"""
        preferred_path = os.path.realpath(os.path.expanduser(self._preferred_catalog_csv_path()))
        current = str(current_path or "").strip()
        normalized_current = ""
        if current:
            try:
                normalized_current = os.path.realpath(os.path.expanduser(current))
            except Exception:
                normalized_current = ""

        if normalized_current:
            sibling_path = os.path.join(os.path.dirname(normalized_current), CATALOG_LOCAL_FILENAME)
            sibling_path = os.path.realpath(os.path.expanduser(sibling_path))
            if os.path.isfile(sibling_path):
                return sibling_path

        if preferred_path and os.path.isfile(preferred_path):
            return preferred_path

        if normalized_current and os.path.isfile(normalized_current):
            current_name = os.path.basename(normalized_current).lower()
            if current_name == CATALOG_LOCAL_FILENAME:
                return normalized_current
            try:
                os.makedirs(os.path.dirname(preferred_path), exist_ok=True)
                shutil.copy2(normalized_current, preferred_path)
                return preferred_path
            except Exception as exc:
                config.logger.warning(
                    "Catalog path migration skipped source=%s target=%s error=%s",
                    normalized_current,
                    preferred_path,
                    exc,
                )
                return normalized_current

        return preferred_path or normalized_current

    def _validate_catalog_csv_header(self, path: str) -> None:
        """校验目录 CSV 表头，兼容带引号或 BOM 的写法。"""
        normalized = os.path.realpath(os.path.expanduser(str(path or "").strip()))
        if not normalized or not os.path.isfile(normalized):
            raise TianyiApiError("远端 CSV 内容异常（文件不存在）")

        try:
            with open(normalized, "r", encoding="utf-8-sig", newline="") as check_fp:
                reader = csv.reader(check_fp)
                header = next(reader, [])
        except StopIteration:
            header = []
        except Exception as exc:
            raise TianyiApiError(f"远端 CSV 内容异常（读取失败: {exc}）") from exc

        normalized_header = [str(item or "").strip().strip('"').strip("'").lower() for item in list(header or [])]
        if "game_id" not in normalized_header or "title" not in normalized_header:
            raise TianyiApiError("远端 CSV 内容异常（缺少表头）")

    def _decode_catalog_payload_if_needed(self, path: str) -> None:
        """兼容 GitHub contents API 返回的 JSON/base64 结构。"""
        normalized = os.path.realpath(os.path.expanduser(str(path or "").strip()))
        if not normalized or not os.path.isfile(normalized):
            return

        try:
            with open(normalized, "rb") as fp:
                head_bytes = fp.read(256 * 1024)
        except Exception:
            return

        text = head_bytes.decode("utf-8", errors="ignore").lstrip("\ufeff\r\n\t ")
        if not text.startswith("{"):
            return

        try:
            payload = json.loads(text)
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        if str(payload.get("encoding", "") or "").strip().lower() != "base64":
            return

        content = str(payload.get("content", "") or "")
        if not content:
            return
        try:
            decoded = base64.b64decode(content.encode("utf-8"), validate=False)
        except Exception as exc:
            raise TianyiApiError(f"远端 CSV 内容异常（base64 解码失败: {exc}）") from exc
        if len(decoded) < 64:
            raise TianyiApiError("远端 CSV 内容异常（文件过小）")
        with open(normalized, "wb") as fp:
            fp.write(decoded)

    def _normalize_catalog_date(self, raw: str) -> str:
        value = str(raw or "").strip()
        matched = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", value)
        if not matched:
            return ""
        try:
            year = int(matched.group(1))
            month = int(matched.group(2))
            day = int(matched.group(3))
        except Exception:
            return ""
        if year <= 0:
            return ""
        if month < 1 or month > 12:
            return ""
        if day < 1 or day > 31:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"

    async def get_catalog_version(self) -> Dict[str, Any]:
        """返回当前游戏目录 CSV 的版本日期。"""
        meta_path = self._catalog_meta_file()
        date_value = ""
        meta_csv_path = ""
        try:
            if os.path.isfile(meta_path):
                payload = await asyncio.to_thread(_load_json_file, meta_path)
                if isinstance(payload, dict):
                    date_value = self._normalize_catalog_date(str(payload.get("date", "") or ""))
                    meta_csv_path = str(payload.get("csv_path", "") or "").strip()
        except Exception:
            date_value = ""
        if not date_value:
            date_value = CATALOG_DEFAULT_DATE
        csv_path = str(self.catalog.csv_path or "").strip()
        if meta_csv_path:
            resolved = os.path.realpath(os.path.expanduser(meta_csv_path))
            if resolved:
                csv_path = resolved
        try:
            migrated_csv_path = self._ensure_catalog_csv_storage_path(csv_path)
            if migrated_csv_path:
                csv_path = migrated_csv_path
                self.catalog.csv_path = migrated_csv_path
        except Exception:
            pass
        return {
            "date": date_value,
            "csv_path": csv_path,
        }

    async def update_catalog_from_github(self) -> Dict[str, Any]:
        """从 GitHub 拉取最新 CSV，并覆盖当前使用的目录文件。"""
        async with self._catalog_update_lock:
            current_meta = await self.get_catalog_version()
            current_date = self._normalize_catalog_date(str(current_meta.get("date", "") or "")) or CATALOG_DEFAULT_DATE
            config.logger.info("Catalog update: checking github (current=%s)", current_date)

            ssl_context, _ = self._build_qr_ssl_context()
            timeout = aiohttp.ClientTimeout(total=CATALOG_UPDATE_HTTP_TIMEOUT_SECONDS)
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            headers = {
                "Accept": "application/vnd.github+json, application/json, text/plain, */*",
                "User-Agent": CATALOG_UPDATE_USER_AGENT,
            }

            items: Any = []
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(CATALOG_UPDATE_GITHUB_API_URL, headers=headers) as resp:
                    if int(resp.status) >= 400:
                        raise TianyiApiError(f"获取远端列表失败 status={resp.status}")
                    items = await resp.json(content_type=None)

            if not isinstance(items, list):
                raise TianyiApiError("远端列表格式异常")

            candidates: List[Tuple[str, str, str]] = []
            raw_csv_names: List[str] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "") or "").strip()
                if not name.lower().endswith(".csv"):
                    continue
                raw_csv_names.append(name)
                stem = name[:-4]
                date_value = self._normalize_catalog_date(stem)
                if not date_value:
                    continue
                api_url = str(item.get("url", "") or "").strip()
                if not api_url:
                    continue
                download_url = str(item.get("download_url", "") or "").strip()
                if not download_url:
                    continue
                candidates.append((date_value, download_url, api_url))

            if not candidates:
                config.logger.warning("Catalog update: no valid csv candidates in github gl/: %s", raw_csv_names)
                raise TianyiApiError("远端目录未找到有效 CSV 文件")

            candidates.sort(key=lambda row: row[0], reverse=True)
            latest_date, latest_url, latest_api_url = candidates[0]
            config.logger.info("Catalog update: latest=%s url=%s", latest_date, latest_url)

            if latest_date <= current_date:
                return {
                    "updated": False,
                    "date": current_date,
                    "latest_date": latest_date,
                    "message": "当前已是最新，无需更新",
                }

            target_path = self._ensure_catalog_csv_storage_path(str(self.catalog.csv_path or "").strip())
            if target_path:
                self.catalog.csv_path = target_path
            if not target_path:
                raise TianyiApiError("无法解析本地 CSV 路径")

            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            tmp_path = f"{target_path}.tmp"
            config.logger.info("Catalog update: downloading to %s", target_path)

            async def download_to_tmp(
                *,
                session: aiohttp.ClientSession,
                url: str,
                request_headers: Dict[str, str],
                source: str,
            ) -> str:
                last_exc: Optional[BaseException] = None
                for attempt in range(1, CATALOG_UPDATE_DOWNLOAD_RETRIES + 1):
                    downloaded_bytes = 0
                    try:
                        async with session.get(url, headers=request_headers) as resp:
                            status = int(resp.status)
                            if status >= 400:
                                if status in CATALOG_UPDATE_TRANSIENT_STATUSES and attempt < CATALOG_UPDATE_DOWNLOAD_RETRIES:
                                    config.logger.warning(
                                        "Catalog update: download transient error source=%s attempt=%s status=%s",
                                        source,
                                        attempt,
                                        status,
                                    )
                                    await asyncio.sleep(CATALOG_UPDATE_RETRY_BACKOFF_SECONDS * attempt)
                                    continue
                                raise TianyiApiError(f"下载远端 CSV 失败 status={status}")
                            with open(tmp_path, "wb") as fp:
                                async for chunk in resp.content.iter_chunked(64 * 1024):
                                    if not chunk:
                                        continue
                                    fp.write(chunk)
                                    downloaded_bytes += len(chunk)
                                    if downloaded_bytes > CATALOG_UPDATE_MAX_BYTES:
                                        raise TianyiApiError("远端 CSV 文件过大，已中止下载")
                            if downloaded_bytes < 64:
                                raise TianyiApiError("远端 CSV 内容异常（文件过小）")
                            self._decode_catalog_payload_if_needed(tmp_path)
                            self._validate_catalog_csv_header(tmp_path)
                            return url
                    except TianyiApiError as exc:
                        last_exc = exc
                        try:
                            if os.path.exists(tmp_path):
                                os.remove(tmp_path)
                        except Exception:
                            pass
                        break
                    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                        last_exc = exc
                        try:
                            if os.path.exists(tmp_path):
                                os.remove(tmp_path)
                        except Exception:
                            pass
                        if attempt < CATALOG_UPDATE_DOWNLOAD_RETRIES:
                            config.logger.warning(
                                "Catalog update: download attempt failed source=%s attempt=%s error=%s",
                                source,
                                attempt,
                                exc,
                            )
                            await asyncio.sleep(CATALOG_UPDATE_RETRY_BACKOFF_SECONDS * attempt)
                            continue
                        break
                raise TianyiApiError(f"下载远端 CSV 失败: {last_exc}") if last_exc else TianyiApiError("下载远端 CSV 失败")

            ssl_context, _ = self._build_qr_ssl_context()
            timeout = aiohttp.ClientTimeout(total=CATALOG_UPDATE_HTTP_TIMEOUT_SECONDS)
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            used_url = ""
            download_errors: List[str] = []
            file_name = latest_url.rsplit("/", 1)[-1].strip()
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                sources: List[Tuple[str, str, Dict[str, str]]] = []
                if file_name:
                    sources.extend(
                        [
                            (
                                "jsdelivr",
                                f"https://cdn.jsdelivr.net/gh/panyiwei-home/Freedeck@main/gl/{file_name}",
                                {
                                    "Accept": "text/csv, text/plain, */*",
                                    "User-Agent": CATALOG_UPDATE_USER_AGENT,
                                },
                            ),
                            (
                                "fastly_jsdelivr",
                                f"https://fastly.jsdelivr.net/gh/panyiwei-home/Freedeck@main/gl/{file_name}",
                                {
                                    "Accept": "text/csv, text/plain, */*",
                                    "User-Agent": CATALOG_UPDATE_USER_AGENT,
                                },
                            ),
                        ]
                    )
                sources.extend(
                    [
                        (
                            "github_raw",
                            latest_url,
                            {
                                "Accept": "text/csv, text/plain, */*",
                                "User-Agent": CATALOG_UPDATE_USER_AGENT,
                            },
                        ),
                        (
                            "github_api_raw",
                            latest_api_url,
                            {
                                "Accept": "application/vnd.github.raw, text/csv, text/plain, */*",
                                "User-Agent": CATALOG_UPDATE_USER_AGENT,
                            },
                        ),
                    ]
                )
                for source, url, req_headers in sources:
                    try:
                        used_url = await download_to_tmp(session=session, url=url, request_headers=req_headers, source=source)
                        break
                    except TianyiApiError as exc:
                        download_errors.append(f"{source}: {exc}")
                        config.logger.warning("Catalog update: download failed source=%s url=%s error=%s", source, url, exc)
                        continue
                if not used_url:
                    raise TianyiApiError("；".join(download_errors) if download_errors else "下载远端 CSV 失败")

            os.replace(tmp_path, target_path)

            meta_path = self._catalog_meta_file()
            meta_payload = {
                "date": latest_date,
                "updated_at": _now_wall_ts(),
                "source": "github",
                "url": used_url or latest_url,
                "csv_path": target_path,
            }
            await asyncio.to_thread(_save_json_file, meta_path, meta_payload)

            await asyncio.to_thread(self.catalog.load)
            config.logger.info("Catalog update: updated to %s", latest_date)

            return {
                "updated": True,
                "date": latest_date,
                "latest_date": latest_date,
                "message": f"已更新游戏列表：{latest_date}",
                "csv_path": target_path,
            }

    def _steam_console_log_candidates(self) -> List[str]:
        homes: List[str] = []
        home_candidates = [
            str(getattr(decky, "DECKY_USER_HOME", "") or "").strip(),
            str(os.environ.get("DECKY_USER_HOME", "") or "").strip(),
            str(Path.home()),
            str(os.environ.get("HOME", "") or "").strip(),
            "/home/deck",
        ]
        for value in home_candidates:
            if not value:
                continue
            try:
                resolved = os.path.realpath(os.path.expanduser(value))
            except Exception:
                continue
            if resolved and resolved not in homes:
                homes.append(resolved)

        candidates: List[str] = []
        for home in homes:
            candidates.extend(
                [
                    os.path.join(home, ".local", "share", "Steam", "logs", "console_log.txt"),
                    os.path.join(home, ".steam", "steam", "logs", "console_log.txt"),
                    os.path.join(home, ".steam", "root", "logs", "console_log.txt"),
                ]
            )
        deduped: List[str] = []
        seen = set()
        for raw in candidates:
            path = os.path.realpath(os.path.expanduser(raw))
            if not path or path in seen:
                continue
            seen.add(path)
            deduped.append(path)
        return deduped

    def _find_steam_console_log_path(self) -> str:
        for candidate in self._steam_console_log_candidates():
            if candidate and os.path.isfile(candidate):
                return candidate
        return ""

    def _steam_compat_log_candidates(self, steam_root: str = "") -> List[str]:
        candidates: List[str] = []
        normalized_root = self._normalize_dir_path(steam_root)
        if normalized_root:
            candidates.append(os.path.join(normalized_root, "logs", "compat_log.txt"))

        for root in self._runtime_repair_candidate_steam_roots():
            candidates.append(os.path.join(root, "logs", "compat_log.txt"))

        deduped: List[str] = []
        seen = set()
        for raw in candidates:
            path = self._normalize_dir_path(raw)
            if not path or path in seen:
                continue
            seen.add(path)
            deduped.append(path)
        return deduped

    def _find_steam_compat_log_path(self, steam_root: str = "") -> str:
        for candidate in self._steam_compat_log_candidates(steam_root):
            if candidate and os.path.isfile(candidate):
                return candidate
        return ""

    def _load_steam_official_compat_mappings(self, steam_root: str = "") -> Dict[int, str]:
        path = self._find_steam_compat_log_path(steam_root)
        if not path:
            return {}

        try:
            st = os.stat(path)
            inode = int(getattr(st, "st_ino", 0) or 0)
            mtime_ns = int(getattr(st, "st_mtime_ns", 0) or 0)
            size = int(getattr(st, "st_size", 0) or 0)
        except Exception:
            return {}

        cache = self._steam_compat_log_cache if isinstance(self._steam_compat_log_cache, dict) else {}
        cached_mappings = cache.get("mappings") if isinstance(cache.get("mappings"), dict) else {}
        if (
            str(cache.get("path", "") or "") == path
            and _safe_int(cache.get("inode"), 0) == inode
            and _safe_int(cache.get("mtime_ns"), 0) == mtime_ns
            and _safe_int(cache.get("size"), 0) == size
            and cached_mappings
        ):
            return {max(0, _safe_int(app_id, 0)): str(tool or "").strip() for app_id, tool in cached_mappings.items() if max(0, _safe_int(app_id, 0)) > 0 and str(tool or "").strip()}

        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fp:
                lines = fp.readlines()
        except Exception:
            return {}

        mappings: Dict[int, str] = {}
        for raw_line in lines:
            line = str(raw_line or "").strip()
            if not line:
                continue

            matched = STEAM_COMPAT_LOG_REQUEST_PATTERN.search(line)
            if matched:
                app_id = max(0, _safe_int(matched.group(1), 0))
                tool_name = str(matched.group(2) or "").strip()
                if app_id > 0 and tool_name:
                    mappings[app_id] = tool_name
                continue

            matched = STEAM_COMPAT_LOG_RECORD_PATTERN.search(line)
            if matched:
                app_id = max(0, _safe_int(matched.group(1), 0))
                tool_name = str(matched.group(2) or "").strip()
                if app_id > 0 and tool_name:
                    mappings[app_id] = tool_name

        self._steam_compat_log_cache = {
            "path": path,
            "inode": inode,
            "mtime_ns": mtime_ns,
            "size": size,
            "mappings": dict(mappings),
        }
        return mappings

    def _resolve_official_compat_tool_name(self, *, steam_root: str = "", app_id: int) -> str:
        app_id_unsigned = max(0, _safe_int(app_id, 0))
        if app_id_unsigned <= 0:
            return ""
        mappings = self._load_steam_official_compat_mappings(steam_root)
        return str(mappings.get(app_id_unsigned, "") or "").strip()

    def _is_windows_compat_tool_name(self, tool_name: str) -> bool:
        normalized = str(tool_name or "").strip().lower()
        if not normalized:
            return False
        return normalized.startswith("proton-") or normalized.startswith("proton_")

    async def _steam_console_log_loop(self) -> None:
        """轮询 Steam console_log.txt，提取启动/退出事件作为游玩时长兜底数据源。"""
        while True:
            try:
                await self._steam_console_log_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                config.logger.warning("Steam console_log watcher failed: %s", exc)
            await asyncio.sleep(STEAM_CONSOLE_LOG_POLL_SECONDS)

    async def _steam_console_log_tick(self) -> None:
        path = self._find_steam_console_log_path()
        if not path:
            now = _now_wall_ts()
            if now - int(self._steam_console_log_last_missing_at or 0) > 90:
                self._steam_console_log_last_missing_at = now
                config.logger.info("Steam console_log.txt not found, waiting for Steam logs...")
            return

        if path != self._steam_console_log_path:
            self._steam_console_log_path = path
            self._steam_console_log_inode = 0
            self._steam_console_log_offset = 0
            self._steam_console_log_buffer = ""
            self._steam_console_log_running_appids.clear()
            config.logger.info("Steam console_log watcher bound to %s", path)

        def read_new_lines_sync(
            *,
            file_path: str,
            offset: int,
            inode: int,
            buffer: str,
        ) -> Tuple[int, int, str, List[str], str]:
            try:
                st = os.stat(file_path)
            except FileNotFoundError:
                return offset, inode, buffer, [], "missing"
            except Exception:
                return offset, inode, buffer, [], "stat_failed"

            size = int(getattr(st, "st_size", 0) or 0)
            current_inode = int(getattr(st, "st_ino", 0) or 0)
            normalized_offset = max(0, int(offset or 0))
            normalized_inode = max(0, int(inode or 0))

            if normalized_inode <= 0:
                # 初始化：避免解析历史日志，直接从文件末尾开始监听新增事件。
                return size, current_inode, "", [], "init"
            if current_inode != normalized_inode or size < normalized_offset:
                # 文件轮转或截断：同样从末尾重新开始，避免误判历史事件为当前运行状态。
                return size, current_inode, "", [], "reset"

            remaining = max(0, size - normalized_offset)
            if remaining <= 0:
                return normalized_offset, current_inode, buffer, [], ""

            budget = min(int(STEAM_CONSOLE_LOG_MAX_BYTES_PER_TICK), remaining)
            chunks: List[bytes] = []
            try:
                with open(file_path, "rb") as fp:
                    fp.seek(normalized_offset)
                    to_read = int(budget)
                    while to_read > 0:
                        chunk = fp.read(min(int(STEAM_CONSOLE_LOG_READ_CHUNK_BYTES), to_read))
                        if not chunk:
                            break
                        chunks.append(chunk)
                        to_read -= len(chunk)
            except FileNotFoundError:
                return normalized_offset, current_inode, buffer, [], "missing"
            except Exception:
                return normalized_offset, current_inode, buffer, [], "read_failed"

            raw = b"".join(chunks)
            if not raw:
                return normalized_offset, current_inode, buffer, [], ""

            new_offset = normalized_offset + len(raw)
            text = buffer + raw.decode("utf-8", errors="replace")
            parts = text.split("\n")
            if text.endswith("\n"):
                lines = parts[:-1]
                next_buffer = ""
            else:
                lines = parts[:-1]
                next_buffer = parts[-1] if parts else ""
            cleaned = [line.rstrip("\r") for line in lines if line]
            if len(next_buffer) > 4096:
                next_buffer = next_buffer[-4096:]
            return new_offset, current_inode, next_buffer, cleaned, ""

        (
            next_offset,
            next_inode,
            next_buffer,
            lines,
            reset_reason,
        ) = await asyncio.to_thread(
            read_new_lines_sync,
            file_path=path,
            offset=self._steam_console_log_offset,
            inode=self._steam_console_log_inode,
            buffer=self._steam_console_log_buffer,
        )

        if reset_reason in {"init", "reset"}:
            self._steam_console_log_running_appids.clear()

        self._steam_console_log_offset = int(next_offset or 0)
        self._steam_console_log_inode = int(next_inode or 0)
        self._steam_console_log_buffer = str(next_buffer or "")

        if not lines:
            return

        events: List[Tuple[str, int]] = []
        for line in lines:
            matched = STEAM_CONSOLE_LOG_PATTERN.search(line)
            if not matched:
                continue
            action = str(matched.group(1) or "").strip().lower()
            outer_app_id = _to_uint32_app_id(matched.group(2), 0)
            inner_match = STEAM_CONSOLE_LOG_STEAMLAUNCH_PATTERN.search(line)
            inner_app_id = _to_uint32_app_id(inner_match.group(1), 0) if inner_match else 0
            app_id_unsigned = inner_app_id or outer_app_id
            # Freedeck shortcuts 使用非 Steam AppID（高位为 1），避免误把 Steam 正版 AppID 计入。
            if app_id_unsigned > 0 and app_id_unsigned < 0x80000000:
                continue
            if not action or app_id_unsigned <= 0:
                continue
            events.append((action, app_id_unsigned))

        for action, app_id_unsigned in events:
            if action in {"added", "updated"}:
                if app_id_unsigned in self._steam_console_log_running_appids:
                    continue
                self._steam_console_log_running_appids.add(app_id_unsigned)
                try:
                    await self.record_game_action(
                        phase="start",
                        app_id=str(app_id_unsigned),
                        action_name="LaunchApp",
                    )
                except Exception:
                    # 兜底 watcher 不应影响主流程；忽略单次解析失败。
                    pass
            elif action == "removed":
                self._steam_console_log_running_appids.discard(app_id_unsigned)
                try:
                    await self.record_game_action(
                        phase="end",
                        app_id=str(app_id_unsigned),
                        action_name="",
                    )
                except Exception:
                    pass

    def _normalize_panel_mode(self, context: Optional[Dict[str, Any]] = None) -> tuple[str, bool, bool]:
        """规范化面板轮询模式。"""
        payload = context if isinstance(context, dict) else {}
        mode = str(payload.get("poll_mode", "") or "").strip().lower()
        visible = bool(payload.get("visible", True))
        has_focus = bool(payload.get("has_focus", True))
        if mode not in {PANEL_POLL_MODE_ACTIVE, PANEL_POLL_MODE_IDLE, PANEL_POLL_MODE_BACKGROUND}:
            mode = PANEL_POLL_MODE_BACKGROUND if not visible else PANEL_POLL_MODE_IDLE
        if mode != PANEL_POLL_MODE_BACKGROUND and not visible:
            mode = PANEL_POLL_MODE_BACKGROUND
        return mode, visible, has_focus

    def _panel_task_refresh_window(self, mode: str, active_tasks: int) -> float:
        """按模式与活跃任务数量返回任务刷新窗口。"""
        if mode == PANEL_POLL_MODE_BACKGROUND:
            return PANEL_TASK_REFRESH_BACKGROUND_SECONDS
        if active_tasks > 0:
            return PANEL_TASK_REFRESH_ACTIVE_SECONDS
        return PANEL_TASK_REFRESH_IDLE_SECONDS

    def _panel_installed_refresh_window(self, mode: str, active_tasks: int) -> float:
        """按模式与活跃任务数量返回安装列表刷新窗口。"""
        if mode == PANEL_POLL_MODE_BACKGROUND:
            return PANEL_INSTALLED_REFRESH_BACKGROUND_SECONDS
        if active_tasks > 0:
            return PANEL_INSTALLED_REFRESH_ACTIVE_SECONDS
        return PANEL_INSTALLED_REFRESH_IDLE_SECONDS

    def _count_active_tasks(self, tasks: Sequence[Dict[str, Any]]) -> int:
        """统计非终态任务数量。"""
        count = 0
        for item in tasks:
            payload = item or {}
            status = str(payload.get("status", "")).strip().lower()
            install_status = str(payload.get("install_status", "")).strip().lower()
            if install_status == "installing":
                count += 1
                continue
            if status == "complete" and install_status and install_status not in {"installed", "failed", "skipped", "canceled", "bundled"}:
                count += 1
                continue
            if status and not _is_terminal(status):
                count += 1
        return count

    def _invalidate_panel_cache(self, *, tasks: bool = False, installed: bool = False, all_data: bool = False) -> None:
        """失效面板缓存，确保关键变更后可及时刷新。"""
        if all_data or tasks:
            self._panel_tasks_cache = []
            self._panel_tasks_cache_at = 0.0
        if all_data or installed:
            self._panel_installed_cache = {"total": 0, "preview": []}
            self._panel_installed_cache_at = 0.0
        if all_data or tasks or installed:
            self._panel_last_expensive_refresh_at = 0.0

    async def get_panel_state(self, *, request_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """返回 Decky 面板状态。"""
        async with self._panel_cache_lock:
            now = time.monotonic()
            requested_mode, visible, has_focus = self._normalize_panel_mode(request_context)

            cached_tasks = list(self._panel_tasks_cache)
            if not cached_tasks:
                cached_tasks = [_task_to_view(task) for task in list(self.store.tasks)]

            active_tasks = self._count_active_tasks(cached_tasks)
            effective_mode = requested_mode
            if requested_mode != PANEL_POLL_MODE_BACKGROUND:
                effective_mode = PANEL_POLL_MODE_ACTIVE if active_tasks > 0 else PANEL_POLL_MODE_IDLE

            task_window = self._panel_task_refresh_window(effective_mode, active_tasks)
            installed_window = self._panel_installed_refresh_window(effective_mode, active_tasks)
            tasks_refreshed = False
            installed_refreshed = False

            tasks = cached_tasks
            if now - float(self._panel_tasks_cache_at or 0.0) >= float(task_window):
                try:
                    tasks = await asyncio.wait_for(
                        self.refresh_tasks(sync_aria2=True, persist=False),
                        timeout=PANEL_TASK_REFRESH_TIMEOUT_SECONDS,
                    )
                    self._panel_tasks_cache = list(tasks)
                    self._panel_tasks_cache_at = now
                    tasks_refreshed = True
                except Exception as exc:
                    config.logger.warning("Panel tasks refresh fallback to cache: %s", exc)
                    tasks = list(self._panel_tasks_cache) if self._panel_tasks_cache else list(cached_tasks)

            active_tasks = self._count_active_tasks(tasks)
            if requested_mode != PANEL_POLL_MODE_BACKGROUND:
                effective_mode = PANEL_POLL_MODE_ACTIVE if active_tasks > 0 else PANEL_POLL_MODE_IDLE
                task_window = self._panel_task_refresh_window(effective_mode, active_tasks)
                installed_window = self._panel_installed_refresh_window(effective_mode, active_tasks)

            installed_cached = dict(self._panel_installed_cache or {"total": 0, "preview": []})
            installed_cached["preview"] = list(installed_cached.get("preview") or [])
            installed = installed_cached
            if now - float(self._panel_installed_cache_at or 0.0) >= float(installed_window):
                installed = self._build_installed_summary(limit=60, persist=False)
                self._panel_installed_cache = {
                    "total": int(installed.get("total", 0) or 0),
                    "preview": list(installed.get("preview") or []),
                }
                self._panel_installed_cache_at = now
                installed_refreshed = True
            elif not installed_cached.get("preview"):
                installed = self._build_installed_summary(limit=60, persist=False)
                self._panel_installed_cache = {
                    "total": int(installed.get("total", 0) or 0),
                    "preview": list(installed.get("preview") or []),
                }
                self._panel_installed_cache_at = now
                installed_refreshed = True

            if tasks_refreshed or installed_refreshed:
                self._panel_last_expensive_refresh_at = now

            self._panel_last_mode = effective_mode
            self._panel_last_active_tasks = active_tasks

            last_expensive_at = float(self._panel_last_expensive_refresh_at or 0.0)
            last_task_at = float(self._panel_tasks_cache_at or 0.0)
            last_installed_at = float(self._panel_installed_cache_at or 0.0)

            power_diagnostics = {
                "requested_mode": requested_mode,
                "effective_mode": effective_mode,
                "visible": bool(visible),
                "has_focus": bool(has_focus),
                "active_tasks": int(active_tasks),
                "task_refresh_interval_seconds": float(task_window),
                "installed_refresh_interval_seconds": float(installed_window),
                "tasks_refreshed": bool(tasks_refreshed),
                "installed_refreshed": bool(installed_refreshed),
                "last_expensive_refresh_age_seconds": round(max(0.0, now - last_expensive_at), 3)
                if last_expensive_at > 0
                else -1.0,
                "last_tasks_refresh_age_seconds": round(max(0.0, now - last_task_at), 3) if last_task_at > 0 else -1.0,
                "last_installed_refresh_age_seconds": round(max(0.0, now - last_installed_at), 3)
                if last_installed_at > 0
                else -1.0,
            }

        summary = self.catalog.summary()
        cached_cookie = str(self.store.login.cookie or "").strip()
        cached_account = str(self.store.login.user_account or "").strip()
        login_ok = bool(cached_cookie)
        account = cached_account
        message = "未登录"
        if login_ok:
            message = f"已登录（缓存）：{cached_account or '未知账号'}"

        cached_baidu_cookie = str(getattr(self.store, "baidu_login", None) and self.store.baidu_login.cookie or "").strip()
        cached_baidu_account = str(
            getattr(self.store, "baidu_login", None) and self.store.baidu_login.user_account or ""
        ).strip()
        baidu_login_ok = bool(cached_baidu_cookie and "BDUSS=" in cached_baidu_cookie)
        baidu_account = cached_baidu_account
        baidu_message = "未登录百度网盘"
        if cached_baidu_cookie:
            if baidu_login_ok:
                baidu_message = f"已登录（缓存）：{cached_baidu_account or '未知账号'}"
            else:
                baidu_message = "已缓存 Cookie（缺少 BDUSS）"

        cached_ctfile_token = str(
            getattr(self.store, "ctfile_login", None) and self.store.ctfile_login.token or ""
        ).strip()
        ctfile_configured = bool(cached_ctfile_token)
        ctfile_message = "未配置 CTFile token"
        if ctfile_configured:
            ctfile_message = "已配置 token（缓存）"
        ctfile_hint = _mask_secret(cached_ctfile_token)
        ctfile_updated_at = int(getattr(self.store, "ctfile_login", None) and self.store.ctfile_login.updated_at or 0)

        # 面板轮询走纯缓存快路径，避免本地端口探测与网络校验阻塞 RPC。
        login_url = ""
        library_url = ""

        return {
            "login": {
                "logged_in": login_ok,
                "user_account": account,
                "message": message,
                "login_url": login_url,
            },
            "baidu_login": {
                "logged_in": baidu_login_ok,
                "user_account": baidu_account,
                "message": baidu_message,
            },
            "ctfile_login": {
                "configured": ctfile_configured,
                "token_hint": ctfile_hint,
                "updated_at": ctfile_updated_at,
                "message": ctfile_message,
            },
            "catalog": {
                "total": summary.get("total", 0),
                "preview": [],
                "path": summary.get("path", ""),
            },
            "installed": installed,
            "tasks": tasks,
            "settings": asdict(self.store.settings),
            "library_url": library_url,
            "login_capture": await self.get_login_capture_status(),
            "baidu_login_capture": await self.get_baidu_login_capture_status(),
            "power_diagnostics": power_diagnostics,
        }

    def get_cloud_login_url(self) -> str:
        """返回天翼云官方登录网址。"""
        return "https://cloud.189.cn/web/login.html"

    async def get_login_url(self) -> str:
        """返回本地登录桥接页面地址。"""
        target = quote(self.get_cloud_login_url(), safe="")
        return await self._ensure_local_web_ready(f"/tianyi/library/login-bridge?target={target}")

    async def get_library_url(self) -> str:
        """返回本地游戏库网页地址。"""
        return await self._ensure_local_web_ready("/tianyi/library")

    async def peek_login_url(self) -> str:
        """只读取登录桥接地址，不主动启动服务。"""
        target = quote(self.get_cloud_login_url(), safe="")
        return await self._peek_local_web_url(f"/tianyi/library/login-bridge?target={target}")

    async def peek_library_url(self) -> str:
        """只读取游戏库地址，不主动启动服务。"""
        return await self._peek_local_web_url("/tianyi/library")

    async def check_login_state(self) -> tuple[bool, str, str]:
        """校验当前登录态。"""
        cookie = (self.store.login.cookie or "").strip()
        if not cookie:
            return False, "", "未登录"
        now_ts = _now_wall_ts()
        cached_account = str(getattr(self.store.login, "user_account", "") or "").strip()
        cached_at = _safe_int(getattr(self.store.login, "updated_at", 0), 0)
        if cached_account and cached_at > 0 and now_ts - cached_at <= int(LOGIN_STATE_CACHE_SECONDS or 0):
            return True, cached_account, "登录态缓存有效"
        try:
            account = await get_user_account(cookie)
        except Exception as exc:
            return False, "", f"登录态检查失败: {exc}"

        if not account:
            self.store.clear_login()
            return False, "", "登录态已失效，请重新登录"

        # 登录态有效时刷新账号名（仅在缓存过期或账号变化时落盘，避免频繁写 state.json）。
        if account != cached_account or cached_at <= 0 or now_ts - cached_at > int(LOGIN_STATE_CACHE_SECONDS or 0):
            self.store.set_login(cookie, account)
        return True, account, "登录态有效"

    def get_baidu_cloud_login_url(self) -> str:
        """返回百度网盘官方登录网址。"""
        return "https://pan.baidu.com/"

    async def check_baidu_login_state(self) -> tuple[bool, str, str]:
        """校验百度网盘登录态（轻量：仅检查缓存 Cookie）。"""
        cookie = (getattr(self.store, "baidu_login", None) and self.store.baidu_login.cookie or "").strip()
        if not cookie:
            return False, "", "未登录百度网盘"
        if "BDUSS=" not in cookie:
            return False, "", "百度网盘 Cookie 不完整（缺少 BDUSS），请重新登录"
        account = str(getattr(self.store.baidu_login, "user_account", "") or "").strip()
        return True, account, "百度网盘登录态已缓存"

    async def start_baidu_login_capture(self, timeout_seconds: int = CAPTURE_DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """启动百度网盘 Cookie 自动采集流程（CDP/CookieDB）。"""
        try:
            timeout = int(timeout_seconds or CAPTURE_DEFAULT_TIMEOUT_SECONDS)
        except Exception:
            timeout = CAPTURE_DEFAULT_TIMEOUT_SECONDS
        timeout = max(CAPTURE_MIN_TIMEOUT_SECONDS, min(CAPTURE_MAX_TIMEOUT_SECONDS, timeout))

        async with self._baidu_capture_lock:
            if self._baidu_capture_task and not self._baidu_capture_task.done():
                self._baidu_capture_task.cancel()
                try:
                    await self._baidu_capture_task
                except BaseException:
                    pass

            quick_diag: Dict[str, Any] = {"timeout_seconds": timeout}
            await self._set_baidu_capture_state(
                stage="starting",
                message="正在检查百度网盘登录态...",
                reason="",
                next_action="",
                user_account="",
                diagnostics=quick_diag,
            )

            login_ok, account, login_message = await self.check_baidu_login_state()
            quick_diag["check_message"] = login_message
            if login_ok:
                await self._set_baidu_capture_state(
                    stage="completed",
                    message=f"检测到缓存登录态：{account or '未知账号'}",
                    reason="",
                    next_action="",
                    user_account=account,
                    diagnostics={"source": "stored_cookie", "check_message": login_message},
                    source_attempts=["stored_cookie"],
                    success_source="stored_cookie",
                    source_diagnostics={
                        "stored_cookie": {
                            "ok": True,
                            "reason": "",
                            "message": login_message,
                        }
                    },
                )
                return dict(self._baidu_capture_state)

            initial_attempt = await self._attempt_capture_baidu_sources_once()
            quick_diag["initial_reason"] = str(initial_attempt.get("reason", ""))
            quick_diag["source_diagnostics"] = dict(initial_attempt.get("source_diagnostics") or {})

            if bool(initial_attempt.get("success")):
                resolved_cookie = str(initial_attempt.get("cookie", "") or "")
                resolved_account = str(initial_attempt.get("account", "") or "")
                success_source = str(initial_attempt.get("success_source", "") or "")
                if resolved_cookie:
                    self.store.set_baidu_login(resolved_cookie, resolved_account)
                    await self._set_baidu_capture_state(
                        stage="completed",
                        message=f"登录成功：{resolved_account or '百度网盘'}",
                        reason="",
                        next_action="",
                        user_account=resolved_account,
                        diagnostics={
                            "source": "initial_dual_source_probe",
                            "success_source": success_source,
                        },
                        source_attempts=list(initial_attempt.get("source_attempts") or []),
                        success_source=success_source,
                        source_diagnostics=dict(initial_attempt.get("source_diagnostics") or {}),
                    )
                    return dict(self._baidu_capture_state)

            await self._set_baidu_capture_state(
                stage="running",
                message="正在持续采集，请在浏览器完成百度网盘登录后返回...",
                reason="waiting_login",
                next_action="",
                user_account="",
                diagnostics=quick_diag,
                source_attempts=list(initial_attempt.get("source_attempts") or []),
                success_source="",
                source_diagnostics=dict(initial_attempt.get("source_diagnostics") or {}),
            )
            self._baidu_capture_task = asyncio.create_task(
                self._baidu_capture_loop(timeout_seconds=timeout, seed_diagnostics=quick_diag),
                name="freedeck_baidu_capture",
            )
            return dict(self._baidu_capture_state)

    async def get_baidu_login_capture_status(self) -> Dict[str, Any]:
        """查询百度网盘 Cookie 自动采集状态。"""
        return dict(self._baidu_capture_state)

    async def stop_baidu_login_capture(self) -> Dict[str, Any]:
        """停止百度网盘 Cookie 自动采集流程。"""
        async with self._baidu_capture_lock:
            if self._baidu_capture_task and not self._baidu_capture_task.done():
                self._baidu_capture_task.cancel()
                try:
                    await self._baidu_capture_task
                except BaseException:
                    pass
            self._baidu_capture_task = None
            await self._set_baidu_capture_state(
                stage="stopped",
                message="已停止",
                reason="stopped",
                next_action="",
                user_account=str(getattr(self.store.baidu_login, "user_account", "") or "").strip(),
                diagnostics={},
            )
            return dict(self._baidu_capture_state)

    async def clear_baidu_login(self) -> Dict[str, Any]:
        """清理百度网盘登录态。"""
        self.store.clear_baidu_login()
        await self.stop_baidu_login_capture()
        self._invalidate_panel_cache(all_data=True)
        return {"logged_in": False, "user_account": "", "message": "已注销百度网盘账号"}

    def get_ctfile_login_guide_url(self) -> str:
        """返回 CTFile token 获取引导页地址。"""
        # 说明：目前使用 ctfileGet 的公开页面作为“登录后获取 session_id”的引导，
        # 用户复制 token 后回到 Freedeck 粘贴保存即可。
        return "https://ctfile.qinlili.bid/"

    def get_ctfile_login_state(self) -> Dict[str, Any]:
        """读取 CTFile token 状态（脱敏）。"""
        token = str(getattr(self.store, "ctfile_login", None) and self.store.ctfile_login.token or "").strip()
        configured = bool(token)
        updated_at = int(getattr(self.store, "ctfile_login", None) and self.store.ctfile_login.updated_at or 0)
        return {
            "configured": configured,
            "token_hint": _mask_secret(token),
            "updated_at": updated_at,
            "message": "已配置 token（缓存）" if configured else "未配置 token",
        }

    async def set_ctfile_token(self, token: str) -> Dict[str, Any]:
        """保存 CTFile token（session_id）。"""
        normalized = str(token or "").strip()
        if not normalized:
            raise TianyiApiError("token 不能为空")
        self.store.set_ctfile_token(normalized)
        self._invalidate_panel_cache(all_data=True)
        return self.get_ctfile_login_state()

    async def clear_ctfile_token(self) -> Dict[str, Any]:
        """清除 CTFile token。"""
        self.store.clear_ctfile_token()
        self._invalidate_panel_cache(all_data=True)
        return self.get_ctfile_login_state()

    async def save_manual_cookie(self, cookie: str, user_account: str = "") -> Dict[str, Any]:
        """手动保存 cookie。"""
        normalized = (cookie or "").strip()
        if not normalized:
            raise TianyiApiError("cookie 不能为空")
        account = (user_account or "").strip()
        if not account:
            fetched = await get_user_account(normalized)
            if not fetched:
                raise TianyiApiError("cookie 无效，请重新获取")
            account = fetched
        self.store.set_login(normalized, account)
        await self._set_capture_state(
            stage="completed",
            message=f"已保存 cookie，登录账号：{account}",
            reason="",
            next_action="",
            user_account=account,
            diagnostics={"source": "manual_cookie"},
        )
        return {"logged_in": True, "user_account": account, "message": "登录态已保存"}

    async def clear_login(self) -> Dict[str, Any]:
        """清理本地登录态。"""
        self.store.clear_login()
        self._invalidate_panel_cache(all_data=True)
        async with self._qr_login_lock:
            await self._close_qr_login_context_locked()
            await self._set_qr_login_state(
                session_id="",
                stage="idle",
                message="未开始",
                reason="",
                next_action="",
                user_account="",
                image_url="",
                expires_at=0,
                diagnostics={},
            )
        await self._set_capture_state(
            stage="idle",
            message="未开始",
            reason="",
            next_action="",
            user_account="",
            diagnostics={},
        )
        await self._cancel_cloud_save_task()
        await self._set_cloud_save_state(
            stage="idle",
            running=False,
            message="未开始",
            reason="login_cleared",
            progress=0.0,
            current_game="",
            total_games=0,
            processed_games=0,
            uploaded=0,
            skipped=0,
            failed=0,
            results=[],
            diagnostics={},
        )
        return {"logged_in": False, "user_account": "", "message": "已清理登录态"}

    def _installed_record_session_key(self, record: TianyiInstalledGame) -> str:
        """为已安装记录构造稳定会话键。"""
        game_id = str(record.game_id or "").strip()
        install_path = self._normalize_dir_path(str(record.install_path or "").strip())
        if game_id and install_path:
            return f"{game_id}|{install_path}"
        if game_id:
            return f"{game_id}|"
        if install_path:
            return f"|{install_path}"
        return ""

    def _derive_tianyi_launch_token(self, game_id: str) -> str:
        """与 steam_shortcuts 使用同一规则构造 Freedeck 启动 token。"""
        token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_id or "")).strip("_")
        return token or "game"

    def _snapshot_record_playtime(self, record: TianyiInstalledGame, *, now_ts: Optional[int] = None) -> Dict[str, Any]:
        """输出记录的游玩时长快照（包含进行中的会话增量）。"""
        now = max(0, _safe_int(now_ts, 0)) or _now_wall_ts()
        total_seconds = max(0, _safe_int(record.playtime_seconds, 0))
        sessions = max(0, _safe_int(record.playtime_sessions, 0))
        last_played_at = max(0, _safe_int(record.playtime_last_played_at, 0))
        active_started_at = max(0, _safe_int(record.playtime_active_started_at, 0))
        active_app_id = max(0, _safe_int(record.playtime_active_app_id, 0))

        active_seconds = 0
        active = bool(active_started_at > 0 and active_app_id > 0)
        if active and now > active_started_at:
            active_seconds = min(now - active_started_at, PLAYTIME_SESSION_MAX_SECONDS)
        snapshot_seconds = total_seconds + active_seconds
        if active_seconds > 0:
            last_played_at = max(last_played_at, now)

        return {
            "seconds": snapshot_seconds,
            "sessions": sessions,
            "last_played_at": last_played_at,
            "active": active,
            "active_app_id": active_app_id,
            "active_started_at": active_started_at,
            "active_seconds_included": active_seconds,
        }

    async def _resolve_installed_record_by_app_id(self, app_id_unsigned: int) -> Optional[TianyiInstalledGame]:
        """按 Steam AppID 定位已安装记录，必要时自动回填 appid 缓存。"""
        target_app_id = max(0, int(app_id_unsigned or 0))
        if target_app_id <= 0:
            return None

        records = list(self.store.installed_games or [])
        for record in records:
            if max(0, _safe_int(record.steam_app_id, 0)) == target_app_id:
                return record

        # 慢路径优化：一次性读取 shortcuts，避免对每条记录重复解析 shortcuts.vdf。
        shortcut_index: Dict[str, Any] = {}
        try:
            shortcut_index = await asyncio.to_thread(list_tianyi_shortcuts_sync)
        except Exception:
            shortcut_index = {}

        by_token = shortcut_index.get("by_token", {}) if isinstance(shortcut_index, dict) else {}
        if isinstance(by_token, dict) and by_token:
            matched: Optional[TianyiInstalledGame] = None
            needs_save = False

            for record in records:
                game_id = str(record.game_id or "").strip()
                if not game_id:
                    continue
                token = self._derive_tianyi_launch_token(game_id)
                row = by_token.get(token)
                if not isinstance(row, dict):
                    continue
                resolved_app_id = max(0, _safe_int(row.get("appid_unsigned"), 0))
                if resolved_app_id <= 0:
                    continue
                if resolved_app_id != max(0, _safe_int(record.steam_app_id, 0)):
                    record.steam_app_id = resolved_app_id
                    needs_save = True
                if resolved_app_id == target_app_id and matched is None:
                    matched = record

            if needs_save:
                await asyncio.to_thread(self.store.save)
            return matched

        matched: Optional[TianyiInstalledGame] = None
        needs_save = False
        for record in records:
            game_id = str(record.game_id or "").strip()
            if not game_id:
                continue
            try:
                shortcut = await asyncio.to_thread(resolve_tianyi_shortcut_sync, game_id=game_id)
            except Exception:
                continue
            if not bool(shortcut.get("ok")):
                continue
            resolved_app_id = max(0, _safe_int(shortcut.get("appid_unsigned"), 0))
            if resolved_app_id <= 0:
                continue
            if resolved_app_id != max(0, _safe_int(record.steam_app_id, 0)):
                record.steam_app_id = resolved_app_id
                needs_save = True
            if resolved_app_id == target_app_id and matched is None:
                matched = record

        if needs_save:
            await asyncio.to_thread(self.store.save)
        return matched

    async def _recover_playtime_sessions_from_store(self) -> None:
        """从 state.json 恢复进行中的游玩会话。"""
        now = _now_wall_ts()
        recovered: Dict[str, Dict[str, Any]] = {}
        stale_updated = False

        for record in list(self.store.installed_games or []):
            active_started_at = max(0, _safe_int(record.playtime_active_started_at, 0))
            active_app_id = max(0, _safe_int(record.playtime_active_app_id, 0))
            if active_started_at <= 0 or active_app_id <= 0:
                continue

            age = max(0, now - active_started_at)
            if age > PLAYTIME_STALE_SESSION_SECONDS:
                record.playtime_active_started_at = 0
                record.playtime_active_app_id = 0
                stale_updated = True
                continue

            key = self._installed_record_session_key(record)
            if not key:
                continue
            recovered[key] = {
                "game_id": str(record.game_id or "").strip(),
                "install_path": str(record.install_path or "").strip(),
                "app_id": active_app_id,
                "started_at": active_started_at,
            }

        async with self._playtime_lock:
            self._playtime_sessions = recovered

        if stale_updated:
            await asyncio.to_thread(self.store.save)
            self._invalidate_panel_cache(installed=True)

    async def _finalize_active_playtime_sessions(self, *, reason: str = "") -> None:
        """在服务关闭等场景结算所有活跃游玩会话。"""
        del reason  # 预留给后续诊断扩展。

        async with self._playtime_lock:
            active_sessions = dict(self._playtime_sessions or {})
            self._playtime_sessions = {}

        if not active_sessions:
            return

        now = _now_wall_ts()
        changed = False
        for session in active_sessions.values():
            game_id = str(session.get("game_id", "") or "").strip()
            install_path = str(session.get("install_path", "") or "").strip()
            started_at = max(0, _safe_int(session.get("started_at"), 0))

            record = self._find_installed_record(game_id=game_id, install_path=install_path)
            if record is None and game_id:
                record = self._find_installed_record(game_id=game_id)
            if record is None:
                continue

            record_changed = False
            if started_at > 0 and now > started_at:
                added = min(now - started_at, PLAYTIME_SESSION_MAX_SECONDS)
                if added > 0:
                    record.playtime_seconds = max(0, _safe_int(record.playtime_seconds, 0)) + added
                    record.playtime_sessions = max(0, _safe_int(record.playtime_sessions, 0)) + 1
                    record.playtime_last_played_at = max(0, now)
                    record_changed = True
            if max(0, _safe_int(record.playtime_active_started_at, 0)) > 0:
                record.playtime_active_started_at = 0
                record_changed = True
            if max(0, _safe_int(record.playtime_active_app_id, 0)) > 0:
                record.playtime_active_app_id = 0
                record_changed = True

            if record_changed:
                record.updated_at = now
                changed = True

        if changed:
            await asyncio.to_thread(self.store.save)
            self._invalidate_panel_cache(installed=True)

    async def record_game_action(self, *, phase: str, app_id: str, action_name: str = "") -> Dict[str, Any]:
        """记录 Steam 启动/退出事件并累计游玩时长。"""
        normalized_phase = str(phase or "").strip().lower()
        if normalized_phase not in {"start", "end"}:
            return {"accepted": False, "reason": "invalid_phase", "message": "无效的 phase"}

        app_id_unsigned = _to_uint32_app_id(app_id, 0)
        if app_id_unsigned <= 0:
            return {"accepted": False, "reason": "invalid_app_id", "message": "无效的 app_id"}

        normalized_action = str(action_name or "").strip()
        if normalized_phase == "start" and normalized_action and normalized_action != "LaunchApp":
            return {
                "accepted": False,
                "reason": "ignored_action",
                "message": "非 LaunchApp 事件已忽略",
                "action_name": normalized_action,
            }

        record = await self._resolve_installed_record_by_app_id(app_id_unsigned)
        if record is None:
            return {
                "accepted": False,
                "reason": "app_not_managed",
                "message": "该 AppID 不属于 Freedeck 安装记录",
                "app_id": app_id_unsigned,
            }

        session_key = self._installed_record_session_key(record)
        if not session_key:
            return {"accepted": False, "reason": "record_invalid", "message": "已安装记录无效"}

        now = _now_wall_ts()
        changed = False
        added_seconds = 0
        active_session_ended = False
        now_record = self._find_installed_record(game_id=record.game_id, install_path=record.install_path) or record
        duplicate_start_grace_seconds = 5

        async with self._playtime_lock:
            existing = dict(self._playtime_sessions.get(session_key) or {})
            started_at = max(
                0,
                _safe_int(
                    existing.get("started_at"),
                    _safe_int(now_record.playtime_active_started_at, 0),
                ),
            )

            if normalized_phase == "start":
                if started_at > 0 and now > started_at:
                    elapsed = now - started_at
                    if elapsed > duplicate_start_grace_seconds:
                        added_seconds = min(elapsed, PLAYTIME_SESSION_MAX_SECONDS)
                        if added_seconds > 0:
                            now_record.playtime_seconds = max(0, _safe_int(now_record.playtime_seconds, 0)) + added_seconds
                            now_record.playtime_sessions = max(0, _safe_int(now_record.playtime_sessions, 0)) + 1
                            now_record.playtime_last_played_at = max(0, now)
                            changed = True

                self._playtime_sessions[session_key] = {
                    "game_id": str(now_record.game_id or "").strip(),
                    "install_path": str(now_record.install_path or "").strip(),
                    "app_id": app_id_unsigned,
                    "started_at": now,
                }
                if max(0, _safe_int(now_record.playtime_active_started_at, 0)) != now:
                    now_record.playtime_active_started_at = now
                    changed = True
                if max(0, _safe_int(now_record.playtime_active_app_id, 0)) != app_id_unsigned:
                    now_record.playtime_active_app_id = app_id_unsigned
                    changed = True
                if max(0, _safe_int(now_record.steam_app_id, 0)) != app_id_unsigned:
                    now_record.steam_app_id = app_id_unsigned
                    changed = True
            else:
                active_session_ended = started_at > 0
                if started_at > 0 and now > started_at:
                    added_seconds = min(now - started_at, PLAYTIME_SESSION_MAX_SECONDS)
                    if added_seconds > 0:
                        now_record.playtime_seconds = max(0, _safe_int(now_record.playtime_seconds, 0)) + added_seconds
                        now_record.playtime_sessions = max(0, _safe_int(now_record.playtime_sessions, 0)) + 1
                        now_record.playtime_last_played_at = max(0, now)
                        changed = True

                if session_key in self._playtime_sessions:
                    self._playtime_sessions.pop(session_key, None)
                if max(0, _safe_int(now_record.playtime_active_started_at, 0)) > 0:
                    now_record.playtime_active_started_at = 0
                    changed = True
                if max(0, _safe_int(now_record.playtime_active_app_id, 0)) > 0:
                    now_record.playtime_active_app_id = 0
                    changed = True
                if max(0, _safe_int(now_record.steam_app_id, 0)) != app_id_unsigned:
                    now_record.steam_app_id = app_id_unsigned
                    changed = True

            if changed:
                now_record.updated_at = now
                await asyncio.to_thread(self.store.save)

        if changed:
            self._invalidate_panel_cache(installed=True)

        auto_upload_enabled = bool(getattr(self.store.settings, "cloud_save_auto_upload", False))
        if normalized_phase == "end" and active_session_ended and auto_upload_enabled:
            target_record = copy.deepcopy(now_record)
            config.logger.info(
                "Cloud save auto upload queued: game=%s game_id=%s app_id=%s",
                str(now_record.game_title or "").strip() or "未命名游戏",
                str(now_record.game_id or "").strip(),
                app_id_unsigned,
            )
            asyncio.create_task(
                self._auto_upload_cloud_save_for_record(record=target_record, app_id=app_id_unsigned),
                name=f"freedeck-cloud-save-auto-{app_id_unsigned}",
            )

        playtime = self._snapshot_record_playtime(now_record, now_ts=now)
        return {
            "accepted": True,
            "reason": "",
            "message": "记录成功",
            "phase": normalized_phase,
            "app_id": app_id_unsigned,
            "game_id": str(now_record.game_id or "").strip(),
            "game_title": str(now_record.game_title or "").strip(),
            "playtime_seconds": max(0, _safe_int(playtime.get("seconds"), 0)),
            "playtime_sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
            "added_seconds": max(0, int(added_seconds or 0)),
            "auto_upload_enabled": auto_upload_enabled,
            "auto_upload_queued": bool(normalized_phase == "end" and active_session_ended and auto_upload_enabled),
        }

    async def get_library_game_time_stats(self, *, app_id: str = "", title: str = "") -> Dict[str, Any]:
        """按 Steam 库页面游戏返回 Freedeck 游玩时长与最后运行时间。"""
        app_id_unsigned = _to_uint32_app_id(app_id, 0)
        title_raw = str(title or "").strip()
        title_norm = self._normalize_cover_text(title_raw)

        record: Optional[TianyiInstalledGame] = None
        if app_id_unsigned > 0:
            record = await self._resolve_installed_record_by_app_id(app_id_unsigned)

        if record is None and title_norm:
            for candidate in list(self.store.installed_games or []):
                candidate_title = str(candidate.game_title or "").strip()
                if not candidate_title:
                    continue
                candidate_norm = self._normalize_cover_text(candidate_title)
                if not candidate_norm:
                    continue
                if candidate_norm == title_norm or candidate_norm in title_norm or title_norm in candidate_norm:
                    record = candidate
                    break

        if record is None:
            return {
                "managed": False,
                "reason": "not_managed",
                "message": "当前库页面游戏不属于 Freedeck 安装记录",
                "app_id": app_id_unsigned,
                "title": title_raw,
                "my_playtime_seconds": 0,
                "my_playtime_text": "-",
                "my_playtime_active": False,
                "last_played_at": 0,
            }

        # 如果库页能命中 managed 记录，但 appid 映射不一致，趁机补齐以便后续 start/end 事件能正确累计。
        if app_id_unsigned > 0 and max(0, _safe_int(record.steam_app_id, 0)) != app_id_unsigned:
            try:
                # 避免误覆盖：只有当没有其它记录占用该 app_id 时才更新。
                occupied = False
                for item in list(self.store.installed_games or []):
                    if item is record:
                        continue
                    if max(0, _safe_int(item.steam_app_id, 0)) == app_id_unsigned:
                        occupied = True
                        break
                if not occupied:
                    record.steam_app_id = int(app_id_unsigned)
                    record.updated_at = _now_wall_ts()
                    await asyncio.to_thread(self.store.save)
            except Exception:
                # 忽略保存失败，避免影响库页读接口。
                pass

        game_id = str(record.game_id or "").strip()
        game_title = str(record.game_title or title_raw or "").strip()
        playtime = self._snapshot_record_playtime(record)
        my_playtime_seconds = max(0, _safe_int(playtime.get("seconds"), 0))
        my_playtime_text = _format_playtime_seconds(my_playtime_seconds)
        last_played_at = max(0, _safe_int(playtime.get("last_played_at"), 0))

        return {
            "managed": True,
            "reason": "",
            "message": "",
            "app_id": app_id_unsigned,
            "game_id": game_id,
            "title": game_title,
            "my_playtime_seconds": my_playtime_seconds,
            "my_playtime_text": my_playtime_text,
            "my_playtime_active": bool(playtime.get("active")),
            "last_played_at": last_played_at,
        }

    async def start_qr_login(self) -> Dict[str, Any]:
        """启动后端二维码登录会话。"""
        async with self._qr_login_lock:
            await self._close_qr_login_context_locked()

            login_ok, account, message = await self.check_login_state()
            if login_ok and account:
                await self._set_qr_login_state(
                    session_id="",
                    stage="completed",
                    message=f"检测到有效登录态：{account}",
                    reason="",
                    next_action="",
                    user_account=account,
                    image_url="",
                    expires_at=0,
                    diagnostics={"source": "stored_cookie", "check_message": message},
                )
                return dict(self._qr_login_state)

            session_id = uuid.uuid4().hex
            created_at = _now_wall_ts()
            expires_at = created_at + QR_LOGIN_SESSION_TIMEOUT_SECONDS
            timeout = aiohttp.ClientTimeout(total=QR_LOGIN_HTTP_TIMEOUT_SECONDS)
            ssl_context, tls_diag = self._build_qr_ssl_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            client = aiohttp.ClientSession(
                timeout=timeout,
                cookie_jar=aiohttp.CookieJar(unsafe=True),
                connector=connector,
            )
            context: Dict[str, Any] = {
                "session_id": session_id,
                "client": client,
                "created_at": created_at,
                "expires_at": expires_at,
                "poll_count": 0,
                "tls_diag": tls_diag,
            }

            try:
                bootstrap = await self._bootstrap_qr_login_context(context)
                context.update(bootstrap)
                context["image_url"] = f"/api/tianyi/login/qr/image?session_id={session_id}&_ts={_now_wall_ts()}"
                self._qr_login_context = context

                await self._set_qr_login_state(
                    session_id=session_id,
                    stage="running",
                    message="请使用天翼云盘 App 扫码登录",
                    reason="waiting_scan",
                    next_action="scan_qr",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={
                        "source": "qr_api",
                        "created_at": created_at,
                        "expires_at": expires_at,
                        "req_id": str(context.get("req_id", "")),
                        "tls": tls_diag,
                    },
                )
                return dict(self._qr_login_state)
            except Exception as exc:
                await self._safe_close_client_session(client)
                error_text = str(exc)
                reason = "qr_start_failed"
                if "CERTIFICATE_VERIFY_FAILED" in error_text.upper() or "certificate verify failed" in error_text.lower():
                    reason = "ssl_verify_failed"
                await self._set_qr_login_state(
                    session_id="",
                    stage="failed",
                    message=f"二维码会话启动失败：{exc}",
                    reason=reason,
                    next_action="retry",
                    user_account="",
                    image_url="",
                    expires_at=0,
                    diagnostics={
                        "exception": error_text,
                        "tls": tls_diag,
                    },
                )
                return dict(self._qr_login_state)

    async def poll_qr_login(self, session_id: str = "") -> Dict[str, Any]:
        """轮询二维码登录状态。"""
        async with self._qr_login_lock:
            context = self._qr_login_context
            if context is None:
                return dict(self._qr_login_state)

            current_id = str(context.get("session_id", ""))
            if session_id and session_id != current_id:
                return dict(self._qr_login_state)

            expires_at = int(context.get("expires_at") or 0)
            now_ts = _now_wall_ts()
            if expires_at > 0 and now_ts >= expires_at:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="二维码已过期，请刷新后重试",
                    reason="qr_expired",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={"poll_count": int(context.get("poll_count") or 0)},
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            client = context.get("client")
            if not isinstance(client, aiohttp.ClientSession):
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="二维码会话异常，请刷新后重试",
                    reason="qr_context_invalid",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={},
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            state_payload = dict(context.get("state_payload") or {})
            now_ms = str(int(time.time() * 1000))
            state_payload["date"] = now_ms
            state_payload["timeStamp"] = now_ms

            req_id = str(context.get("req_id", ""))
            lt = str(context.get("lt", ""))
            login_page_url = str(context.get("login_page_url", ""))
            headers = self._build_qr_headers(req_id=req_id, lt=lt, referer=login_page_url)

            try:
                async with client.post(
                    "https://open.e.189.cn/api/logbox/oauth2/qrcodeLoginState.do",
                    data=state_payload,
                    headers=headers,
                ) as resp:
                    raw_text = await resp.text()
                    if resp.status >= 400:
                        raise TianyiApiError(f"二维码状态接口失败 status={resp.status}")
            except Exception as exc:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="状态轮询失败，正在重试...",
                    reason="poll_exception",
                    next_action="wait",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={"exception": str(exc), "poll_count": int(context.get("poll_count") or 0)},
                )
                return dict(self._qr_login_state)

            try:
                payload = self._parse_json_like_text(raw_text)
            except Exception as exc:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="状态解析失败，正在重试...",
                    reason="poll_parse_failed",
                    next_action="wait",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={"exception": str(exc), "raw": str(raw_text)[:320]},
                )
                return dict(self._qr_login_state)

            status_code = self._extract_qr_status_code(payload)
            context["poll_count"] = int(context.get("poll_count") or 0) + 1

            poll_diag: Dict[str, Any] = {
                "poll_count": int(context.get("poll_count") or 0),
                "status_code": status_code,
            }

            if status_code == QR_STATUS_SUCCESS:
                redirect_url = self._extract_qr_redirect_url(payload)
                account, cookie, verify_reason = await self._finalize_qr_login_success(
                    context=context,
                    redirect_url=redirect_url,
                )
                poll_diag["redirect_url"] = redirect_url
                if verify_reason:
                    poll_diag["verify_reason"] = verify_reason

                if account and cookie:
                    self.store.set_login(cookie, account)
                    await self._set_qr_login_state(
                        session_id=current_id,
                        stage="completed",
                        message=f"登录成功：{account}",
                        reason="",
                        next_action="",
                        user_account=account,
                        image_url=str(context.get("image_url", "")),
                        expires_at=expires_at,
                        diagnostics=poll_diag,
                    )
                    await self._close_qr_login_context_locked()
                    return dict(self._qr_login_state)

                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="扫码已确认，但未拿到有效登录态",
                    reason="qr_cookie_verify_failed",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_EXPIRED:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="二维码已失效，请刷新后重试",
                    reason="qr_expired",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_SCANNED_WAIT_CONFIRM:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="已扫码，请在手机上确认登录",
                    reason="await_confirm",
                    next_action="confirm_on_phone",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_NEED_EXTRA_VERIFY:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="账号触发二次验证，请在天翼云官方页面完成验证后重试",
                    reason="need_extra_verify",
                    next_action="open_official_login",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_WAITING:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="等待扫码登录",
                    reason="waiting_scan",
                    next_action="scan_qr",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                return dict(self._qr_login_state)

            await self._set_qr_login_state(
                session_id=current_id,
                stage="running",
                message="正在等待登录状态更新...",
                reason="polling",
                next_action="wait",
                user_account="",
                image_url=str(context.get("image_url", "")),
                expires_at=expires_at,
                diagnostics=poll_diag,
            )
            return dict(self._qr_login_state)

    async def stop_qr_login(self, session_id: str = "") -> Dict[str, Any]:
        """停止二维码登录会话。"""
        async with self._qr_login_lock:
            context = self._qr_login_context
            if context is not None:
                current_id = str(context.get("session_id", ""))
                if not session_id or session_id == current_id:
                    await self._close_qr_login_context_locked()
                    await self._set_qr_login_state(
                        session_id=current_id,
                        stage="stopped",
                        message="已停止二维码登录",
                        reason="qr_stopped",
                        next_action="retry",
                        user_account="",
                        image_url="",
                        expires_at=0,
                        diagnostics={},
                    )
                    return dict(self._qr_login_state)
            return dict(self._qr_login_state)

    async def get_qr_login_state(self) -> Dict[str, Any]:
        """读取二维码登录状态。"""
        return dict(self._qr_login_state)

    async def get_qr_login_image(self, session_id: str = "") -> Tuple[bytes, str]:
        """读取二维码图片二进制。"""
        async with self._qr_login_lock:
            context = self._qr_login_context
            if context is None:
                raise TianyiApiError("二维码会话不存在，请先刷新二维码")

            current_id = str(context.get("session_id", ""))
            if session_id and session_id != current_id:
                raise TianyiApiError("二维码会话已更新，请刷新页面")

            client = context.get("client")
            if not isinstance(client, aiohttp.ClientSession):
                raise TianyiApiError("二维码会话异常，请刷新二维码")

            image_remote_url = str(context.get("image_remote_url", ""))
            if not image_remote_url:
                raise TianyiApiError("二维码地址缺失，请刷新二维码")

            headers = self._build_qr_headers(
                req_id=str(context.get("req_id", "")),
                lt=str(context.get("lt", "")),
                referer=str(context.get("login_page_url", "")),
            )
            async with client.get(image_remote_url, headers=headers) as resp:
                if resp.status >= 400:
                    raise TianyiApiError(f"二维码图片获取失败 status={resp.status}")
                content_type = str(resp.headers.get("Content-Type", "image/jpeg") or "image/jpeg")
                body = await resp.read()
                if not body:
                    raise TianyiApiError("二维码图片为空，请刷新二维码")
                return body, content_type

    async def start_login_capture(self, timeout_seconds: int = CAPTURE_DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """启动自动 Cookie 采集流程。"""
        try:
            timeout = int(timeout_seconds or CAPTURE_DEFAULT_TIMEOUT_SECONDS)
        except Exception:
            timeout = CAPTURE_DEFAULT_TIMEOUT_SECONDS
        timeout = max(CAPTURE_MIN_TIMEOUT_SECONDS, min(CAPTURE_MAX_TIMEOUT_SECONDS, timeout))

        async with self._capture_lock:
            if self._capture_task and not self._capture_task.done():
                self._capture_task.cancel()
                try:
                    await self._capture_task
                except BaseException:
                    pass

            quick_diag: Dict[str, Any] = {"timeout_seconds": timeout}
            await self._set_capture_state(
                stage="starting",
                message="正在检查当前登录态...",
                reason="",
                next_action="",
                user_account="",
                diagnostics=quick_diag,
            )

            # 优先走本地已存登录态的快速校验，避免用户已登录却仍等待超时。
            login_ok, account, login_message = await self.check_login_state()
            quick_diag["check_message"] = login_message
            if login_ok and account:
                await self._set_capture_state(
                    stage="completed",
                    message=f"检测到有效登录态：{account}",
                    reason="",
                    next_action="",
                    user_account=account,
                    diagnostics={"source": "stored_cookie", "check_message": login_message},
                    source_attempts=["stored_cookie"],
                    success_source="stored_cookie",
                    source_diagnostics={
                        "stored_cookie": {
                            "ok": True,
                            "reason": "",
                            "message": login_message,
                        }
                    },
                )
                return dict(self._capture_state)

            # 入页后立即执行一次双通道采集尝试，命中即马上回传并落盘。
            initial_attempt = await self._attempt_capture_sources_once()
            quick_diag["initial_reason"] = str(initial_attempt.get("reason", ""))
            quick_diag["main_landing_detected"] = bool(initial_attempt.get("main_landing_detected"))
            quick_diag["source_diagnostics"] = dict(initial_attempt.get("source_diagnostics") or {})

            if bool(initial_attempt.get("success")):
                resolved_cookie = str(initial_attempt.get("cookie", "") or "")
                resolved_account = str(initial_attempt.get("account", "") or "")
                success_source = str(initial_attempt.get("success_source", "") or "")
                if resolved_cookie and resolved_account:
                    self.store.set_login(resolved_cookie, resolved_account)
                    await self._set_capture_state(
                        stage="completed",
                        message=f"登录成功：{resolved_account}",
                        reason="",
                        next_action="",
                        user_account=resolved_account,
                        diagnostics={
                            "source": "initial_dual_source_probe",
                            "success_source": success_source,
                            "main_landing_detected": bool(initial_attempt.get("main_landing_detected")),
                        },
                        source_attempts=list(initial_attempt.get("source_attempts") or []),
                        success_source=success_source,
                        source_diagnostics=dict(initial_attempt.get("source_diagnostics") or {}),
                    )
                    return dict(self._capture_state)

            await self._set_capture_state(
                stage="starting",
                message="正在启动持续采集，请在网页完成扫码登录...",
                reason="",
                next_action="",
                user_account="",
                diagnostics=quick_diag,
                source_attempts=list(initial_attempt.get("source_attempts") or []),
                success_source="",
                source_diagnostics=dict(initial_attempt.get("source_diagnostics") or {}),
            )
            self._capture_task = asyncio.create_task(
                self._capture_loop(timeout_seconds=timeout, seed_diagnostics=quick_diag),
                name="freedeck_tianyi_capture",
            )
            return dict(self._capture_state)

    async def stop_login_capture(self) -> Dict[str, Any]:
        """停止自动 Cookie 采集流程。"""
        async with self._capture_lock:
            if self._capture_task and not self._capture_task.done():
                self._capture_task.cancel()
                try:
                    await self._capture_task
                except BaseException:
                    pass
            self._capture_task = None

            await self._set_capture_state(
                stage="stopped",
                message="已停止自动采集，可改用手动 Cookie",
                reason="capture_stopped",
                next_action="manual_cookie",
                user_account="",
                diagnostics={},
            )
            return dict(self._capture_state)

    async def get_login_capture_status(self) -> Dict[str, Any]:
        """读取当前自动采集状态。"""
        return dict(self._capture_state)

    async def list_catalog(self, query: str, page: int, page_size: int, sort_mode: str = "default") -> Dict[str, Any]:
        """查询游戏目录。"""
        # page_size 默认跟随设置，但允许前端覆盖。
        if page_size <= 0:
            page_size = self.store.settings.page_size
        return self.catalog.list(query=query, page=page, page_size=page_size, sort_mode=sort_mode)

    async def list_switch_catalog(self, query: str, page: int, page_size: int, sort_mode: str = "default") -> Dict[str, Any]:
        """查询 Switch 模拟器资源目录（独立入口）。"""
        if page_size <= 0:
            page_size = self.store.settings.page_size
        return self.catalog.list_switch(query=query, page=page, page_size=page_size, sort_mode=sort_mode)

    async def list_gba_catalog(self, query: str, page: int, page_size: int) -> Dict[str, Any]:
        """查询 GBA 模拟器资源目录（静态 CSV）。"""
        if page_size <= 0:
            page_size = self.store.settings.page_size
        if self.gba_catalog.load_error:
            raise ValueError(self.gba_catalog.load_error)
        return self.gba_catalog.list(query=query, page=page, page_size=page_size)

    async def resolve_catalog_cover(
        self,
        *,
        game_id: str = "",
        title: str = "",
        categories: str = "",
    ) -> Dict[str, Any]:
        """按游戏标题解析封面 URL（优先 Steam 商店），并做内存缓存。"""
        cache_key = str(game_id or title or "").strip().lower()
        now_ts = _now_wall_ts()
        if not cache_key:
            return {
                "cover_url": "",
                "square_cover_url": "",
                "source": "",
                "matched_title": "",
                "app_id": 0,
                "protondb_tier": "",
                "cached": False,
            }

        catalog_entry = None
        catalog_app_id = 0
        catalog_cover_url = ""
        catalog_square_cover_url = ""
        try:
            if str(game_id or "").strip():
                catalog_entry = self.catalog.get_by_game_id(str(game_id or "").strip())
                catalog_app_id = _safe_int(getattr(catalog_entry, "app_id", 0), 0) if catalog_entry else 0
                catalog_cover_url = str(getattr(catalog_entry, "cover_url", "") or "").strip() if catalog_entry else ""
                catalog_square_cover_url = str(getattr(catalog_entry, "square_cover_url", "") or "").strip() if catalog_entry else ""
        except Exception:
            catalog_entry = None
            catalog_app_id = 0
            catalog_cover_url = ""
            catalog_square_cover_url = ""

        effective_title = str(title or "").strip()
        if not effective_title and catalog_entry is not None:
            effective_title = str(getattr(catalog_entry, "title", "") or "").strip()

        is_switch_entry = False
        try:
            is_switch_entry = bool(
                catalog_entry is not None and str(getattr(catalog_entry, "category_parent", "") or "").strip() == "527"
            )
        except Exception:
            is_switch_entry = False

        steamgriddb_key = ""
        try:
            steamgriddb_enabled = bool(getattr(self.store.settings, "steamgriddb_enabled", False))
        except Exception:
            steamgriddb_enabled = False

        # 默认情况下仅在 Switch 模拟器资源中使用 SteamGridDB（避免大量封面预取耗尽公共 Key 配额）。
        # 如用户在设置中显式开启，则非 Switch 游戏也会尝试升级竖版封面。
        if (is_switch_entry or steamgriddb_enabled) and self._steamgriddb_available():
            steamgriddb_key = resolve_steamgriddb_api_key(getattr(self.store.settings, "steamgriddb_api_key", ""))

        cached: Optional[Dict[str, Any]] = None
        async with self._catalog_cover_lock:
            value = self._catalog_cover_cache.get(cache_key)
            if isinstance(value, dict) and int(value.get("expires_at", 0)) > now_ts:
                cached = dict(value)

        if cached is not None:
            cached_app_id = _safe_int(cached.get("app_id"), 0)
            if catalog_app_id > 0 and cached_app_id != catalog_app_id:
                # The shipped catalog may include an authoritative Steam AppID mapping. If a previous fuzzy search
                # result is cached for the same game_id, prefer the catalog AppID to avoid wrong covers.
                cached = None
            else:
                if catalog_app_id > 0 and cached_app_id <= 0:
                    cached["app_id"] = catalog_app_id
                    cached_app_id = catalog_app_id
                if catalog_cover_url and not str(cached.get("cover_url", "") or "").strip():
                    cached["cover_url"] = catalog_cover_url
                if catalog_square_cover_url and not str(cached.get("square_cover_url", "") or "").strip():
                    cached["square_cover_url"] = catalog_square_cover_url
                if (
                    (catalog_cover_url or catalog_square_cover_url)
                    and str(cached.get("source", "") or "").strip() in {"", "steam_appid"}
                ):
                    cached["source"] = "catalog_cover"
                cached_source = str(cached.get("source", "") or "").strip()
                if steamgriddb_key and cached_app_id > 0 and cached_source != "steamgriddb":
                    try:
                        sgdb_portrait = await resolve_steamgriddb_portrait_grid(
                            api_key=steamgriddb_key,
                            steam_app_id=cached_app_id,
                        )
                        http_status = _safe_int(sgdb_portrait.get("http_status"), 0)
                        if http_status in {429, 500, 502, 503, 504}:
                            self._mark_steamgriddb_unavailable(http_status=http_status)
                        portrait_url = str(sgdb_portrait.get("portrait") or "").strip()
                        if bool(sgdb_portrait.get("ok")) and portrait_url:
                            cached["square_cover_url"] = portrait_url
                            cached["source"] = "steamgriddb"
                            async with self._catalog_cover_lock:
                                current = self._catalog_cover_cache.get(cache_key)
                                if (
                                    isinstance(current, dict)
                                    and int(current.get("expires_at", 0)) > now_ts
                                    and _safe_int(current.get("app_id"), 0) == cached_app_id
                                ):
                                    current["square_cover_url"] = portrait_url
                                    current["source"] = "steamgriddb"
                    except Exception as exc:
                        config.logger.warning(
                            "SteamGridDB portrait upgrade failed app=%s title=%s: %s",
                            cached_app_id,
                            effective_title,
                            exc,
                        )

                if (
                    steamgriddb_key
                    and not str(cached.get("square_cover_url", "") or "").strip()
                    and cached_source != "steamgriddb"
                ):
                    try:
                        terms = self._build_catalog_cover_terms(title=effective_title, categories=categories)
                        sgdb_fallback = await self._resolve_steamgriddb_portrait_by_terms(
                            api_key=steamgriddb_key,
                            terms=terms,
                        )
                        http_status = _safe_int(sgdb_fallback.get("http_status"), 0)
                        if http_status in {429, 500, 502, 503, 504}:
                            self._mark_steamgriddb_unavailable(http_status=http_status)
                        portrait_url = str(sgdb_fallback.get("portrait") or "").strip()
                        if bool(sgdb_fallback.get("ok")) and portrait_url:
                            cached["square_cover_url"] = portrait_url
                            cached["source"] = "steamgriddb"
                            cached["matched_title"] = str(
                                sgdb_fallback.get("matched_title") or cached.get("matched_title") or ""
                            )
                            async with self._catalog_cover_lock:
                                current = self._catalog_cover_cache.get(cache_key)
                                if isinstance(current, dict) and int(current.get("expires_at", 0)) > now_ts:
                                    current["square_cover_url"] = portrait_url
                                    current["source"] = "steamgriddb"
                                    current["matched_title"] = cached["matched_title"]
                    except Exception as exc:
                        config.logger.warning("SteamGridDB search upgrade failed title=%s: %s", effective_title, exc)

                return {
                    "cover_url": str(cached.get("cover_url", "") or ""),
                    "square_cover_url": str(cached.get("square_cover_url", "") or ""),
                    "source": str(cached.get("source", "") or ""),
                    "matched_title": str(cached.get("matched_title", "") or ""),
                    "app_id": cached_app_id,
                    "protondb_tier": str(cached.get("protondb_tier", "") or ""),
                    "cached": True,
                }

        cover_url = catalog_cover_url
        square_cover_url = catalog_square_cover_url
        source = "catalog_cover" if (catalog_cover_url or catalog_square_cover_url) else ""
        matched_title = effective_title if (catalog_cover_url or catalog_square_cover_url or catalog_app_id > 0) else ""
        app_id = int(catalog_app_id) if catalog_app_id > 0 else 0
        protondb_tier = ""
        terms = self._build_catalog_cover_terms(title=effective_title, categories=categories)

        if app_id > 0:
            if not cover_url:
                cover_url = self._build_store_cover_url_from_app_id(app_id)
            if not square_cover_url:
                square_cover_url = self._build_store_square_cover_url(app_id)
            if not source:
                source = "steam_appid"
            if not matched_title:
                matched_title = effective_title
            if not is_switch_entry:
                try:
                    ssl_context, _ = self._build_qr_ssl_context()
                    timeout = aiohttp.ClientTimeout(total=PROTONDB_HTTP_TIMEOUT_SECONDS)
                    connector = aiohttp.TCPConnector(ssl=ssl_context)
                    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                        proton_summary = await self._fetch_protondb_summary(session=session, app_id=app_id)
                        protondb_tier = str(proton_summary.get("tier", "") or "").strip()
                except Exception:
                    protondb_tier = ""

        # Switch 模拟器资源优先走 SteamGridDB（更贴近主机封面，且避免 Steam 搜索误匹配/耗时）。
        if terms and app_id <= 0 and not is_switch_entry:
            try:
                ssl_context, _ = self._build_qr_ssl_context()
                timeout = aiohttp.ClientTimeout(total=CATALOG_COVER_HTTP_TIMEOUT_SECONDS)
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    headers = {
                        "Accept": "application/json, text/plain, */*",
                        "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
                        "Referer": "https://store.steampowered.com/",
                    }

                    candidate_by_app: Dict[int, Dict[str, Any]] = {}
                    best_score_seen = -1

                    async def fetch_store_items(term_text: str, *, cc: str) -> List[Dict[str, Any]]:
                        query = str(term_text or "").strip()
                        if not query:
                            return []
                        search_url = str(
                            URL("https://store.steampowered.com/api/storesearch/").with_query(
                                {"term": query, "l": "schinese", "cc": str(cc or "us")}
                            )
                        )
                        try:
                            async with session.get(search_url, headers=headers) as resp:
                                if int(resp.status) != 200:
                                    return []
                                payload = await resp.json(content_type=None)
                        except Exception:
                            return []
                        items = payload.get("items") if isinstance(payload, dict) else []
                        return [item for item in items if isinstance(item, dict)]

                    for term in terms:
                        search_term = str(term or "").strip()
                        if not search_term:
                            continue

                        # NOTE: Keep '-' at the beginning (or escaped) to avoid accidentally forming a Unicode range
                        # in the character class. A previous pattern like `[\\-–…]` could match almost everything
                        # between '\' and '–', causing lowercase letters to be replaced (e.g. "Outer Wilds" -> "O W").
                        normalized = re.sub(r"[-–—:：\\]+", " ", search_term)
                        normalized = re.sub(r"\s+", " ", normalized).strip()
                        shrink_tokens: List[str] = []
                        if normalized and normalized != search_term:
                            shrink_tokens.append(normalized)
                        shrink_tokens.append(search_term)

                        for cc in ("cn", "us"):
                            if best_score_seen >= 240:
                                break

                            for candidate in shrink_tokens:
                                items = await fetch_store_items(candidate, cc=cc)
                                if not items and re.search(r"[A-Za-z]", candidate):
                                    tokens = normalized.split(" ") if normalized else candidate.split(" ")
                                    max_drop = min(4, max(0, len(tokens) - 2))
                                    for drop in range(1, max_drop + 1):
                                        shrunk = " ".join(tokens[: len(tokens) - drop]).strip()
                                        if not shrunk or len(shrunk) < 4:
                                            continue
                                        items = await fetch_store_items(shrunk, cc=cc)
                                        if items:
                                            break

                                if not items:
                                    continue

                                ranked = self._rank_catalog_cover_candidates(term=search_term, items=items, limit=3)
                                for resolved in ranked:
                                    resolved_app_id = _safe_int(resolved.get("app_id"), 0)
                                    if resolved_app_id <= 0:
                                        continue
                                    resolved["cc"] = cc
                                    resolved["term"] = search_term
                                    prev = candidate_by_app.get(resolved_app_id)
                                    if prev is None or int(resolved.get("match_score", 0)) > int(prev.get("match_score", 0)):
                                        candidate_by_app[resolved_app_id] = dict(resolved)
                                    best_score_seen = max(best_score_seen, int(resolved.get("match_score", 0)))

                                if best_score_seen >= 240:
                                    break

                    selected: Optional[Dict[str, Any]] = None
                    if candidate_by_app:
                        ranked_candidates = sorted(
                            candidate_by_app.values(),
                            key=lambda item: int(item.get("match_score", 0)),
                            reverse=True,
                        )
                        app_details_cache: Dict[int, Dict[str, Any]] = {}
                        for candidate in ranked_candidates[: int(CATALOG_COVER_VALIDATE_MAX_CANDIDATES or 10)]:
                            candidate_app_id = _safe_int(candidate.get("app_id"), 0)
                            if candidate_app_id <= 0:
                                continue

                            candidate_cc = str(candidate.get("cc", "us") or "us").strip() or "us"
                            details = app_details_cache.get(candidate_app_id)
                            if details is None:
                                details = await self._fetch_steam_app_details(
                                    session=session,
                                    app_id=candidate_app_id,
                                    cc=candidate_cc,
                                    headers=headers,
                                )
                                app_details_cache[candidate_app_id] = dict(details) if isinstance(details, dict) else {}

                            details_data = details if isinstance(details, dict) else {}
                            if _safe_int(details_data.get("_http_status"), 0) > 0:
                                details_data = {}

                            if details_data:
                                app_type = str(details_data.get("type", "") or "").strip().lower()
                                if app_type and app_type != "game":
                                    continue

                            resolved_name = str((details_data.get("name") if details_data else "") or "").strip()
                            if not resolved_name:
                                resolved_name = str(candidate.get("matched_title", "") or "").strip()
                            if not resolved_name:
                                continue

                            candidate_term = str(candidate.get("term", "") or "").strip()
                            if not candidate_term:
                                continue

                            if not self._is_cover_title_match_confident(term=candidate_term, name=resolved_name):
                                continue

                            selected = dict(candidate)
                            selected["matched_title"] = resolved_name
                            break

                    if selected is not None:
                        cover_url = str(selected.get("cover_url", "") or "").strip()
                        matched_title = str(selected.get("matched_title", "") or "").strip()
                        source = str(selected.get("source", "") or "").strip()
                        app_id = _safe_int(selected.get("app_id"), 0)
                        if app_id > 0:
                            square_cover_url = self._build_store_square_cover_url(app_id)
                            proton_summary = await self._fetch_protondb_summary(session=session, app_id=app_id)
                            protondb_tier = str(proton_summary.get("tier", "") or "").strip()
            except Exception as exc:
                config.logger.warning("解析游戏封面失败: title=%s error=%s", effective_title, exc)

        if steamgriddb_key and self._steamgriddb_available() and app_id > 0:
            try:
                sgdb_portrait = await resolve_steamgriddb_portrait_grid(api_key=steamgriddb_key, steam_app_id=app_id)
                http_status = _safe_int(sgdb_portrait.get("http_status"), 0)
                if http_status in {429, 500, 502, 503, 504}:
                    self._mark_steamgriddb_unavailable(http_status=http_status)
                if bool(sgdb_portrait.get("ok")):
                    portrait_url = str(sgdb_portrait.get("portrait") or "").strip()
                    if portrait_url:
                        square_cover_url = portrait_url
                        source = "steamgriddb"
            except Exception as exc:
                config.logger.warning("SteamGridDB portrait resolve failed app=%s title=%s: %s", app_id, effective_title, exc)

        if steamgriddb_key and self._steamgriddb_available() and not square_cover_url and terms:
            try:
                sgdb_fallback = await self._resolve_steamgriddb_portrait_by_terms(api_key=steamgriddb_key, terms=terms)
                http_status = _safe_int(sgdb_fallback.get("http_status"), 0)
                if http_status in {429, 500, 502, 503, 504}:
                    self._mark_steamgriddb_unavailable(http_status=http_status)
                if bool(sgdb_fallback.get("ok")):
                    portrait_url = str(sgdb_fallback.get("portrait") or "").strip()
                    if portrait_url:
                        candidate_name = str(sgdb_fallback.get("matched_title") or "").strip()
                        match_score = _safe_int(sgdb_fallback.get("match_score"), 0)
                        accepted = False
                        if is_switch_entry:
                            best_local_score = 0
                            if candidate_name:
                                try:
                                    best_local_score = max(
                                        self._score_cover_title_match(term=str(term or ""), name=candidate_name)
                                        for term in terms[:4]
                                    )
                                except Exception:
                                    best_local_score = 0
                            # Switch 资源优先“有封面”，因此放宽判定阈值，避免大量空白占位。
                            accepted = bool(best_local_score >= 85 or match_score >= 120)
                        else:
                            if candidate_name and match_score >= 200:
                                for term in terms[:4]:
                                    if self._is_cover_title_match_confident(term=str(term or ""), name=candidate_name):
                                        accepted = True
                                        break

                        if accepted:
                            square_cover_url = portrait_url
                            source = "steamgriddb"
                        if not matched_title:
                            matched_title = candidate_name
            except Exception as exc:
                config.logger.warning("SteamGridDB search fallback failed title=%s: %s", effective_title, exc)

        # Switch 资源：SteamGridDB 无法命中/被限流时，兜底尝试 Steam 商店搜索，
        # 以提升“有封面”的覆盖率（仅在严格匹配通过时采用，避免误匹配）。
        if is_switch_entry and terms and app_id <= 0 and not cover_url and not square_cover_url:
            try:
                store_fallback = await self._resolve_store_cover_by_terms_strict(terms=terms)
                if store_fallback:
                    app_id = max(0, _safe_int(store_fallback.get("app_id"), 0))
                    cover_url = str(store_fallback.get("cover_url", "") or "").strip()
                    matched_title = str(store_fallback.get("matched_title", "") or "").strip() or matched_title
                    source = str(store_fallback.get("source", "") or "").strip() or "steam_store_search"
                    if app_id > 0 and not square_cover_url:
                        square_cover_url = self._build_store_square_cover_url(app_id)
            except Exception as exc:
                config.logger.warning("Steam store cover fallback failed title=%s: %s", effective_title, exc)

        has_positive_payload = bool(cover_url or square_cover_url or app_id > 0 or protondb_tier)
        negative_ttl = int(CATALOG_COVER_NEGATIVE_TTL_SECONDS or 0)
        if not has_positive_payload and is_switch_entry:
            # Switch 封面几乎完全依赖 SteamGridDB：
            # 若因限流进入 cooldown，避免长时间缓存空结果导致“封面一片空白”。
            try:
                if not self._steamgriddb_available():
                    negative_ttl = min(negative_ttl or 0, 180) if int(negative_ttl or 0) > 0 else 180
            except Exception:
                pass
        expires_at = now_ts + (CATALOG_COVER_CACHE_TTL_SECONDS if has_positive_payload else negative_ttl)
        cache_value = {
            "cover_url": cover_url,
            "square_cover_url": square_cover_url,
            "source": source,
            "matched_title": matched_title,
            "app_id": int(app_id),
            "protondb_tier": protondb_tier,
            "expires_at": int(expires_at),
        }
        async with self._catalog_cover_lock:
            self._catalog_cover_cache[cache_key] = cache_value

        return {
            "cover_url": cover_url,
            "square_cover_url": square_cover_url,
            "source": source,
            "matched_title": matched_title,
            "app_id": int(app_id),
            "protondb_tier": protondb_tier,
            "cached": False,
        }

    def _build_catalog_cover_terms(self, *, title: str, categories: str = "") -> List[str]:
        """生成封面检索候选词，优先英文名（避免使用分类词导致误匹配）。"""
        raw_title = str(title or "").strip()
        if not raw_title:
            return []

        def sanitize(value: str) -> str:
            text = str(value or "").strip()
            text = text.replace("\u3000", " ")
            text = re.sub(r"\s+", " ", text)
            text = re.sub(r"(?i)\b(v|ver|version)\s*\d+(?:\.\d+){0,3}\b", "", text).strip()
            # Strip bundle-like ranges such as "1-7" to reduce mismatches.
            text = re.sub(r"\b\d+\s*[-–—]\s*\d+\b", "", text).strip()
            text = re.sub(r"\s+", " ", text).strip()
            return text

        def strip_editions(value: str) -> str:
            text = str(value or "").strip()
            if not text:
                return ""

            # Remove trailing hardware notes like "(只支持AMD)".
            text = re.sub(r"[（(][^）)]{0,24}只\s*支\s*持[^）)]*[）)]\s*$", "", text).strip()

            # Strip common Chinese edition suffixes (only at end).
            text = re.sub(
                r"(?:[:：\-\u2013\u2014\s]*(?:数字)?豪华版|[:：\-\u2013\u2014\s]*豪华版|[:：\-\u2013\u2014\s]*黄金版|[:：\-\u2013\u2014\s]*究极版|[:：\-\u2013\u2014\s]*终极版|[:：\-\u2013\u2014\s]*完整版|[:：\-\u2013\u2014\s]*完全版|[:：\-\u2013\u2014\s]*年度版|[:：\-\u2013\u2014\s]*决定版|[:：\-\u2013\u2014\s]*典藏版|[:：\-\u2013\u2014\s]*传奇版|[:：\-\u2013\u2014\s]*特别版)\s*$",
                "",
                text,
            ).strip()

            # Strip common English edition suffixes (only at end).
            text = re.sub(
                r"(?i)(?:[\-–—:：]\s*)?(digital\s+deluxe|deluxe|gold|ultimate|complete|definitive|collector'?s|standard|premium|legend|special|limited)\s+edition\s*$",
                "",
                text,
            ).strip()
            text = re.sub(r"(?i)(?:[\-–—:：]\s*)?(game\s+of\s+the\s+year|goty)\s+edition\s*$", "", text).strip()
            text = re.sub(r"[\-–—:：\s]+$", "", text).strip()
            return " ".join(text.split())

        parts: List[str] = []
        raw_parts = [part for part in re.split(r"[\/／|｜]+", raw_title) if str(part or "").strip()]
        for part in raw_parts:
            cleaned = sanitize(part)
            if not cleaned:
                continue
            stripped = strip_editions(cleaned)
            if stripped and stripped != cleaned:
                parts.append(stripped)
            parts.append(cleaned)
            for candidate in (stripped, cleaned):
                value = str(candidate or "").strip()
                if not value:
                    continue
                # Some repack titles append suffixes like "Main - Something Edition" which may not exist on Steam.
                # Add the main title as a fallback term (only when the suffix looks like an edition/bundle marker).
                dash_parts = re.split(r"\s+[\-–—]\s+", value, maxsplit=1)
                if len(dash_parts) != 2:
                    continue
                dash_main = str(dash_parts[0] or "").strip()
                dash_suffix = str(dash_parts[1] or "").strip()
                if not dash_main or not dash_suffix or dash_main == value or len(dash_main) < 4:
                    continue

                suffix_norm = self._normalize_cover_text(dash_suffix)
                suffix_tokens = set(suffix_norm.split()) if suffix_norm else set()
                suffix_noise = {
                    "edition",
                    "bundle",
                    "pack",
                    "upgrade",
                    "dlc",
                    "soundtrack",
                    "ost",
                    "season",
                    "pass",
                    "expansion",
                    "expansions",
                    "artbook",
                }
                if suffix_tokens & suffix_noise or re.search(r"(豪华版|黄金版|终极版|究极版|完整版|完全版|年度版|合集|捆绑|季票)", dash_suffix):
                    parts.append(dash_main)

        fallback = sanitize(raw_title)
        if fallback:
            parts.append(fallback)

        ascii_parts = [part for part in parts if re.search(r"[A-Za-z]", part)]
        ordered: List[str] = []
        for value in ascii_parts + parts:
            if not value:
                continue
            if value not in ordered:
                ordered.append(value)
        return ordered[:8]

    def _normalize_cover_text(self, value: str) -> str:
        text = str(value or "").lower()
        text = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", text)
        tokens = text.split()
        # Basic roman numeral normalization (avoid mapping single-letter "i" which is common in titles).
        roman_map = {
            "ii": "2",
            "iii": "3",
            "iv": "4",
            "vi": "6",
            "vii": "7",
            "viii": "8",
            "ix": "9",
            "xi": "11",
            "xii": "12",
            "xiii": "13",
            "xiv": "14",
            "xv": "15",
            "xvi": "16",
            "xvii": "17",
            "xviii": "18",
            "xix": "19",
            "xx": "20",
        }
        normalized = [roman_map.get(token, token) for token in tokens]
        return " ".join(normalized)

    def _evaluate_cover_title_match(self, *, terms: Sequence[str], name: str) -> Dict[str, Any]:
        """Evaluate whether a resolved title matches any expected search term."""
        candidate_name = str(name or "").strip()
        if not candidate_name:
            return {
                "confident": False,
                "best_score": -1,
                "best_term": "",
                "matched_title": "",
            }

        best_score = -1
        best_term = ""
        confident = False
        for raw_term in list(terms or [])[:4]:
            term = str(raw_term or "").strip()
            if not term:
                continue
            score = self._score_cover_title_match(term=term, name=candidate_name)
            if score > best_score:
                best_score = score
                best_term = term
            if self._is_cover_title_match_confident(term=term, name=candidate_name):
                confident = True

        return {
            "confident": bool(confident),
            "best_score": int(best_score),
            "best_term": best_term,
            "matched_title": candidate_name,
        }

    def _cover_match_key_tokens(self, tokens: Set[str]) -> Set[str]:
        """Pick "important" tokens for title matching to reduce false positives."""
        if not tokens:
            return set()

        stopwords = {
            "the",
            "a",
            "an",
            "of",
            "and",
            "or",
            "to",
            "for",
            "in",
            "on",
            "at",
            "with",
            "edition",
            "digital",
            "deluxe",
            "gold",
            "ultimate",
            "complete",
            "definitive",
            "collector",
            "collectors",
            "standard",
            "premium",
            "legend",
            "legendary",
            "special",
            "limited",
            "goty",
            "year",
            "remake",
            "remastered",
            "redux",
            "enhanced",
            "directors",
            "director",
            "cut",
        }

        key: Set[str] = set()
        for token in tokens:
            if not token:
                continue
            if token.isdigit():
                key.add(token)
            elif len(token) >= 3 and token not in stopwords:
                key.add(token)
        return key or set(tokens)

    def _steamgriddb_available(self) -> bool:
        """Whether SteamGridDB requests should be attempted right now."""
        now_ts = _now_wall_ts()
        try:
            until = int(self._steamgriddb_disabled_until or 0)
        except Exception:
            until = 0
        return now_ts >= until

    def _mark_steamgriddb_unavailable(self, *, http_status: int = 0) -> None:
        """Disable SteamGridDB requests for a short cooldown window."""
        now_ts = _now_wall_ts()
        prev_until = _safe_int(getattr(self, "_steamgriddb_disabled_until", 0), 0)
        until = now_ts + int(STEAMGRIDDB_DISABLE_SECONDS or 0)
        if until > prev_until:
            self._steamgriddb_disabled_until = until
            status_suffix = f" (HTTP {int(http_status)})" if int(http_status or 0) > 0 else ""
            config.logger.warning("SteamGridDB 暂不可用%s，%ss 内将跳过请求", status_suffix, int(STEAMGRIDDB_DISABLE_SECONDS or 0))

    def _score_cover_title_match(self, *, term: str, name: str) -> int:
        query_norm = self._normalize_cover_text(term)
        name_norm = self._normalize_cover_text(name)
        if not query_norm or not name_norm:
            return -1

        query_tokens = set(query_norm.split()) if query_norm else set()
        key_query_tokens = self._cover_match_key_tokens(query_tokens)
        name_tokens = set(name_norm.split()) if name_norm else set()
        key_query_has_digits = any(token.isdigit() for token in key_query_tokens)
        if key_query_tokens and name_tokens and key_query_has_digits:
            if any(token.isdigit() and token not in name_tokens for token in key_query_tokens):
                return -1

        score = 0
        if query_norm and name_norm:
            if query_norm == name_norm:
                score += 200
            elif name_norm.startswith(query_norm) or query_norm.startswith(name_norm):
                shorter = min(len(query_norm), len(name_norm))
                longer = max(len(query_norm), len(name_norm))
                ratio = shorter / max(1, longer)
                if ratio >= 0.9:
                    score += 120
                elif ratio >= 0.75:
                    score += 90
                else:
                    score += 60
            elif query_norm in name_norm:
                score += 90
            elif name_norm in query_norm:
                score += 45

        match_query_tokens = key_query_tokens or query_tokens
        if match_query_tokens and name_tokens:
            overlap = len(match_query_tokens & name_tokens)
            missing = len(match_query_tokens - name_tokens)
            extra = len(name_tokens - match_query_tokens)
            score += overlap * 14
            score -= missing * 6
            score -= max(0, extra - 2) * 2
            if missing == 0 and overlap >= 2:
                score += 30
        return int(score)

    def _is_cover_title_match_confident(self, *, term: str, name: str) -> bool:
        score = self._score_cover_title_match(term=term, name=name)
        if score < int(CATALOG_COVER_STRICT_MIN_SCORE or 0):
            return False

        query_norm = self._normalize_cover_text(term)
        name_norm = self._normalize_cover_text(name)
        query_tokens = set(query_norm.split()) if query_norm else set()
        name_tokens = set(name_norm.split()) if name_norm else set()
        key_query_tokens = self._cover_match_key_tokens(query_tokens)

        # For very short queries, avoid guessing (e.g. "Star Trek" -> "Star Trek: Voyager ...").
        if len(key_query_tokens) <= 2 and score < 200:
            return False

        if len(key_query_tokens) >= 3 and key_query_tokens and name_tokens:
            overlap = len(key_query_tokens & name_tokens)
            coverage = overlap / max(1, len(key_query_tokens))
            if coverage < float(CATALOG_COVER_STRICT_KEY_COVERAGE or 0.0):
                return False
        return True

    def _rank_catalog_cover_candidates(self, *, term: str, items: Any, limit: int = 3) -> List[Dict[str, Any]]:
        """Rank Steam storesearch results by title match score (descending)."""
        if not isinstance(items, list) or not items:
            return []

        query_norm = self._normalize_cover_text(term)
        query_tokens = set(query_norm.split()) if query_norm else set()
        key_query_tokens = self._cover_match_key_tokens(query_tokens)
        key_query_has_digits = any(token.isdigit() for token in key_query_tokens)

        ranked: List[Dict[str, Any]] = []
        for item in items[:CATALOG_COVER_SEARCH_LIMIT]:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type", "") or "").strip().lower()
            if item_type and item_type not in {"game", "app"}:
                continue
            cover_url = self._extract_store_cover_url(item)
            app_id = self._extract_store_app_id(item)
            if not cover_url and app_id <= 0:
                continue
            name = str(item.get("name", "") or "").strip()
            if not name:
                continue

            name_norm = self._normalize_cover_text(name)
            name_tokens = set(name_norm.split()) if name_norm else set()

            if key_query_tokens and name_tokens:
                # Avoid obvious mismatches like "Portal" matching "Portal 2".
                if key_query_has_digits and any(token.isdigit() and token not in name_tokens for token in key_query_tokens):
                    continue

                if len(key_query_tokens) >= 3:
                    overlap_key = len(key_query_tokens & name_tokens)
                    missing_key = len(key_query_tokens - name_tokens)
                    if missing_key > 0:
                        coverage = overlap_key / max(1, len(key_query_tokens))
                        if coverage < 0.75:
                            continue

            score = self._score_cover_title_match(term=term, name=name)
            if score < int(CATALOG_COVER_MIN_SCORE or 0):
                continue

            ranked.append(
                {
                    "cover_url": cover_url or self._build_store_cover_url_from_app_id(app_id),
                    "matched_title": name,
                    "source": "steam_store_search",
                    "app_id": int(app_id),
                    "match_score": int(score),
                }
            )

        ranked.sort(key=lambda item: int(item.get("match_score", 0)), reverse=True)
        out: List[Dict[str, Any]] = []
        seen: Set[int] = set()
        for item in ranked:
            app_id = _safe_int(item.get("app_id"), 0)
            if app_id in seen:
                continue
            seen.add(app_id)
            out.append(item)
            if len(out) >= max(1, int(limit or 1)):
                break
        return out

    def _build_store_cover_url_from_app_id(self, app_id: int) -> str:
        app = _safe_int(app_id, 0)
        if app <= 0:
            return ""
        return f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app}/capsule_616x353.jpg"

    async def _fetch_steam_app_details(
        self,
        *,
        session: aiohttp.ClientSession,
        app_id: int,
        cc: str,
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        """Fetch Steam store appdetails for validation (type/name)."""
        app = _safe_int(app_id, 0)
        if app <= 0:
            return {}

        locale_cc = str(cc or "us").strip() or "us"
        url = str(
            URL("https://store.steampowered.com/api/appdetails").with_query(
                {"appids": str(app), "l": "english", "cc": locale_cc}
            )
        )
        try:
            timeout = aiohttp.ClientTimeout(total=float(STEAM_APPDETAILS_HTTP_TIMEOUT_SECONDS or 4.0))
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if int(resp.status) != 200:
                    return {"_http_status": int(resp.status)}
                payload = await resp.json(content_type=None)
        except Exception:
            return {}

        if not isinstance(payload, dict):
            return {}
        entry = payload.get(str(app)) or payload.get(app)
        if not isinstance(entry, dict) or not bool(entry.get("success")):
            return {}
        data = entry.get("data")
        return data if isinstance(data, dict) else {}

    async def _validate_store_app_id_match(self, *, app_id: int, terms: Sequence[str]) -> Dict[str, Any]:
        """Validate whether a Steam app id matches the expected game title."""
        app = _safe_int(app_id, 0)
        query_terms = [str(term or "").strip() for term in list(terms or [])[:4] if str(term or "").strip()]
        if app <= 0 or not query_terms:
            return {
                "checked": False,
                "valid": False,
                "app_id": int(app),
                "name": "",
                "best_score": -1,
                "best_term": "",
            }

        try:
            ssl_context, _ = self._build_qr_ssl_context()
        except Exception:
            ssl_context = None

        timeout = aiohttp.ClientTimeout(total=float(STEAM_APPDETAILS_HTTP_TIMEOUT_SECONDS or 4.0))
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
            "Referer": "https://store.steampowered.com/",
        }

        try:
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                details = await self._fetch_steam_app_details(
                    session=session,
                    app_id=app,
                    cc="us",
                    headers=headers,
                )
        except Exception:
            details = {}

        if not isinstance(details, dict) or not details:
            return {
                "checked": False,
                "valid": False,
                "app_id": int(app),
                "name": "",
                "best_score": -1,
                "best_term": "",
            }

        app_type = str(details.get("type", "") or "").strip().lower()
        app_name = str(details.get("name", "") or "").strip()
        if app_type and app_type != "game":
            return {
                "checked": True,
                "valid": False,
                "app_id": int(app),
                "name": app_name,
                "best_score": -1,
                "best_term": "",
                "reason": f"unexpected_type:{app_type}",
            }
        if not app_name:
            return {
                "checked": False,
                "valid": False,
                "app_id": int(app),
                "name": "",
                "best_score": -1,
                "best_term": "",
            }

        match = self._evaluate_cover_title_match(terms=query_terms, name=app_name)
        return {
            "checked": True,
            "valid": bool(match.get("confident")),
            "app_id": int(app),
            "name": app_name,
            "best_score": int(match.get("best_score", -1)),
            "best_term": str(match.get("best_term", "") or ""),
            "matched_title": str(match.get("matched_title", "") or ""),
        }

    async def _resolve_store_cover_by_terms_strict(self, *, terms: Sequence[str]) -> Optional[Dict[str, Any]]:
        """Resolve a Steam store cover by search terms with strict title validation.

        Intended as a fallback when Switch resources fail to resolve a SteamGridDB cover.
        """
        query_terms = [str(term or "").strip() for term in (terms or []) if str(term or "").strip()]
        if not query_terms:
            return None

        try:
            ssl_context, _ = self._build_qr_ssl_context()
        except Exception:
            ssl_context = None

        timeout = aiohttp.ClientTimeout(total=float(CATALOG_COVER_HTTP_TIMEOUT_SECONDS or 6.0))
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
            "Referer": "https://store.steampowered.com/",
        }

        app_details_cache: Dict[int, Dict[str, Any]] = {}

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for term in query_terms[:3]:
                for cc in ("cn", "us"):
                    query = str(term or "").strip()
                    if not query:
                        continue
                    search_url = str(
                        URL("https://store.steampowered.com/api/storesearch/").with_query(
                            {"term": query, "l": "schinese", "cc": str(cc or "us")}
                        )
                    )
                    try:
                        async with session.get(search_url, headers=headers) as resp:
                            if int(resp.status) != 200:
                                continue
                            payload = await resp.json(content_type=None)
                    except Exception:
                        continue

                    items = payload.get("items") if isinstance(payload, dict) else []
                    ranked = self._rank_catalog_cover_candidates(
                        term=query,
                        items=items,
                        limit=int(CATALOG_COVER_VALIDATE_MAX_CANDIDATES or 10),
                    )
                    for candidate in ranked:
                        app_id = _safe_int(candidate.get("app_id"), 0)
                        if app_id <= 0:
                            continue

                        details = app_details_cache.get(app_id)
                        if details is None:
                            details = await self._fetch_steam_app_details(
                                session=session,
                                app_id=app_id,
                                cc=str(cc or "us"),
                                headers=headers,
                            )
                            app_details_cache[app_id] = dict(details) if isinstance(details, dict) else {}

                        details_data = details if isinstance(details, dict) else {}
                        if _safe_int(details_data.get("_http_status"), 0) > 0:
                            details_data = {}

                        if details_data:
                            app_type = str(details_data.get("type", "") or "").strip().lower()
                            if app_type and app_type != "game":
                                continue

                        resolved_name = str((details_data.get("name") if details_data else "") or "").strip()
                        if not resolved_name:
                            resolved_name = str(candidate.get("matched_title", "") or "").strip()
                        if not resolved_name:
                            continue

                        if not self._is_cover_title_match_confident(term=query, name=resolved_name):
                            continue

                        cover_url = str(candidate.get("cover_url", "") or "").strip()
                        if not cover_url:
                            cover_url = self._build_store_cover_url_from_app_id(app_id)
                        return {
                            "cover_url": cover_url,
                            "matched_title": resolved_name,
                            "source": "steam_store_search",
                            "app_id": int(app_id),
                            "match_score": int(_safe_int(candidate.get("match_score"), 0)),
                        }

        return None

    def _pick_catalog_cover_candidate(self, *, term: str, items: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(items, list) or not items:
            return None

        query_norm = self._normalize_cover_text(term)
        query_tokens = set(query_norm.split()) if query_norm else set()
        key_query_tokens = self._cover_match_key_tokens(query_tokens)
        key_query_has_digits = any(token.isdigit() for token in key_query_tokens)
        best_score = -1
        best: Optional[Dict[str, str]] = None

        for item in items[:CATALOG_COVER_SEARCH_LIMIT]:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type", "") or "").strip().lower()
            if item_type and item_type not in {"game", "app"}:
                continue
            cover_url = self._extract_store_cover_url(item)
            app_id = self._extract_store_app_id(item)
            if not cover_url:
                continue
            name = str(item.get("name", "") or "").strip()
            name_norm = self._normalize_cover_text(name)
            name_tokens = set(name_norm.split()) if name_norm else set()

            if key_query_tokens and name_tokens:
                # Avoid obvious mismatches like "Portal" matching "Portal 2".
                if key_query_has_digits and any(token.isdigit() and token not in name_tokens for token in key_query_tokens):
                    continue

                if len(key_query_tokens) >= 3:
                    overlap_key = len(key_query_tokens & name_tokens)
                    missing_key = len(key_query_tokens - name_tokens)
                    if missing_key > 0:
                        coverage = overlap_key / max(1, len(key_query_tokens))
                        if coverage < 0.75:
                            continue

            score = 0
            if query_norm and name_norm:
                if query_norm == name_norm:
                    score += 200
                elif name_norm.startswith(query_norm) or query_norm.startswith(name_norm):
                    shorter = min(len(query_norm), len(name_norm))
                    longer = max(len(query_norm), len(name_norm))
                    ratio = shorter / max(1, longer)
                    if ratio >= 0.9:
                        score += 120
                    elif ratio >= 0.75:
                        score += 90
                    else:
                        score += 60
                elif query_norm in name_norm:
                    score += 90
                elif name_norm in query_norm:
                    score += 45
            match_query_tokens = key_query_tokens or query_tokens
            if match_query_tokens and name_tokens:
                overlap = len(match_query_tokens & name_tokens)
                missing = len(match_query_tokens - name_tokens)
                extra = len(name_tokens - match_query_tokens)
                score += overlap * 14
                score -= missing * 6
                score -= max(0, extra - 2) * 2
                if missing == 0 and overlap >= 2:
                    score += 30

            if score > best_score:
                best_score = score
                best = {
                    "cover_url": cover_url,
                    "matched_title": name,
                    "source": "steam_store_search",
                    "app_id": int(app_id),
                    "match_score": int(best_score),
                }

        if not best:
            return None
        if best_score < CATALOG_COVER_MIN_SCORE:
            return None
        return best

    def _pick_steamgriddb_game_candidate(self, *, term: str, games: Any) -> Optional[Dict[str, Any]]:
        """Pick the best SteamGridDB autocomplete match for the given term."""
        if not isinstance(games, list) or not games:
            return None

        query_norm = self._normalize_cover_text(term)
        query_tokens = set(query_norm.split()) if query_norm else set()
        key_query_tokens = self._cover_match_key_tokens(query_tokens)
        key_query_has_digits = any(token.isdigit() for token in key_query_tokens)

        best_score = -1
        best: Optional[Dict[str, Any]] = None

        for item in games[:12]:
            if not isinstance(item, dict):
                continue
            gid = _safe_int(item.get("id"), 0)
            name = str(item.get("name", "") or "").strip()
            if gid <= 0 or not name:
                continue

            name_norm = self._normalize_cover_text(name)
            name_tokens = set(name_norm.split()) if name_norm else set()
            if not name_tokens:
                continue

            if key_query_tokens:
                if key_query_has_digits and any(token.isdigit() and token not in name_tokens for token in key_query_tokens):
                    continue
                if len(key_query_tokens) >= 3:
                    overlap_key = len(key_query_tokens & name_tokens)
                    missing_key = len(key_query_tokens - name_tokens)
                    if missing_key > 0:
                        coverage = overlap_key / max(1, len(key_query_tokens))
                        if coverage < 0.75:
                            continue

            score = 0
            if query_norm and name_norm:
                if query_norm == name_norm:
                    score += 200
                elif name_norm.startswith(query_norm) or query_norm.startswith(name_norm):
                    shorter = min(len(query_norm), len(name_norm))
                    longer = max(len(query_norm), len(name_norm))
                    ratio = shorter / max(1, longer)
                    if ratio >= 0.9:
                        score += 120
                    elif ratio >= 0.75:
                        score += 90
                    else:
                        score += 60
                elif query_norm in name_norm:
                    score += 90
                elif name_norm in query_norm:
                    score += 45

            match_query_tokens = key_query_tokens or query_tokens
            if match_query_tokens:
                overlap = len(match_query_tokens & name_tokens)
                missing = len(match_query_tokens - name_tokens)
                extra = len(name_tokens - match_query_tokens)
                score += overlap * 14
                score -= missing * 6
                score -= max(0, extra - 2) * 2
                if missing == 0 and overlap >= 2:
                    score += 30

            if bool(item.get("verified")):
                score += 5

            if score > best_score:
                best_score = score
                best = {
                    "game_id": int(gid),
                    "name": name,
                    "match_score": int(score),
                }

        if not best:
            return None
        if best_score < 35:
            return None
        return best

    async def _resolve_steamgriddb_portrait_by_terms(self, *, api_key: str, terms: Sequence[str]) -> Dict[str, Any]:
        """Try resolving a portrait cover from SteamGridDB by searching terms."""
        key = str(api_key or "").strip()
        if not key:
            return {
                "ok": False,
                "message": "SteamGridDB API key 缺失",
                "portrait": "",
                "matched_title": "",
                "game_id": 0,
                "http_status": 0,
            }

        best: Optional[Dict[str, Any]] = None
        best_http_status = 0
        for term in list(terms)[:3]:
            keyword = str(term or "").strip()
            if not keyword:
                continue
            try:
                result = await search_steamgriddb_autocomplete(api_key=key, term=keyword)
            except Exception:
                continue
            if not bool(result.get("ok")):
                best_http_status = max(best_http_status, _safe_int(result.get("http_status"), 0))
                continue
            candidate = self._pick_steamgriddb_game_candidate(term=keyword, games=result.get("games"))
            if candidate is None:
                continue
            if best is None or int(candidate.get("match_score", 0)) > int(best.get("match_score", 0)):
                best = candidate
                if int(candidate.get("match_score", 0)) >= 240:
                    break

        if best is None:
            return {
                "ok": False,
                "message": "SteamGridDB 未找到匹配条目",
                "portrait": "",
                "matched_title": "",
                "game_id": 0,
                "http_status": int(best_http_status or 0),
            }

        gid = _safe_int(best.get("game_id"), 0)
        if gid <= 0:
            return {
                "ok": False,
                "message": "SteamGridDB game_id 无效",
                "portrait": "",
                "matched_title": "",
                "game_id": 0,
                "http_status": int(best_http_status or 0),
            }

        resolved = await resolve_steamgriddb_portrait_grid_by_game_id(api_key=key, game_id=gid)
        http_status = max(best_http_status, _safe_int(resolved.get("http_status"), 0))
        portrait = str(resolved.get("portrait", "") or "").strip()
        if not bool(resolved.get("ok")) or not portrait:
            return {
                "ok": False,
                "message": str(resolved.get("message", "") or "未从 SteamGridDB 获取到竖版封面"),
                "portrait": "",
                "matched_title": "",
                "game_id": int(gid),
                "http_status": int(http_status or 0),
            }

        return {
            "ok": True,
            "message": "",
            "portrait": portrait,
            "matched_title": str(best.get("name", "") or "").strip(),
            "game_id": int(gid),
            "match_score": int(best.get("match_score", 0)),
            "http_status": int(http_status or 0),
        }

    async def _resolve_steamgriddb_artwork_by_terms(self, *, api_key: str, terms: Sequence[str]) -> Dict[str, Any]:
        """Try resolving Steam grid/hero/logo/icon from SteamGridDB by searching terms."""
        key = str(api_key or "").strip()
        if not key:
            return {
                "ok": False,
                "message": "SteamGridDB API key 缺失",
                "landscape": "",
                "portrait": "",
                "hero": "",
                "logo": "",
                "icon": "",
                "matched_title": "",
                "game_id": 0,
                "http_status": 0,
            }

        best: Optional[Dict[str, Any]] = None
        best_http_status = 0
        for term in list(terms)[:3]:
            keyword = str(term or "").strip()
            if not keyword:
                continue
            try:
                result = await search_steamgriddb_autocomplete(api_key=key, term=keyword)
            except Exception:
                continue
            if not bool(result.get("ok")):
                best_http_status = max(best_http_status, _safe_int(result.get("http_status"), 0))
                continue
            candidate = self._pick_steamgriddb_game_candidate(term=keyword, games=result.get("games"))
            if candidate is None:
                continue
            if best is None or int(candidate.get("match_score", 0)) > int(best.get("match_score", 0)):
                best = candidate
                if int(candidate.get("match_score", 0)) >= 240:
                    break

        if best is None:
            return {
                "ok": False,
                "message": "SteamGridDB 未找到匹配条目",
                "landscape": "",
                "portrait": "",
                "hero": "",
                "logo": "",
                "icon": "",
                "matched_title": "",
                "game_id": 0,
                "http_status": int(best_http_status or 0),
            }

        gid = _safe_int(best.get("game_id"), 0)
        if gid <= 0:
            return {
                "ok": False,
                "message": "SteamGridDB game_id 无效",
                "landscape": "",
                "portrait": "",
                "hero": "",
                "logo": "",
                "icon": "",
                "matched_title": "",
                "game_id": 0,
                "http_status": int(best_http_status or 0),
            }

        resolved = await resolve_steamgriddb_artwork_by_game_id(api_key=key, game_id=gid)
        http_status = max(best_http_status, _safe_int(resolved.get("http_status"), 0))
        if not bool(resolved.get("ok")):
            return {
                "ok": False,
                "message": str(resolved.get("message", "") or "未从 SteamGridDB 获取到素材"),
                "landscape": "",
                "portrait": "",
                "hero": "",
                "logo": "",
                "icon": "",
                "matched_title": "",
                "game_id": int(gid),
                "http_status": int(http_status or 0),
            }

        return {
            "ok": True,
            "message": "",
            "landscape": str(resolved.get("landscape") or "").strip(),
            "portrait": str(resolved.get("portrait") or "").strip(),
            "hero": str(resolved.get("hero") or "").strip(),
            "logo": str(resolved.get("logo") or "").strip(),
            "icon": str(resolved.get("icon") or "").strip(),
            "matched_title": str(best.get("name", "") or "").strip(),
            "game_id": int(gid),
            "match_score": int(best.get("match_score", 0)),
            "http_status": int(http_status or 0),
        }

    def _extract_store_app_id(self, item: Dict[str, Any]) -> int:
        app_id = _safe_int(item.get("id"), 0)
        if app_id <= 0:
            app_id = _safe_int(item.get("appid"), 0)
        return app_id

    def _extract_store_cover_url(self, item: Dict[str, Any]) -> str:
        for key in (
            "large_capsule_image",
            "header_image",
            "capsule_image",
            "small_capsule_image",
            "tiny_image",
        ):
            value = str(item.get(key, "") or "").strip()
            if value.startswith("http://") or value.startswith("https://"):
                return value

        app_id = self._extract_store_app_id(item)
        if app_id > 0:
            return f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app_id}/capsule_616x353.jpg"
        return ""

    def _build_store_square_cover_url(self, app_id: int) -> str:
        """优先返回更适合方形裁切的 Steam 竖版素材 URL。"""
        if _safe_int(app_id, 0) <= 0:
            return ""
        app = _safe_int(app_id, 0)
        return f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app}/library_600x900_2x.jpg"

    async def _fetch_protondb_summary(self, *, session: aiohttp.ClientSession, app_id: int) -> Dict[str, Any]:
        """读取 ProtonDB 摘要信息。"""
        if app_id <= 0:
            return {}
        api_url = f"https://www.protondb.com/api/v1/reports/summaries/{int(app_id)}.json"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
            "Referer": "https://www.protondb.com/",
        }
        try:
            timeout = aiohttp.ClientTimeout(total=PROTONDB_HTTP_TIMEOUT_SECONDS)
            async with session.get(api_url, headers=headers, timeout=timeout) as resp:
                if int(resp.status) != 200:
                    return {}
                payload = await resp.json(content_type=None)
                if not isinstance(payload, dict):
                    return {}
                tier = str(payload.get("tier", "") or "").strip()
                return {"tier": tier}
        except Exception:
            return {}

    def _build_hltb_search_payload(self, term: str) -> Dict[str, Any]:
        """构建 HLTB 搜索请求体。"""
        raw_text = str(term or "").strip()
        normalized = self._normalize_cover_text(raw_text)
        words = [item for item in re.split(r"\s+", normalized or raw_text) if item]
        return {
            "searchType": "games",
            "searchTerms": words[:8],
            "searchPage": 1,
            "size": 20,
            "searchOptions": {
                "games": {
                    "userId": 0,
                    "platform": "",
                    "sortCategory": "popular",
                    "rangeCategory": "main",
                    "rangeTime": {"min": 0, "max": 0},
                    "gameplay": {
                        "perspective": "",
                        "flow": "",
                        "genre": "",
                        "difficulty": "",
                    },
                    "rangeYear": {"min": "", "max": ""},
                    "modifier": "hide_dlc",
                },
                "users": {"sortCategory": "postcount"},
                "lists": {"sortCategory": "follows"},
                "filter": "",
                "sort": 0,
                "randomizer": 0,
            },
            "useCache": True,
        }

    async def _fetch_hltb_token(self, *, session: aiohttp.ClientSession, headers: Dict[str, str]) -> str:
        """获取 HLTB finder API 的短期鉴权 token。"""
        init_url = f"{HLTB_TOKEN_URL}?t={int(time.time() * 1000)}"
        try:
            async with session.get(init_url, headers=headers) as resp:
                if int(resp.status) != 200:
                    return ""
                data = await resp.json(content_type=None)
        except Exception:
            return ""
        if not isinstance(data, dict):
            return ""
        token = str(data.get("token", "") or "").strip()
        return token

    async def _get_cached_hltb_token(self, *, session: aiohttp.ClientSession, headers: Dict[str, str]) -> str:
        """获取缓存的 HLTB token，必要时自动刷新。"""
        now_ts = _now_wall_ts()
        async with self._hltb_token_lock:
            cached = str(self._hltb_token or "").strip()
            expires_at = _safe_int(self._hltb_token_expires_at, 0)
            if cached and expires_at > now_ts:
                return cached

        fetched = await self._fetch_hltb_token(session=session, headers=headers)
        if fetched:
            async with self._hltb_token_lock:
                self._hltb_token = fetched
                self._hltb_token_expires_at = now_ts + HLTB_TOKEN_CACHE_SECONDS
            return fetched

        async with self._hltb_token_lock:
            return str(self._hltb_token or "").strip()

    async def _resolve_hltb_steam_hint(
        self,
        *,
        cache_key: str,
        title: str,
        categories: str,
    ) -> Dict[str, Any]:
        """在 HLTB 无法直接命中时，从 Steam 商店检索英文标题作为补充候选。

        HLTB finder API 对中文搜索词命中率极低；Steam storesearch（cc=cn）可用中文检索并返回英文标题。
        """
        now_ts = _now_wall_ts()
        async with self._hltb_hint_lock:
            cached = self._hltb_hint_cache.get(cache_key)
            if isinstance(cached, dict) and _safe_int(cached.get("expires_at"), 0) > now_ts:
                return dict(cached)

        hint_title = ""
        hint_app_id = 0
        source_term = ""
        terms = self._build_catalog_cover_terms(title=title, categories=categories)
        if not terms:
            terms = [str(title or "").strip()]
        terms = terms[:2]

        try:
            ssl_context, _ = self._build_qr_ssl_context()
            timeout = aiohttp.ClientTimeout(total=min(4.0, float(CATALOG_COVER_HTTP_TIMEOUT_SECONDS or 4.0)))
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                for term in terms:
                    query = str(term or "").strip()
                    if not query:
                        continue
                    search_url = str(
                        URL("https://store.steampowered.com/api/storesearch/").with_query(
                            {"term": query, "l": "english", "cc": "cn"}
                        )
                    )
                    headers = {
                        "Accept": "application/json, text/plain, */*",
                        "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
                        "Referer": "https://store.steampowered.com/",
                    }
                    try:
                        async with session.get(search_url, headers=headers) as resp:
                            if int(resp.status) != 200:
                                continue
                            payload = await resp.json(content_type=None)
                    except Exception:
                        continue

                    items = payload.get("items") if isinstance(payload, dict) else []
                    resolved = self._pick_catalog_cover_candidate(term=query, items=items)
                    if not resolved:
                        continue
                    hint_title = str(resolved.get("matched_title", "") or "").strip()
                    hint_app_id = max(0, _safe_int(resolved.get("app_id"), 0))
                    source_term = query
                    if hint_title:
                        break
        except Exception:
            hint_title = ""
            hint_app_id = 0
            source_term = ""

        ok = bool(hint_title)
        # 命中时长缓存期更长；未命中也做短期缓存，避免频繁打 Steam storesearch。
        hint_expires_at = now_ts + (30 * 24 * 3600 if ok else 24 * 3600)
        hint_payload = {
            "ok": ok,
            "title": hint_title,
            "app_id": int(hint_app_id),
            "source_term": source_term,
            "expires_at": int(hint_expires_at),
        }
        async with self._hltb_hint_lock:
            self._hltb_hint_cache[cache_key] = dict(hint_payload)
        return hint_payload

    async def _post_hltb_search(
        self,
        *,
        session: aiohttp.ClientSession,
        payload: Dict[str, Any],
        headers: Dict[str, str],
        token: str,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """请求 HLTB finder 搜索接口。"""
        req_headers = dict(headers)
        if token:
            req_headers["x-auth-token"] = token
        try:
            async with session.post(HLTB_SEARCH_URL, headers=req_headers, json=payload) as resp:
                status = int(resp.status)
                if status != 200:
                    return status, []
                data = await resp.json(content_type=None)
        except Exception:
            return 0, []
        rows = data.get("data") if isinstance(data, dict) else []
        if isinstance(rows, list):
            return 200, [item for item in rows if isinstance(item, dict)]
        return 200, []

    async def _post_hltb_search_legacy(
        self,
        *,
        session: aiohttp.ClientSession,
        payload: Dict[str, Any],
        headers: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """兼容旧版 HLTB /api/search 接口。"""
        try:
            async with session.post(HLTB_LEGACY_SEARCH_URL, headers=headers, json=payload) as resp:
                if int(resp.status) != 200:
                    return []
                data = await resp.json(content_type=None)
        except Exception:
            return []
        rows = data.get("data") if isinstance(data, dict) else []
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
        return []

    def _pick_hltb_candidate(
        self,
        *,
        title: str,
        term: str,
        app_id: int,
        rows: Any,
    ) -> Optional[Dict[str, Any]]:
        """从 HLTB 搜索结果中选取最佳候选。"""
        if not isinstance(rows, list) or not rows:
            return None

        query_norm = self._normalize_cover_text(title or term)
        query_tokens = set(query_norm.split()) if query_norm else set()
        best_score = -1
        best_item: Optional[Dict[str, Any]] = None

        for item in rows[:HLTB_SEARCH_LIMIT]:
            if not isinstance(item, dict):
                continue
            game_name = str(item.get("game_name", "") or "").strip()
            if not game_name:
                continue

            candidate_app_id = _safe_int(item.get("profile_steam"), 0)
            name_norm = self._normalize_cover_text(game_name)
            name_tokens = set(name_norm.split()) if name_norm else set()

            score = 1
            if app_id > 0 and candidate_app_id == app_id:
                score += 260
            if query_norm and name_norm:
                if query_norm == name_norm:
                    score += 120
                elif query_norm in name_norm or name_norm in query_norm:
                    score += 80
            if query_tokens and name_tokens:
                score += len(query_tokens & name_tokens) * 14
            score += min(20, max(0, _safe_int(item.get("comp_all_count"), 0)) // 500)

            if score > best_score:
                best_score = score
                best_item = dict(item)

        return best_item

    async def resolve_hltb_stats(
        self,
        *,
        game_id: str = "",
        title: str = "",
        categories: str = "",
        app_id: int = 0,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """查询并缓存 HLTB 时长数据。"""
        cache_key = str(game_id or title or "").strip().lower()
        now_ts = _now_wall_ts()
        if not cache_key or not str(title or "").strip():
            return {
                "main_story_hours": 0.0,
                "main_story_text": "-",
                "total_hours": 0.0,
                "total_time_text": "-",
                "hltb_game_id": 0,
                "matched_title": "",
                "source_term": "",
                "cached": False,
            }

        if not force_refresh:
            async with self._hltb_lock:
                cached = self._hltb_cache.get(cache_key)
                if isinstance(cached, dict) and _safe_int(cached.get("expires_at"), 0) > now_ts:
                    main_hours = float(cached.get("main_story_hours", 0.0) or 0.0)
                    total_hours = float(cached.get("total_hours", 0.0) or 0.0)
                    return {
                        "main_story_hours": main_hours,
                        "main_story_text": _format_hours_value(main_hours),
                        "total_hours": total_hours,
                        "total_time_text": _format_hours_value(total_hours),
                        "hltb_game_id": _safe_int(cached.get("hltb_game_id"), 0),
                        "matched_title": str(cached.get("matched_title", "") or ""),
                        "source_term": str(cached.get("source_term", "") or ""),
                        "cached": True,
                    }

        main_hours = 0.0
        total_hours = 0.0
        hltb_game_id = 0
        matched_title = ""
        source_term = ""
        terms = self._build_catalog_cover_terms(title=title, categories=categories)
        if not terms:
            terms = [str(title or "").strip()]

        if terms:
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Origin": "https://howlongtobeat.com",
                "Referer": "https://howlongtobeat.com/",
                "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
            }
            try:
                ssl_context, _ = self._build_qr_ssl_context()
                timeout = aiohttp.ClientTimeout(total=HLTB_HTTP_TIMEOUT_SECONDS)
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    token = await self._get_cached_hltb_token(session=session, headers=headers)

                    async def try_terms(search_title: str, term_list: Sequence[str], app_id_hint: int) -> bool:
                        nonlocal main_hours, total_hours, hltb_game_id, matched_title, source_term, token
                        for term in term_list:
                            payload = self._build_hltb_search_payload(term)
                            status, rows = await self._post_hltb_search(
                                session=session,
                                payload=payload,
                                headers=headers,
                                token=token,
                            )
                            if status in {401, 403}:
                                token = await self._fetch_hltb_token(session=session, headers=headers)
                                if token:
                                    async with self._hltb_token_lock:
                                        self._hltb_token = token
                                        self._hltb_token_expires_at = _now_wall_ts() + HLTB_TOKEN_CACHE_SECONDS
                                    status, rows = await self._post_hltb_search(
                                        session=session,
                                        payload=payload,
                                        headers=headers,
                                        token=token,
                                    )

                            candidate = self._pick_hltb_candidate(
                                title=search_title,
                                term=term,
                                app_id=app_id_hint,
                                rows=rows,
                            )
                            if not candidate:
                                continue

                            comp_main = max(0, _safe_int(candidate.get("comp_main"), 0))
                            comp_all = max(0, _safe_int(candidate.get("comp_all"), 0))
                            comp_100 = max(0, _safe_int(candidate.get("comp_100"), 0))
                            comp_plus = max(0, _safe_int(candidate.get("comp_plus"), 0))
                            total_seconds = comp_all or comp_100 or comp_plus or comp_main
                            main_hours = round((comp_main / 3600.0), 1) if comp_main > 0 else 0.0
                            total_hours = round((total_seconds / 3600.0), 1) if total_seconds > 0 else 0.0
                            hltb_game_id = max(0, _safe_int(candidate.get("game_id"), 0))
                            matched_title = str(candidate.get("game_name", "") or "").strip()
                            source_term = str(term or "").strip()
                            return True
                        return False

                    ok = False
                    # HLTB finder API 对非英文检索（尤其中文）命中率极低，先跳过无意义的请求以降低超时概率。
                    if any(re.search(r"[A-Za-z]", str(term or "")) for term in terms):
                        ok = await try_terms(title, terms, int(app_id or 0))
                    if not ok:
                        # 如果我们已有 Steam 商店 AppID（来自 CSV 的 steam_appid），优先用 appdetails 拿到英文名，
                        # 再喂给 HLTB 搜索，比 storesearch(中文 term) 更稳且避免误匹配。
                        app_id_hint = max(0, _safe_int(app_id, 0))
                        if 0 < app_id_hint < 0x80000000:
                            try:
                                steam_headers = {
                                    "Accept": "application/json, text/plain, */*",
                                    "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
                                    "Referer": "https://store.steampowered.com/",
                                }
                                details = await self._fetch_steam_app_details(
                                    session=session,
                                    app_id=app_id_hint,
                                    cc="cn",
                                    headers=steam_headers,
                                )
                                steam_type = str(details.get("type", "") or "").strip().lower()
                                steam_name = str(details.get("name", "") or "").strip()
                                if steam_name and steam_type not in {"dlc", "music", "video"}:
                                    steam_terms = self._build_catalog_cover_terms(title=steam_name, categories="") or [
                                        steam_name
                                    ]
                                    ok = await try_terms(steam_name, steam_terms, app_id_hint)
                            except Exception:
                                ok = False
                    if not ok:
                        hint = await self._resolve_hltb_steam_hint(
                            cache_key=cache_key,
                            title=title,
                            categories=categories,
                        )
                        hint_title = str(hint.get("title", "") or "").strip()
                        hint_app_id = max(0, _safe_int(hint.get("app_id"), 0))
                        if hint.get("ok") and hint_title:
                            hint_terms = self._build_catalog_cover_terms(title=hint_title, categories="") or [hint_title]
                            await try_terms(hint_title, hint_terms, hint_app_id or int(app_id or 0))
            except Exception as exc:
                config.logger.warning("解析 HLTB 时长失败: title=%s error=%s", title, exc)

        has_positive_payload = bool(main_hours > 0 or total_hours > 0 or hltb_game_id > 0)
        expires_at = now_ts + (
            HLTB_CACHE_TTL_SECONDS if has_positive_payload else HLTB_NEGATIVE_TTL_SECONDS
        )
        cache_value = {
            "main_story_hours": float(main_hours),
            "total_hours": float(total_hours),
            "hltb_game_id": int(hltb_game_id),
            "matched_title": matched_title,
            "source_term": source_term,
            "expires_at": int(expires_at),
        }
        async with self._hltb_lock:
            self._hltb_cache[cache_key] = cache_value

        return {
            "main_story_hours": float(main_hours),
            "main_story_text": _format_hours_value(main_hours),
            "total_hours": float(total_hours),
            "total_time_text": _format_hours_value(total_hours),
            "hltb_game_id": int(hltb_game_id),
            "matched_title": matched_title,
            "source_term": source_term,
            "cached": False,
        }

    async def get_settings(self) -> Dict[str, Any]:
        """获取下载设置。"""
        return asdict(self.store.settings)

    def _new_cloud_save_state(self) -> Dict[str, Any]:
        """构建云存档任务默认状态。"""
        return {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "current_game": "",
            "total_games": 0,
            "processed_games": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": dict(self.store.cloud_save_last_result or {}),
        }

    def _copy_cloud_save_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """复制单游戏上传结果并规范字段。"""
        result = dict(item or {})
        result["game_id"] = str(result.get("game_id", "") or "")
        result["game_title"] = str(result.get("game_title", "") or "")
        result["game_key"] = str(result.get("game_key", "") or "")
        result["status"] = str(result.get("status", "") or "")
        result["reason"] = str(result.get("reason", "") or "")
        result["cloud_path"] = str(result.get("cloud_path", "") or "")
        source_paths = result.get("source_paths", [])
        if isinstance(source_paths, list):
            result["source_paths"] = [str(path or "") for path in source_paths if str(path or "").strip()]
        else:
            result["source_paths"] = []
        diagnostics = result.get("diagnostics", {})
        result["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        return result

    def _cloud_save_state_snapshot_locked(self) -> Dict[str, Any]:
        """在已持锁场景下复制云存档状态。"""
        state = dict(self._cloud_save_state or {})
        results = state.get("results", [])
        if isinstance(results, list):
            state["results"] = [self._copy_cloud_save_result(item) for item in results if isinstance(item, dict)]
        else:
            state["results"] = []
        diagnostics = state.get("diagnostics", {})
        state["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        last_result = state.get("last_result", {})
        state["last_result"] = dict(last_result) if isinstance(last_result, dict) else {}
        return state

    async def _set_cloud_save_state(self, **patch: Any) -> Dict[str, Any]:
        """更新云存档状态并返回快照。"""
        async with self._cloud_save_lock:
            state = self._cloud_save_state
            for key, value in patch.items():
                if key == "stage":
                    stage = str(value or "idle")
                    if stage not in CLOUD_SAVE_TASK_STAGES:
                        stage = "idle"
                    state["stage"] = stage
                    continue
                if key == "running":
                    state["running"] = bool(value)
                    continue
                if key == "progress":
                    try:
                        progress = float(value)
                    except Exception:
                        progress = 0.0
                    progress = max(0.0, min(100.0, progress))
                    state["progress"] = round(progress, 2)
                    continue
                if key in {"total_games", "processed_games", "uploaded", "skipped", "failed"}:
                    state[key] = max(0, _safe_int(value, 0))
                    continue
                if key in {"message", "reason", "current_game"}:
                    state[key] = str(value or "")
                    continue
                if key == "results":
                    rows = value if isinstance(value, list) else []
                    state["results"] = [self._copy_cloud_save_result(item) for item in rows if isinstance(item, dict)]
                    continue
                if key == "diagnostics":
                    state["diagnostics"] = dict(value) if isinstance(value, dict) else {}
                    continue
                if key == "last_result":
                    state["last_result"] = dict(value) if isinstance(value, dict) else {}
                    continue
                state[key] = value

            state["updated_at"] = _now_wall_ts()
            return self._cloud_save_state_snapshot_locked()

    async def _get_cloud_save_state_snapshot(self) -> Dict[str, Any]:
        """获取云存档状态快照。"""
        async with self._cloud_save_lock:
            return self._cloud_save_state_snapshot_locked()

    async def _cancel_cloud_save_task(self) -> None:
        """取消当前云存档任务（如有）。"""
        target: Optional[asyncio.Task] = None
        async with self._cloud_save_lock:
            if self._cloud_save_task and not self._cloud_save_task.done():
                target = self._cloud_save_task
            self._cloud_save_task = None

        if target is not None:
            target.cancel()
            try:
                await target
            except BaseException:
                pass

    def _new_cloud_save_restore_state(self) -> Dict[str, Any]:
        """构建云存档恢复任务默认状态。"""
        return {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "target_game_id": "",
            "target_game_title": "",
            "target_game_key": "",
            "target_version": "",
            "selected_entry_ids": [],
            "selected_target_dir": "",
            "requires_confirmation": False,
            "conflict_count": 0,
            "conflict_samples": [],
            "restored_files": 0,
            "restored_entries": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": dict(self.store.cloud_save_restore_last_result or {}),
        }

    def _copy_cloud_save_restore_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """复制单条恢复结果并规范字段。"""
        result = dict(item or {})
        result["entry_id"] = str(result.get("entry_id", "") or "")
        result["entry_name"] = str(result.get("entry_name", "") or "")
        result["status"] = str(result.get("status", "") or "")
        result["reason"] = str(result.get("reason", "") or "")
        result["file_count"] = max(0, _safe_int(result.get("file_count"), 0))
        diagnostics = result.get("diagnostics", {})
        result["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        return result

    def _cloud_save_restore_state_snapshot_locked(self) -> Dict[str, Any]:
        """在已持锁场景下复制恢复状态。"""
        state = dict(self._cloud_save_restore_state or {})
        selected_entry_ids = state.get("selected_entry_ids", [])
        if isinstance(selected_entry_ids, list):
            state["selected_entry_ids"] = [str(item or "") for item in selected_entry_ids if str(item or "").strip()]
        else:
            state["selected_entry_ids"] = []

        conflict_samples = state.get("conflict_samples", [])
        if isinstance(conflict_samples, list):
            state["conflict_samples"] = [str(item or "") for item in conflict_samples if str(item or "").strip()]
        else:
            state["conflict_samples"] = []

        results = state.get("results", [])
        if isinstance(results, list):
            state["results"] = [
                self._copy_cloud_save_restore_result(item)
                for item in results
                if isinstance(item, dict)
            ]
        else:
            state["results"] = []

        diagnostics = state.get("diagnostics", {})
        state["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        last_result = state.get("last_result", {})
        state["last_result"] = dict(last_result) if isinstance(last_result, dict) else {}
        return state

    async def _set_cloud_save_restore_state(self, **patch: Any) -> Dict[str, Any]:
        """更新恢复状态并返回快照。"""
        async with self._cloud_save_restore_lock:
            state = self._cloud_save_restore_state
            for key, value in patch.items():
                if key == "stage":
                    stage = str(value or "idle")
                    if stage not in CLOUD_SAVE_RESTORE_TASK_STAGES:
                        stage = "idle"
                    state["stage"] = stage
                    continue
                if key == "running":
                    state["running"] = bool(value)
                    continue
                if key == "progress":
                    try:
                        progress = float(value)
                    except Exception:
                        progress = 0.0
                    state["progress"] = max(0.0, min(100.0, round(progress, 2)))
                    continue
                if key in {"message", "reason", "target_game_id", "target_game_title", "target_game_key", "target_version", "selected_target_dir"}:
                    state[key] = str(value or "")
                    continue
                if key in {"requires_confirmation"}:
                    state[key] = bool(value)
                    continue
                if key in {"conflict_count", "restored_files", "restored_entries"}:
                    state[key] = max(0, _safe_int(value, 0))
                    continue
                if key == "selected_entry_ids":
                    rows = value if isinstance(value, list) else []
                    state["selected_entry_ids"] = [str(item or "") for item in rows if str(item or "").strip()]
                    continue
                if key == "conflict_samples":
                    rows = value if isinstance(value, list) else []
                    state["conflict_samples"] = [str(item or "") for item in rows if str(item or "").strip()]
                    continue
                if key == "results":
                    rows = value if isinstance(value, list) else []
                    state["results"] = [
                        self._copy_cloud_save_restore_result(item)
                        for item in rows
                        if isinstance(item, dict)
                    ]
                    continue
                if key == "diagnostics":
                    state["diagnostics"] = dict(value) if isinstance(value, dict) else {}
                    continue
                if key == "last_result":
                    state["last_result"] = dict(value) if isinstance(value, dict) else {}
                    continue
                state[key] = value

            state["updated_at"] = _now_wall_ts()
            return self._cloud_save_restore_state_snapshot_locked()

    async def _get_cloud_save_restore_state_snapshot(self) -> Dict[str, Any]:
        """获取云存档恢复状态快照。"""
        async with self._cloud_save_restore_lock:
            return self._cloud_save_restore_state_snapshot_locked()

    async def _clear_cloud_save_restore_plan(self) -> None:
        """清理恢复计划临时资源。"""
        async with self._cloud_save_restore_lock:
            plan = dict(self._cloud_save_restore_plan or {})
            self._cloud_save_restore_plan = {}
        self._cleanup_cloud_save_temp_paths(list(plan.get("temp_paths") or []))

    async def get_cloud_save_upload_status(self) -> Dict[str, Any]:
        """返回云存档上传任务状态。"""
        return {"state": await self._get_cloud_save_state_snapshot()}

    async def start_cloud_save_upload(
        self,
        *,
        trigger: str = "manual_all",
        target_record: Optional[TianyiInstalledGame] = None,
    ) -> Dict[str, Any]:
        """启动云存档上传任务。"""
        normalized_trigger = "auto_single" if str(trigger or "").strip().lower() == "auto_single" else "manual_all"
        login_ok, account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        scoped_record: Optional[TianyiInstalledGame] = None
        if normalized_trigger == "auto_single":
            if target_record is None:
                raise TianyiApiError("缺少自动上传目标游戏")
            scoped_record = self._find_installed_record(
                game_id=str(target_record.game_id or "").strip(),
                install_path=str(target_record.install_path or "").strip(),
            )
            if scoped_record is None:
                scoped_record = self._find_installed_record(game_id=str(target_record.game_id or "").strip())
            if scoped_record is None:
                scoped_record = copy.deepcopy(target_record)

        async with self._cloud_save_lock:
            task = self._cloud_save_task
            if task is not None and task.done():
                self._cloud_save_task = None
                task = None

            if task is not None:
                return {
                    "accepted": False,
                    "message": "已有云存档上传任务正在运行",
                    "state": self._cloud_save_state_snapshot_locked(),
                }

            self._cloud_save_state = self._new_cloud_save_state()
            self._cloud_save_state.update(
                {
                    "stage": "scanning",
                    "message": (
                        f"正在准备自动上传：{str(getattr(scoped_record, 'game_title', '') or '').strip() or '未命名游戏'}"
                        if normalized_trigger == "auto_single"
                        else "正在准备云存档上传任务"
                    ),
                    "reason": "",
                    "running": True,
                    "progress": 0.0,
                    "current_game": "",
                    "diagnostics": {
                        "user_account": account,
                        "trigger": normalized_trigger,
                        "target_game_id": str(getattr(scoped_record, "game_id", "") or ""),
                        "target_game_title": str(getattr(scoped_record, "game_title", "") or ""),
                    },
                    "updated_at": _now_wall_ts(),
                }
            )
            self._cloud_save_task = asyncio.create_task(
                self._run_cloud_save_upload_task(
                    cookie=cookie,
                    user_account=account,
                    trigger=normalized_trigger,
                    records=[scoped_record] if scoped_record is not None else None,
                ),
                name="freedeck-cloud-save-upload",
            )
            snapshot = self._cloud_save_state_snapshot_locked()

        return {
            "accepted": True,
            "message": (
                f"已开始自动上传：{str(getattr(scoped_record, 'game_title', '') or '').strip() or '未命名游戏'}"
                if normalized_trigger == "auto_single"
                else "云存档上传任务已启动"
            ),
            "state": snapshot,
        }

    async def _auto_upload_cloud_save_for_record(
        self,
        *,
        record: TianyiInstalledGame,
        app_id: int = 0,
    ) -> Dict[str, Any]:
        """在游戏结束后自动触发单游戏云存档上传。"""
        game_id = str(record.game_id or "").strip()
        game_title = str(record.game_title or "").strip() or "未命名游戏"
        try:
            result = await self.start_cloud_save_upload(trigger="auto_single", target_record=record)
            if bool(result.get("accepted")):
                config.logger.info(
                    "Cloud save auto upload started: game=%s game_id=%s app_id=%s",
                    game_title,
                    game_id,
                    max(0, int(app_id or 0)),
                )
            else:
                config.logger.info(
                    "Cloud save auto upload skipped: game=%s game_id=%s app_id=%s reason=upload_busy message=%s",
                    game_title,
                    game_id,
                    max(0, int(app_id or 0)),
                    str(result.get("message", "") or ""),
                )
            return result
        except TianyiApiError as exc:
            message = str(exc)
            config.logger.info(
                "Cloud save auto upload skipped: game=%s game_id=%s app_id=%s reason=login_required message=%s",
                game_title,
                game_id,
                max(0, int(app_id or 0)),
                message,
            )
            async with self._cloud_save_lock:
                task = self._cloud_save_task
                task_running = task is not None and not task.done()
            if not task_running:
                await self._set_cloud_save_state(
                    stage="idle",
                    running=False,
                    progress=0.0,
                    current_game=game_title,
                    message=f"自动上传已跳过：{message}",
                    reason="login_required",
                    diagnostics={
                        "trigger": "auto_single",
                        "game_id": game_id,
                        "game_title": game_title,
                        "app_id": max(0, int(app_id or 0)),
                        "error": message,
                    },
                )
            return {"accepted": False, "reason": "login_required", "message": message}
        except Exception as exc:
            config.logger.exception("Cloud save auto upload trigger failed: %s", exc)
            async with self._cloud_save_lock:
                task = self._cloud_save_task
                task_running = task is not None and not task.done()
            if not task_running:
                await self._set_cloud_save_state(
                    stage="idle",
                    running=False,
                    progress=0.0,
                    current_game=game_title,
                    message=f"自动上传触发失败：{exc}",
                    reason="unexpected_error",
                    diagnostics={
                        "trigger": "auto_single",
                        "game_id": game_id,
                        "game_title": game_title,
                        "app_id": max(0, int(app_id or 0)),
                        "error": str(exc),
                    },
                )
            return {"accepted": False, "reason": "unexpected_error", "message": str(exc)}

    async def get_cloud_save_restore_status(self) -> Dict[str, Any]:
        """返回云存档恢复任务状态。"""
        return {"state": await self._get_cloud_save_restore_state_snapshot()}

    async def list_cloud_save_restore_options(self) -> Dict[str, Any]:
        """列出可恢复的云存档版本（按游戏分组）。"""
        login_ok, _account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        await self._set_cloud_save_restore_state(
            stage="listing",
            running=True,
            message="正在拉取云存档版本列表",
            reason="",
            progress=0.0,
            diagnostics={},
        )

        games = self._collect_cloud_restore_games()
        grouped: List[Dict[str, Any]] = []
        diagnostics: List[Dict[str, Any]] = []

        try:
            total = len(games)
            for index, game in enumerate(games, start=1):
                game_id = str(game.get("game_id", "") or "").strip()
                game_title = str(game.get("game_title", "") or "").strip() or "未命名游戏"
                game_key = str(game.get("game_key", "") or "").strip()

                item: Dict[str, Any] = {
                    "game_id": game_id,
                    "game_title": game_title,
                    "game_key": game_key,
                    "versions": [],
                    "available": False,
                    "reason": "",
                }

                try:
                    listed = await list_cloud_archives(cookie=cookie, remote_folder_parts=[game_key])
                    files = listed.get("files")
                    if not isinstance(files, list):
                        files = []
                    versions: List[Dict[str, Any]] = []
                    for file_row in files:
                        if not isinstance(file_row, dict):
                            continue
                        name = str(file_row.get("name", "") or "").strip()
                        if not name.lower().endswith(".7z"):
                            continue
                        ts = self._parse_cloud_save_version_timestamp(name)
                        versions.append(
                            {
                                "version_name": name,
                                "timestamp": ts,
                                "display_time": self._format_cloud_save_version_time(ts, name),
                                "size_bytes": max(0, _safe_int(file_row.get("size"), 0)),
                                "file_id": str(file_row.get("file_id", "") or ""),
                                "last_op_time": str(file_row.get("last_op_time", "") or ""),
                            }
                        )
                    versions.sort(
                        key=lambda row: (
                            -_safe_int(row.get("timestamp"), 0),
                            str(row.get("version_name", "") or ""),
                        )
                    )

                    item["versions"] = versions
                    item["available"] = bool(versions)
                    if not versions:
                        item["reason"] = "no_valid_versions"
                    diagnostics.append(
                        {
                            "game_key": game_key,
                            "exists": bool(listed.get("exists", False)),
                            "file_count": len(versions),
                            "trace": listed.get("trace", []),
                        }
                    )
                except TianyiApiError as exc:
                    item["available"] = False
                    item["reason"] = "list_failed"
                    diagnostics.append(
                        {
                            "game_key": game_key,
                            "error": str(exc),
                            "api_diagnostics": dict(getattr(exc, "diagnostics", {}) or {}),
                        }
                    )

                grouped.append(item)
                progress = 100.0 if total <= 0 else (float(index) / float(total)) * 100.0
                await self._set_cloud_save_restore_state(
                    stage="listing",
                    running=True,
                    message=f"正在拉取版本列表 {index}/{total}",
                    progress=progress,
                )
        finally:
            await self._set_cloud_save_restore_state(
                stage="completed",
                running=False,
                progress=100.0,
                message="云存档版本列表已更新",
                diagnostics={"games": len(grouped), "details": diagnostics},
            )

        return {
            "games": grouped,
            "updated_at": _now_wall_ts(),
        }

    async def list_cloud_save_restore_entries(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        version_name: str,
    ) -> Dict[str, Any]:
        """读取指定版本的可选存档项。"""
        login_ok, _account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        normalized_game_id = str(game_id or "").strip()
        normalized_game_title = str(game_title or "").strip()
        normalized_game_key = str(game_key or "").strip()
        normalized_version = str(version_name or "").strip()
        if not normalized_game_key:
            normalized_game_key = self._build_cloud_save_game_key(normalized_game_id, normalized_game_title)
        if not normalized_version:
            raise TianyiApiError("缺少版本名称")

        await self._set_cloud_save_restore_state(
            stage="planning",
            running=True,
            message="正在读取存档项",
            reason="",
            progress=0.0,
            target_game_id=normalized_game_id,
            target_game_title=normalized_game_title,
            target_game_key=normalized_game_key,
            target_version=normalized_version,
            diagnostics={},
        )

        bundle = await self._download_and_extract_cloud_restore_version(
            cookie=cookie,
            game_key=normalized_game_key,
            version_name=normalized_version,
        )
        try:
            extract_dir = str(bundle.get("extract_dir", "") or "")
            entries = [dict(item) for item in list(bundle.get("entries") or []) if isinstance(item, dict)]
            entry_views: List[Dict[str, Any]] = []
            for item in entries:
                entry_id = str(item.get("entry_id", "") or "").strip()
                entry_name = str(item.get("entry_name", "") or "").strip() or entry_id
                rel_path = str(item.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                entry_root = os.path.realpath(os.path.join(extract_dir, rel_path)) if rel_path else extract_dir
                file_count = 0
                if os.path.isfile(entry_root):
                    file_count = 1
                elif os.path.isdir(entry_root):
                    for _dirpath, _dirnames, filenames in os.walk(entry_root):
                        file_count += len(filenames)
                entry_views.append(
                    {
                        "entry_id": entry_id,
                        "entry_name": entry_name,
                        "archive_rel_path": rel_path,
                        "file_count": max(0, file_count),
                    }
                )

            await self._set_cloud_save_restore_state(
                stage="ready",
                running=False,
                progress=100.0,
                message=f"已读取 {len(entry_views)} 个存档项",
                selected_entry_ids=[str(row.get("entry_id", "") or "") for row in entry_views],
                diagnostics={},
            )

            return {
                "game_id": normalized_game_id,
                "game_key": normalized_game_key,
                "game_title": normalized_game_title,
                "version_name": normalized_version,
                "entries": entry_views,
            }
        finally:
            self._cleanup_cloud_save_temp_paths([str(bundle.get("temp_dir", "") or "")])

    async def plan_cloud_save_restore(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        version_name: str,
        selected_entry_ids: Sequence[str],
        target_dir: str = "",
    ) -> Dict[str, Any]:
        """生成恢复计划（冲突探测，不写入）。"""
        login_ok, _account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        normalized_game_id = str(game_id or "").strip()
        normalized_game_title = str(game_title or "").strip() or "未命名游戏"
        normalized_game_key = str(game_key or "").strip()
        normalized_version = str(version_name or "").strip()
        if not normalized_game_key:
            normalized_game_key = self._build_cloud_save_game_key(normalized_game_id, normalized_game_title)
        if not normalized_version:
            raise TianyiApiError("缺少版本名称")

        await self._clear_cloud_save_restore_plan()
        await self._set_cloud_save_restore_state(
            stage="planning",
            running=True,
            progress=0.0,
            message="正在生成恢复计划",
            reason="",
            target_game_id=normalized_game_id,
            target_game_title=normalized_game_title,
            target_game_key=normalized_game_key,
            target_version=normalized_version,
            selected_target_dir="",
            selected_entry_ids=[],
            requires_confirmation=False,
            conflict_count=0,
            conflict_samples=[],
            restored_files=0,
            restored_entries=0,
            results=[],
            diagnostics={},
        )

        bundle = await self._download_and_extract_cloud_restore_version(
            cookie=cookie,
            game_key=normalized_game_key,
            version_name=normalized_version,
        )

        temp_dir = str(bundle.get("temp_dir", "") or "")
        extract_dir = str(bundle.get("extract_dir", "") or "")
        manifest = dict(bundle.get("manifest") or {})
        manifest_playtime = self._extract_manifest_playtime_payload(manifest)
        entries = [dict(item) for item in list(bundle.get("entries") or []) if isinstance(item, dict)]
        available_entry_ids = [str(item.get("entry_id", "") or "").strip() for item in entries if str(item.get("entry_id", "") or "").strip()]
        selected_ids = [str(item or "").strip() for item in list(selected_entry_ids or []) if str(item or "").strip()]
        if not selected_ids:
            selected_ids = list(available_entry_ids)

        is_switch_manifest = self._manifest_is_switch_eden(manifest=manifest, entries=entries)
        entry_target_dirs: Dict[str, str] = {}
        selected_set = {str(item or "").strip() for item in list(selected_ids or []) if str(item or "").strip()}
        target_candidates: List[str] = []
        normalized_target = self._normalize_dir_path(str(target_dir or "").strip())
        diagnostics: Dict[str, Any] = {
            "platform": "switch" if is_switch_manifest else "pc",
        }

        if is_switch_manifest:
            target_result = self._resolve_switch_restore_save_base_candidates(
                game_id=normalized_game_id,
                game_key=normalized_game_key,
                game_title=normalized_game_title,
                manifest=manifest,
                entries=entries,
            )
            target_candidates = [str(item or "") for item in list(target_result.get("candidates") or []) if str(item or "").strip()]
            diagnostics = {
                **diagnostics,
                **dict(target_result.get("diagnostics") or {}),
            }

            if not target_candidates:
                self._cleanup_cloud_save_temp_paths([temp_dir])
                await self._set_cloud_save_restore_state(
                    stage="failed",
                    running=False,
                    progress=100.0,
                    message="未找到可恢复的 Switch 存档目录",
                    reason="target_not_found",
                    selected_entry_ids=selected_ids,
                    diagnostics=diagnostics,
                )
                return {
                    "accepted": False,
                    "reason": "target_not_found",
                    "message": "未找到可恢复的 Switch 存档目录，请确认 Eden 已初始化并至少运行过一次游戏",
                    "target_candidates": [],
                    "available_entries": entries,
                }
            if normalized_target and normalized_target not in target_candidates:
                diagnostics["requested_target_dir"] = normalized_target
                diagnostics["target_dir_stale"] = True
                normalized_target = ""
            if not normalized_target:
                if len(target_candidates) == 1:
                    normalized_target = target_candidates[0]
                else:
                    self._cleanup_cloud_save_temp_paths([temp_dir])
                    await self._set_cloud_save_restore_state(
                        stage="ready",
                        running=False,
                        progress=100.0,
                        message="检测到多个 Switch 存档根目录，请先选择",
                        reason="target_selection_required",
                        selected_entry_ids=selected_ids,
                        diagnostics=diagnostics,
                    )
                    return {
                        "accepted": False,
                        "reason": "target_selection_required",
                        "message": "检测到多个 Switch 存档根目录，请先选择恢复目标",
                        "target_candidates": target_candidates,
                        "available_entries": entries,
                    }
            copy_plan = self._build_switch_restore_copy_plan(
                extract_dir=extract_dir,
                manifest=manifest,
                entries=entries,
                selected_entry_ids=selected_ids,
                save_base=normalized_target,
            )
        else:
            compat_user_dir = self._resolve_current_compat_user_dir(normalized_game_id)
            if compat_user_dir and selected_set:
                for entry in entries:
                    entry_id = str(entry.get("entry_id", "") or "").strip()
                    if not entry_id or entry_id not in selected_set:
                        continue
                    source_path = str(entry.get("source_path", "") or "").strip()
                    archive_rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                    relative = self._extract_proton_relative_path(
                        source_path=source_path,
                        archive_rel_path=archive_rel_path,
                    )
                    if not relative:
                        continue
                    candidate = os.path.join(compat_user_dir, *[part for part in relative.split("/") if part])
                    normalized_candidate = self._normalize_dir_path(candidate)
                    if normalized_candidate:
                        entry_target_dirs[entry_id] = normalized_candidate

            auto_target_ready = bool(selected_set) and len(entry_target_dirs) >= len(selected_set)
            diagnostics = {
                **diagnostics,
                "compat_user_dir": compat_user_dir,
                "auto_target_from_manifest": auto_target_ready,
                "entry_target_dirs": dict(entry_target_dirs),
            }

            if auto_target_ready:
                if normalized_target and normalized_target not in set(entry_target_dirs.values()):
                    diagnostics["requested_target_dir"] = normalized_target
                    diagnostics["target_dir_stale"] = True
                normalized_target = compat_user_dir or self._normalize_dir_path(next(iter(entry_target_dirs.values()), ""))
            else:
                target_result = self._resolve_cloud_restore_target_candidates(
                    game_id=normalized_game_id,
                    game_key=normalized_game_key,
                    game_title=normalized_game_title,
                    entries=entries,
                )
                target_candidates = [str(item or "") for item in list(target_result.get("candidates") or []) if str(item or "").strip()]
                diagnostics = {
                    **diagnostics,
                    **dict(target_result.get("diagnostics") or {}),
                }

                if not target_candidates:
                    self._cleanup_cloud_save_temp_paths([temp_dir])
                    await self._set_cloud_save_restore_state(
                        stage="failed",
                        running=False,
                        progress=100.0,
                        message="未找到可恢复的目标目录",
                        reason="target_not_found",
                        selected_entry_ids=selected_ids,
                        diagnostics=diagnostics,
                    )
                    return {
                        "accepted": False,
                        "reason": "target_not_found",
                        "message": "未找到可恢复的目标目录，请确保游戏已安装并至少启动过一次",
                        "target_candidates": [],
                        "available_entries": entries,
                    }

                if normalized_target and normalized_target not in target_candidates:
                    diagnostics["requested_target_dir"] = normalized_target
                    diagnostics["target_dir_stale"] = True
                    normalized_target = ""

                if not normalized_target:
                    if len(target_candidates) == 1:
                        normalized_target = target_candidates[0]
                    else:
                        self._cleanup_cloud_save_temp_paths([temp_dir])
                        await self._set_cloud_save_restore_state(
                            stage="ready",
                            running=False,
                            progress=100.0,
                            message="检测到多个目标目录，请先选择",
                            reason="target_selection_required",
                            selected_entry_ids=selected_ids,
                            diagnostics=diagnostics,
                        )
                        return {
                            "accepted": False,
                            "reason": "target_selection_required",
                            "message": "检测到多个目标目录，请先选择恢复目标",
                            "target_candidates": target_candidates,
                            "available_entries": entries,
                        }

            copy_plan = self._build_restore_copy_plan(
                extract_dir=extract_dir,
                entries=entries,
                selected_entry_ids=selected_ids,
                target_dir=normalized_target,
                entry_target_dirs=entry_target_dirs,
            )
        copy_pairs = list(copy_plan.get("copy_pairs") or [])
        plan_items = [dict(item) for item in list(copy_plan.get("plan_items") or []) if isinstance(item, dict)]
        if is_switch_manifest:
            entry_target_dirs = {
                str(item.get("entry_id", "") or ""): str(item.get("target_dir", "") or "")
                for item in plan_items
                if str(item.get("entry_id", "") or "").strip() and str(item.get("target_dir", "") or "").strip()
            }
        conflict_count = max(0, _safe_int(copy_plan.get("conflict_count"), 0))
        conflict_samples = [str(item or "") for item in list(copy_plan.get("conflict_samples") or []) if str(item or "").strip()]
        requires_confirmation = conflict_count > 0

        plan_id = uuid.uuid4().hex
        async with self._cloud_save_restore_lock:
            self._cloud_save_restore_plan = {
                "plan_id": plan_id,
                "temp_paths": [temp_dir],
                "copy_pairs": copy_pairs,
                "plan_items": plan_items,
                "requires_confirmation": requires_confirmation,
                "conflict_count": conflict_count,
                "conflict_samples": conflict_samples,
                "target_dir": normalized_target,
                "entry_target_dirs": dict(entry_target_dirs),
                "game_id": normalized_game_id,
                "game_key": normalized_game_key,
                "game_title": normalized_game_title,
                "version_name": normalized_version,
                "selected_entry_ids": selected_ids,
                "manifest_playtime": manifest_playtime,
            }

        await self._set_cloud_save_restore_state(
            stage="ready",
            running=False,
            progress=100.0,
            message="恢复计划已生成",
            reason="",
            selected_entry_ids=selected_ids,
            selected_target_dir=normalized_target,
            requires_confirmation=requires_confirmation,
            conflict_count=conflict_count,
            conflict_samples=conflict_samples,
            restored_files=0,
            restored_entries=0,
            diagnostics={
                **diagnostics,
                "target_candidates": target_candidates,
                "plan_items": plan_items,
                "manifest_playtime": manifest_playtime,
            },
        )

        return {
            "accepted": True,
            "plan_id": plan_id,
            "message": "恢复计划已生成",
            "reason": "",
            "requires_confirmation": requires_confirmation,
            "conflict_count": conflict_count,
            "conflict_samples": conflict_samples,
            "target_candidates": target_candidates,
            "selected_target_dir": normalized_target,
            "selected_entry_ids": selected_ids,
            "available_entries": entries,
            "restorable_files": len(copy_pairs),
            "restorable_entries": len(plan_items),
        }

    async def apply_cloud_save_restore(
        self,
        *,
        plan_id: str,
        confirm_overwrite: bool = False,
    ) -> Dict[str, Any]:
        """执行恢复计划（确认后写入）。"""
        normalized_plan_id = str(plan_id or "").strip()
        if not normalized_plan_id:
            raise TianyiApiError("缺少 plan_id")

        async with self._cloud_save_restore_lock:
            plan = dict(self._cloud_save_restore_plan or {})

        if not plan or str(plan.get("plan_id", "") or "").strip() != normalized_plan_id:
            raise TianyiApiError("恢复计划不存在或已过期，请重新规划")

        requires_confirmation = bool(plan.get("requires_confirmation", False))
        if requires_confirmation and not bool(confirm_overwrite):
            result_payload = {
                "status": "cancelled",
                "reason": "user_cancelled",
                "message": "用户已取消覆盖恢复",
                "game_id": str(plan.get("game_id", "") or ""),
                "game_key": str(plan.get("game_key", "") or ""),
                "game_title": str(plan.get("game_title", "") or ""),
                "version_name": str(plan.get("version_name", "") or ""),
                "target_dir": str(plan.get("target_dir", "") or ""),
                "selected_entry_ids": list(plan.get("selected_entry_ids") or []),
                "restored_files": 0,
                "restored_entries": 0,
                "conflicts_overwritten": 0,
                "results": [],
                "diagnostics": {"requires_confirmation": True},
                "finished_at": _now_wall_ts(),
            }
            await asyncio.to_thread(self.store.set_cloud_save_restore_last_result, result_payload)
            await self._set_cloud_save_restore_state(
                stage="failed",
                running=False,
                progress=100.0,
                message=result_payload["message"],
                reason=result_payload["reason"],
                restored_files=0,
                restored_entries=0,
                results=[],
                diagnostics=dict(result_payload.get("diagnostics", {}) or {}),
                last_result=result_payload,
            )
            await self._clear_cloud_save_restore_plan()
            return result_payload

        copy_pairs = list(plan.get("copy_pairs") or [])
        plan_items = [dict(item) for item in list(plan.get("plan_items") or []) if isinstance(item, dict)]
        target_dir = str(plan.get("target_dir", "") or "")
        restored_files = 0
        results: List[Dict[str, Any]] = []
        exception_text = ""

        await self._set_cloud_save_restore_state(
            stage="applying",
            running=True,
            progress=0.0,
            message="正在恢复存档",
            reason="",
            restored_files=0,
            restored_entries=0,
            results=[],
        )

        try:
            total_files = len(copy_pairs)
            for index, (src, dst) in enumerate(copy_pairs, start=1):
                src_file = os.path.realpath(os.path.expanduser(str(src or "").strip()))
                dst_file = os.path.realpath(os.path.expanduser(str(dst or "").strip()))
                if not src_file or not os.path.isfile(src_file):
                    continue
                parent = os.path.dirname(dst_file)
                os.makedirs(parent, exist_ok=True)
                if os.path.isdir(dst_file):
                    shutil.rmtree(dst_file, ignore_errors=False)
                elif os.path.exists(dst_file):
                    os.remove(dst_file)
                shutil.copy2(src_file, dst_file)
                restored_files += 1
                progress = 100.0 if total_files <= 0 else (float(index) / float(total_files)) * 100.0
                await self._set_cloud_save_restore_state(
                    stage="applying",
                    running=True,
                    progress=progress,
                    message=f"正在恢复文件 {index}/{total_files}",
                    restored_files=restored_files,
                )

            for item in plan_items:
                results.append(
                    {
                        "entry_id": str(item.get("entry_id", "") or ""),
                        "entry_name": str(item.get("entry_name", "") or ""),
                        "status": "restored",
                        "reason": "",
                        "file_count": max(0, _safe_int(item.get("file_count"), 0)),
                        "diagnostics": {},
                    }
                )

            manifest_playtime = dict(plan.get("manifest_playtime") or {})
            playtime_merge: Dict[str, Any] = {}
            try:
                playtime_merge = await self._merge_cloud_restore_playtime(
                    game_id=str(plan.get("game_id", "") or ""),
                    game_key=str(plan.get("game_key", "") or ""),
                    target_dir=target_dir,
                    manifest_playtime=manifest_playtime,
                )
            except Exception as exc:
                playtime_merge = {
                    "merged": False,
                    "reason": "playtime_merge_failed",
                    "message": f"游玩时长合并失败：{exc}",
                }

            result_payload = {
                "status": "success",
                "reason": "",
                "message": f"云存档恢复完成（恢复文件 {restored_files}）",
                "game_id": str(plan.get("game_id", "") or ""),
                "game_key": str(plan.get("game_key", "") or ""),
                "game_title": str(plan.get("game_title", "") or ""),
                "version_name": str(plan.get("version_name", "") or ""),
                "target_dir": target_dir,
                "selected_entry_ids": list(plan.get("selected_entry_ids") or []),
                "restored_files": restored_files,
                "restored_entries": len(plan_items),
                "conflicts_overwritten": max(0, _safe_int(plan.get("conflict_count"), 0)),
                "results": results,
                "diagnostics": {
                    "requires_confirmation": requires_confirmation,
                    "conflict_samples": list(plan.get("conflict_samples") or []),
                    "entry_target_dirs": dict(plan.get("entry_target_dirs") or {}),
                    "playtime_merge": playtime_merge,
                },
                "finished_at": _now_wall_ts(),
            }
            await asyncio.to_thread(self.store.set_cloud_save_restore_last_result, result_payload)
            await self._set_cloud_save_restore_state(
                stage="completed",
                running=False,
                progress=100.0,
                message=str(result_payload.get("message", "") or "恢复完成"),
                reason="",
                restored_files=restored_files,
                restored_entries=len(plan_items),
                results=results,
                diagnostics=dict(result_payload.get("diagnostics", {}) or {}),
                last_result=result_payload,
            )
            return result_payload
        except Exception as exc:
            exception_text = str(exc)
            result_payload = {
                "status": "failed",
                "reason": "apply_failed",
                "message": f"云存档恢复失败：{exc}",
                "game_id": str(plan.get("game_id", "") or ""),
                "game_key": str(plan.get("game_key", "") or ""),
                "game_title": str(plan.get("game_title", "") or ""),
                "version_name": str(plan.get("version_name", "") or ""),
                "target_dir": target_dir,
                "selected_entry_ids": list(plan.get("selected_entry_ids") or []),
                "restored_files": restored_files,
                "restored_entries": 0,
                "conflicts_overwritten": 0,
                "results": results,
                "diagnostics": {"exception": exception_text},
                "finished_at": _now_wall_ts(),
            }
            await asyncio.to_thread(self.store.set_cloud_save_restore_last_result, result_payload)
            await self._set_cloud_save_restore_state(
                stage="failed",
                running=False,
                progress=100.0,
                message=str(result_payload.get("message", "") or "恢复失败"),
                reason=str(result_payload.get("reason", "") or "apply_failed"),
                restored_files=restored_files,
                restored_entries=0,
                results=results,
                diagnostics=dict(result_payload.get("diagnostics", {}) or {}),
                last_result=result_payload,
            )
            return result_payload
        finally:
            await self._clear_cloud_save_restore_plan()

    def _normalize_existing_dir(self, path: str) -> str:
        """规范化并校验目录存在。"""
        normalized = self._normalize_dir_path(path)
        if not normalized or not os.path.isdir(normalized):
            return ""
        return normalized

    def _normalize_dir_path(self, path: str) -> str:
        """规范化目录路径（允许目录暂不存在）。"""
        raw = str(path or "").strip()
        if not raw:
            return ""
        try:
            normalized = os.path.realpath(os.path.expanduser(raw))
        except Exception:
            return ""
        return str(normalized or "").strip()

    def _dedupe_paths(self, paths: Sequence[str], *, require_existing: bool = False) -> List[str]:
        """目录去重并去除被父目录覆盖的子目录。"""
        normalized: List[str] = []
        seen = set()
        for raw in list(paths or []):
            path = self._normalize_dir_path(str(raw or ""))
            if not path:
                continue
            if require_existing and not os.path.isdir(path):
                continue
            if path in seen:
                continue
            seen.add(path)
            normalized.append(path)

        normalized.sort(key=lambda item: (len(item), item))
        compacted: List[str] = []
        for path in normalized:
            covered = False
            for parent in compacted:
                if path == parent or path.startswith(parent + os.sep):
                    covered = True
                    break
            if not covered:
                compacted.append(path)
            if len(compacted) >= CLOUD_SAVE_MAX_SOURCE_PATHS:
                break
        return compacted

    def _new_runtime_repair_state(self) -> Dict[str, Any]:
        """构建运行库修复任务默认状态。"""
        return {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "total_games": 0,
            "processed_games": 0,
            "total_steps": 0,
            "completed_steps": 0,
            "succeeded_steps": 0,
            "skipped_steps": 0,
            "failed_steps": 0,
            "current_game_id": "",
            "current_game_title": "",
            "current_package_id": "",
            "current_package_label": "",
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": dict(self.store.runtime_repair_last_result or {}),
        }

    def _copy_runtime_repair_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """复制单条运行库修复结果并规范字段。"""
        result = dict(item or {})
        result["game_id"] = str(result.get("game_id", "") or "")
        result["game_title"] = str(result.get("game_title", "") or "")
        result["install_path"] = str(result.get("install_path", "") or "")
        result["package_id"] = str(result.get("package_id", "") or "")
        result["package_label"] = str(result.get("package_label", "") or "")
        result["status"] = str(result.get("status", "") or "")
        result["reason"] = str(result.get("reason", "") or "")
        result["message"] = str(result.get("message", "") or "")
        result["source_type"] = str(result.get("source_type", "") or "")
        result["source_path"] = str(result.get("source_path", "") or "")
        result["proton_tool"] = str(result.get("proton_tool", "") or "")
        result["app_id"] = max(0, _safe_int(result.get("app_id"), 0))
        result["return_code"] = _safe_int(result.get("return_code"), 0)
        result["duration_ms"] = max(0, _safe_int(result.get("duration_ms"), 0))
        result["log_excerpt"] = str(result.get("log_excerpt", "") or "")
        return result

    def _runtime_repair_state_snapshot_locked(self) -> Dict[str, Any]:
        """在已持锁场景下复制运行库修复状态。"""
        state = dict(self._runtime_repair_state or {})
        results = state.get("results", [])
        if isinstance(results, list):
            state["results"] = [self._copy_runtime_repair_result(item) for item in results if isinstance(item, dict)]
        else:
            state["results"] = []
        diagnostics = state.get("diagnostics", {})
        state["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        last_result = state.get("last_result", {})
        state["last_result"] = dict(last_result) if isinstance(last_result, dict) else {}
        return state

    async def _set_runtime_repair_state(self, **patch: Any) -> Dict[str, Any]:
        """更新运行库修复状态并返回快照。"""
        async with self._runtime_repair_lock:
            state = self._runtime_repair_state
            for key, value in patch.items():
                if key == "stage":
                    stage = str(value or "idle")
                    if stage not in RUNTIME_REPAIR_TASK_STAGES:
                        stage = "idle"
                    state["stage"] = stage
                    continue
                if key == "running":
                    state["running"] = bool(value)
                    continue
                if key == "progress":
                    try:
                        progress = float(value)
                    except Exception:
                        progress = 0.0
                    state["progress"] = round(max(0.0, min(100.0, progress)), 2)
                    continue
                if key in {"total_games", "processed_games", "total_steps", "completed_steps", "succeeded_steps", "skipped_steps", "failed_steps"}:
                    state[key] = max(0, _safe_int(value, 0))
                    continue
                if key in {"message", "reason", "current_game_id", "current_game_title", "current_package_id", "current_package_label"}:
                    state[key] = str(value or "")
                    continue
                if key == "results":
                    rows = value if isinstance(value, list) else []
                    state["results"] = [self._copy_runtime_repair_result(item) for item in rows if isinstance(item, dict)]
                    continue
                if key == "diagnostics":
                    state["diagnostics"] = dict(value) if isinstance(value, dict) else {}
                    continue
                if key == "last_result":
                    state["last_result"] = dict(value) if isinstance(value, dict) else {}
                    continue
                state[key] = value

            state["updated_at"] = _now_wall_ts()
            return self._runtime_repair_state_snapshot_locked()

    async def _get_runtime_repair_state_snapshot(self) -> Dict[str, Any]:
        """获取运行库修复状态快照。"""
        async with self._runtime_repair_lock:
            return self._runtime_repair_state_snapshot_locked()

    def _runtime_repair_candidate_steam_roots(self) -> List[str]:
        """列出可能的 Steam 根目录。"""
        homes: List[str] = []
        for raw in (_freedeck_base_home_dir(), str(os.environ.get("HOME", "") or ""), str(Path.home())):
            candidate = self._normalize_dir_path(raw)
            if candidate and candidate not in homes:
                homes.append(candidate)

        roots: List[str] = []
        for home in homes:
            for path in (
                os.path.join(home, ".local", "share", "Steam"),
                os.path.join(home, ".steam", "steam"),
            ):
                normalized = self._normalize_existing_dir(path)
                if normalized and os.path.isdir(os.path.join(normalized, "steamapps")) and normalized not in roots:
                    roots.append(normalized)
        return roots

    def _runtime_repair_find_steam_root(self) -> str:
        """返回首个可用 Steam 根目录。"""
        roots = self._runtime_repair_candidate_steam_roots()
        return roots[0] if roots else ""

    def _runtime_repair_common_redist_root(self, steam_root: str = "") -> str:
        """返回 Steam CommonRedist 根目录。"""
        normalized_root = self._normalize_existing_dir(steam_root)
        roots = [normalized_root] if normalized_root else []
        for item in self._runtime_repair_candidate_steam_roots():
            if item not in roots:
                roots.append(item)
        for root in roots:
            target = self._normalize_existing_dir(
                os.path.join(root, "steamapps", "common", "Steamworks Shared", "_CommonRedist")
            )
            if target:
                return target
        return ""

    def _runtime_repair_package_definitions(self, steam_root: str = "") -> List[Dict[str, Any]]:
        """构建首期支持的运行库包列表。"""
        common_root = self._runtime_repair_common_redist_root(steam_root)
        specs: List[Dict[str, Any]] = [
            {
                "package_id": "vcredist_2022_x64",
                "label": "VC++ 2015-2022（x64）",
                "description": "优先修复 64 位 Windows 游戏常见缺失的 VC++ 运行库。",
                "default_selected": True,
                "common_redist_rel_path": os.path.join("vcredist", "2022", "VC_redist.x64.exe"),
                "silent_args": ["/install", "/quiet", "/norestart"],
            },
            {
                "package_id": "vcredist_2022_x86",
                "label": "VC++ 2015-2022（x86）",
                "description": "兼容大量 32 位游戏或旧启动器；建议与 x64 一起安装。",
                "default_selected": True,
                "common_redist_rel_path": os.path.join("vcredist", "2022", "VC_redist.x86.exe"),
                "silent_args": ["/install", "/quiet", "/norestart"],
            },
            {
                "package_id": "vcredist_2012_x64",
                "label": "VC++ 2012（x64）",
                "description": "部分老游戏仍依赖 2012 版运行库。",
                "default_selected": False,
                "common_redist_rel_path": os.path.join("vcredist", "2012", "vcredist_x64.exe"),
                "silent_args": ["/install", "/quiet", "/norestart"],
            },
            {
                "package_id": "vcredist_2012_x86",
                "label": "VC++ 2012（x86）",
                "description": "部分 32 位老游戏依赖 2012（x86）运行库。",
                "default_selected": False,
                "common_redist_rel_path": os.path.join("vcredist", "2012", "vcredist_x86.exe"),
                "silent_args": ["/install", "/quiet", "/norestart"],
            },
            {
                "package_id": "directx_jun2010",
                "label": "DirectX June 2010",
                "description": "修复 D3DX / XAudio / XInput 等老式 DirectX 组件缺失。",
                "default_selected": False,
                "common_redist_rel_path": os.path.join("DirectX", "Jun2010", "DXSETUP.exe"),
                "silent_args": ["/silent"],
            },
            {
                "package_id": "dotnet48",
                "label": ".NET Framework 4.8",
                "description": "部分启动器或工具链依赖 .NET Framework 4.8。",
                "default_selected": False,
                "common_redist_rel_path": os.path.join("DotNet", "4.8", "ndp48-x86-x64-allos-enu.exe"),
                "silent_args": ["/q", "/norestart"],
            },
        ]

        search_prefixes = [
            "_CommonRedist",
            os.path.join("__Installer", "_CommonRedist"),
            "CommonRedist",
            "redist",
            "Redist",
        ]
        for item in specs:
            rel_path = str(item.get("common_redist_rel_path", "") or "").strip()
            local_rel_paths = [os.path.join(prefix, rel_path) for prefix in search_prefixes if prefix]
            fallback_path = self._normalize_dir_path(os.path.join(common_root, rel_path)) if common_root and rel_path else ""
            item["local_rel_paths"] = local_rel_paths
            item["fallback_path"] = fallback_path if fallback_path and os.path.isfile(fallback_path) else ""
            item["global_available"] = bool(item["fallback_path"])
            item["size_bytes"] = max(0, _safe_int(os.path.getsize(item["fallback_path"]), 0)) if item["fallback_path"] else 0
            item["source_hint"] = "优先游戏目录内 CommonRedist，缺失时回退 Steamworks Shared/_CommonRedist。"
        return specs

    def _resolve_runtime_repair_proton_tool_name(self, *, steam_root: str, app_id: int) -> str:
        """解析指定 AppID 当前绑定的 Proton 工具名。"""
        normalized_root = self._normalize_existing_dir(steam_root)
        app_id_unsigned = max(0, _safe_int(app_id, 0))
        if not normalized_root or app_id_unsigned <= 0:
            return "proton_experimental"
        config_path = os.path.join(normalized_root, "config", "config.vdf")
        if not os.path.isfile(config_path):
            return "proton_experimental"
        try:
            with open(config_path, "r", encoding="utf-8", errors="ignore") as fp:
                content = fp.read()
        except Exception:
            return "proton_experimental"

        pattern = re.compile(
            rf'"{re.escape(str(app_id_unsigned))}"\s*\{{.*?"name"\s*"\s*([^"\r\n]+)"',
            re.DOTALL,
        )
        matched = pattern.search(content)
        if not matched:
            return "proton_experimental"
        value = str(matched.group(1) or "").strip()
        return value or "proton_experimental"

    def _resolve_runtime_repair_proton_executable(self, *, steam_root: str, tool_name: str) -> str:
        """将 Proton 工具名解析为 proton 启动脚本路径。"""
        normalized_tool = str(tool_name or "").strip() or "proton_experimental"
        tool_key = normalized_tool.lower()
        roots = [self._normalize_existing_dir(steam_root)] if self._normalize_existing_dir(steam_root) else []
        for item in self._runtime_repair_candidate_steam_roots():
            if item not in roots:
                roots.append(item)

        for root in roots:
            custom_path = self._normalize_dir_path(os.path.join(root, "compatibilitytools.d", normalized_tool, "proton"))
            if custom_path and os.path.isfile(custom_path):
                return custom_path

        if tool_key in {"native"} or tool_key.startswith("steamlinuxruntime_"):
            return ""

        if tool_key in {"proton_experimental", "proton-experimental"}:
            for root in roots:
                target = self._normalize_dir_path(os.path.join(root, "steamapps", "common", "Proton - Experimental", "proton"))
                if target and os.path.isfile(target):
                    return target

        major: Optional[int] = None
        minor: Optional[int] = None
        version_match = re.match(r"^proton[-_](\d+)(?:[._-](\d+))?", normalized_tool, flags=re.IGNORECASE)
        if version_match:
            major = max(0, _safe_int(version_match.group(1), 0))
            minor = max(0, _safe_int(version_match.group(2), 0)) if version_match.group(2) is not None else None

        official_candidates: List[Tuple[int, int, str]] = []
        version_re = re.compile(r"^Proton\s+(\d+)(?:\.(\d+))?")
        for root in roots:
            common_dir = self._normalize_existing_dir(os.path.join(root, "steamapps", "common"))
            if not common_dir:
                continue
            try:
                entries = os.listdir(common_dir)
            except Exception:
                continue
            for name in entries:
                proton_path = self._normalize_dir_path(os.path.join(common_dir, name, "proton"))
                if not proton_path or not os.path.isfile(proton_path):
                    continue
                matched = version_re.match(str(name))
                if not matched:
                    continue
                item_major = max(0, _safe_int(matched.group(1), 0))
                item_minor = max(0, _safe_int(matched.group(2), 0))
                official_candidates.append((item_major, item_minor, proton_path))

        official_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        if tool_key == "proton-stable" and official_candidates:
            return official_candidates[0][2]
        if major is not None:
            for item_major, item_minor, proton_path in official_candidates:
                if item_major != major:
                    continue
                if minor is not None and item_minor != minor:
                    continue
                return proton_path

        for root in roots:
            target = self._normalize_dir_path(os.path.join(root, "steamapps", "common", "Proton - Experimental", "proton"))
            if target and os.path.isfile(target):
                return target
        return official_candidates[0][2] if official_candidates else ""

    def _collect_runtime_repair_candidates(self) -> List[Dict[str, Any]]:
        """收集可用于运行库修复的已安装 PC 游戏。"""
        rows: List[Dict[str, Any]] = []
        seen_keys: Set[str] = set()
        records = sorted(
            list(self.store.installed_games or []),
            key=lambda item: str(getattr(item, "game_title", "") or "").lower(),
        )
        for record in records:
            game_id = str(record.game_id or "").strip()
            title = str(record.game_title or "").strip() or game_id or "未命名游戏"
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            platform = str(record.platform or "").strip().lower()
            emulator_id = str(record.emulator_id or "").strip().lower()
            dedupe_key = game_id or f"path::{install_path}"
            if dedupe_key in seen_keys:
                continue
            if emulator_id:
                continue
            if platform and platform not in {"pc", "windows", "win", "native"}:
                continue
            if not install_path:
                continue

            try:
                shortcut = resolve_tianyi_shortcut_sync(game_id=game_id)
            except Exception as exc:
                shortcut = {"ok": False, "message": str(exc)}

            steam_root = self._normalize_existing_dir(str(shortcut.get("steam_root", "") or ""))
            app_id = max(0, _safe_int(shortcut.get("appid_unsigned"), 0))
            compat_user_dir = self._normalize_existing_dir(str(shortcut.get("compat_user_dir", "") or ""))
            compatdata_path = (
                self._normalize_existing_dir(os.path.join(steam_root, "steamapps", "compatdata", str(app_id)))
                if steam_root and app_id > 0
                else ""
            )
            prefix_ready = bool(compat_user_dir and compatdata_path)
            if not bool(shortcut.get("ok")):
                prefix_message = str(shortcut.get("message", "") or "").strip() or "未找到对应的 Steam 快捷方式"
            elif app_id <= 0:
                prefix_message = "未解析到 Steam AppID"
            elif not prefix_ready:
                prefix_message = "需先从 Steam 启动一次该游戏，生成 compatdata 前缀后才能修复"
            else:
                prefix_message = ""

            proton_tool = self._resolve_runtime_repair_proton_tool_name(steam_root=steam_root, app_id=app_id)
            rows.append(
                {
                    "game_id": game_id,
                    "title": title,
                    "install_path": install_path,
                    "prefix_ready": prefix_ready,
                    "prefix_message": prefix_message,
                    "steam_app_id": app_id,
                    "steam_root": steam_root,
                    "compat_user_dir": compat_user_dir,
                    "compatdata_path": compatdata_path,
                    "proton_tool": proton_tool,
                }
            )
            seen_keys.add(dedupe_key)
        return rows

    def _resolve_runtime_repair_package_source(self, *, install_path: str, package: Dict[str, Any]) -> Tuple[str, str]:
        """解析当前包的实际安装器来源。"""
        normalized_install = self._normalize_existing_dir(install_path)
        for rel_path in list(package.get("local_rel_paths") or []):
            if not normalized_install:
                break
            candidate = self._normalize_dir_path(os.path.join(normalized_install, str(rel_path or "")))
            if candidate and os.path.isfile(candidate):
                return candidate, "game_local"
        fallback_path = self._normalize_dir_path(str(package.get("fallback_path", "") or ""))
        if fallback_path and os.path.isfile(fallback_path):
            return fallback_path, "steam_common_redist"
        return "", ""

    def _runtime_repair_user_context(self) -> Tuple[str, int, int, str]:
        """返回用于执行 Proton 子进程的目标用户信息。"""
        try:
            info = pwd.getpwnam("deck")
        except Exception:
            info = pwd.getpwuid(os.getuid())
        return (
            str(info.pw_name or "deck"),
            int(info.pw_uid),
            int(info.pw_gid),
            str(info.pw_dir or _freedeck_base_home_dir()),
        )

    def _read_runtime_repair_env_file(self, path: str) -> Dict[str, str]:
        """读取简单的 KEY=VALUE 环境文件。"""
        rows: Dict[str, str] = {}
        raw_path = str(path or "").strip()
        if not raw_path or not os.path.isfile(raw_path):
            return rows
        try:
            with open(raw_path, "r", encoding="utf-8", errors="ignore") as handle:
                for raw_line in handle:
                    line = str(raw_line or "").strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = str(key or "").strip()
                    if not key:
                        continue
                    rows[key] = str(value or "").strip()
        except Exception as exc:
            config.logger.warning("Runtime repair env file load failed: path=%s err=%s", raw_path, exc)
        return rows

    def _runtime_repair_graphical_session_env(self, *, target_uid: int, target_home: str) -> Tuple[Dict[str, str], str]:
        """尽量恢复 deck 游戏会话中的图形环境变量。"""
        allowed_keys = {
            "DBUS_SESSION_BUS_ADDRESS",
            "DESKTOP_SESSION",
            "DISPLAY",
            "GAMESCOPE_WAYLAND_DISPLAY",
            "WAYLAND_DISPLAY",
            "XAUTHORITY",
            "XDG_CURRENT_DESKTOP",
            "XDG_RUNTIME_DIR",
            "XDG_SESSION_TYPE",
        }
        xdg_runtime_dir = f"/run/user/{target_uid}"
        env: Dict[str, str] = {}
        source = ""

        env_files = [
            os.path.join(xdg_runtime_dir, "gamescope-environment"),
            os.path.join(target_home, ".config", "gamescope-environment"),
        ]
        for env_file in env_files:
            raw_env = self._read_runtime_repair_env_file(env_file)
            if not raw_env:
                continue
            for key in allowed_keys:
                value = str(raw_env.get(key, "") or "").strip()
                if value:
                    env[key] = value
            if env:
                source = env_file
                break

        if os.path.isdir(xdg_runtime_dir):
            env.setdefault("XDG_RUNTIME_DIR", xdg_runtime_dir)
            bus_path = os.path.join(xdg_runtime_dir, "bus")
            if os.path.exists(bus_path):
                env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={bus_path}")
            gamescope_socket = os.path.join(xdg_runtime_dir, "gamescope-0")
            if os.path.exists(gamescope_socket):
                env.setdefault("GAMESCOPE_WAYLAND_DISPLAY", "gamescope-0")
            wayland_socket = os.path.join(xdg_runtime_dir, "wayland-0")
            if os.path.exists(wayland_socket):
                env.setdefault("WAYLAND_DISPLAY", "wayland-0")

        if not env.get("WAYLAND_DISPLAY"):
            gamescope_wayland = str(env.get("GAMESCOPE_WAYLAND_DISPLAY", "") or "").strip()
            if gamescope_wayland:
                env["WAYLAND_DISPLAY"] = gamescope_wayland

        if not env.get("DISPLAY"):
            for display_name in ("X0", "X1"):
                if os.path.exists(os.path.join("/tmp/.X11-unix", display_name)):
                    env["DISPLAY"] = f":{display_name[1:]}"
                    break

        return env, source

    async def _execute_runtime_repair_step(
        self,
        *,
        game: Dict[str, Any],
        package: Dict[str, Any],
    ) -> Dict[str, Any]:
        """在指定 Proton 前缀内执行单个运行库安装。"""
        game_id = str(game.get("game_id", "") or "")
        game_title = str(game.get("title", "") or "")
        install_path = self._normalize_existing_dir(str(game.get("install_path", "") or ""))
        steam_root = self._normalize_existing_dir(str(game.get("steam_root", "") or ""))
        compatdata_path = self._normalize_existing_dir(str(game.get("compatdata_path", "") or ""))
        app_id = max(0, _safe_int(game.get("steam_app_id"), 0))
        proton_tool = str(game.get("proton_tool", "") or "").strip() or "proton_experimental"
        package_id = str(package.get("package_id", "") or "")
        package_label = str(package.get("label", package_id) or package_id)
        package_args = [str(arg or "") for arg in list(package.get("silent_args") or []) if str(arg or "").strip()]
        started_at = time.monotonic()

        base_result: Dict[str, Any] = {
            "game_id": game_id,
            "game_title": game_title,
            "install_path": install_path,
            "package_id": package_id,
            "package_label": package_label,
            "status": "failed",
            "reason": "",
            "message": "",
            "source_type": "",
            "source_path": "",
            "proton_tool": proton_tool,
            "app_id": app_id,
            "return_code": 0,
            "duration_ms": 0,
            "log_excerpt": "",
        }

        if not install_path:
            base_result.update(reason="install_path_missing", message="安装目录不存在")
            return base_result
        if not steam_root or app_id <= 0 or not compatdata_path:
            base_result.update(reason="prefix_unresolved", message="未解析到 Proton 前缀，请先从 Steam 启动一次该游戏")
            return base_result

        installer_path, source_type = self._resolve_runtime_repair_package_source(install_path=install_path, package=package)
        if not installer_path:
            base_result.update(
                status="skipped",
                reason="installer_missing",
                message=f"未找到 {package_label} 安装器（游戏目录与 Steam CommonRedist 均不存在）",
            )
            return base_result

        proton_executable = self._resolve_runtime_repair_proton_executable(steam_root=steam_root, tool_name=proton_tool)
        if not proton_executable:
            base_result.update(reason="proton_missing", message=f"未找到 Proton 工具：{proton_tool}")
            return base_result

        target_user, target_uid, target_gid, target_home = self._runtime_repair_user_context()
        run_env = dict(os.environ)
        run_env.update(
            {
                "HOME": target_home,
                "USER": target_user,
                "LOGNAME": target_user,
                "PATH": run_env.get("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"),
                "WINEDEBUG": "-all",
                "STEAM_COMPAT_DATA_PATH": compatdata_path,
                "STEAM_COMPAT_CLIENT_INSTALL_PATH": steam_root,
                "STEAM_COMPAT_INSTALL_PATH": install_path,
                "STEAM_COMPAT_LIBRARY_PATHS": ":".join(
                    [
                        path
                        for path in [
                            install_path,
                            os.path.dirname(install_path),
                            os.path.dirname(os.path.dirname(install_path)),
                            steam_root,
                        ]
                        if path
                    ]
                ),
                "SteamAppId": str(app_id),
                "SteamGameId": str(app_id),
                "STEAM_COMPAT_APP_ID": str(app_id),
            }
        )
        graphical_env, graphical_env_source = self._runtime_repair_graphical_session_env(
            target_uid=target_uid,
            target_home=target_home,
        )
        if graphical_env:
            run_env.update(graphical_env)
            config.logger.info(
                "Runtime repair launch env: app_id=%s display=%s wayland=%s gamescope=%s source=%s",
                app_id,
                run_env.get("DISPLAY", ""),
                run_env.get("WAYLAND_DISPLAY", ""),
                run_env.get("GAMESCOPE_WAYLAND_DISPLAY", ""),
                graphical_env_source or "derived",
            )
        else:
            config.logger.warning(
                "Runtime repair launch env missing graphical session vars: app_id=%s uid=%s compatdata=%s",
                app_id,
                target_uid,
                compatdata_path,
            )
        launch_context = (
            "launch_env "
            f"display={run_env.get('DISPLAY', '') or '-'} "
            f"wayland={run_env.get('WAYLAND_DISPLAY', '') or '-'} "
            f"gamescope={run_env.get('GAMESCOPE_WAYLAND_DISPLAY', '') or '-'} "
            f"source={graphical_env_source or 'derived'}"
        )

        preexec_fn = None
        if os.geteuid() == 0 and target_uid > 0 and os.geteuid() != target_uid:
            def _drop_privileges() -> None:
                os.initgroups(target_user, target_gid)
                os.setgid(target_gid)
                os.setuid(target_uid)

            preexec_fn = _drop_privileges

        cmd = [proton_executable, "waitforexitandrun", installer_path, *package_args]
        output_lines: List[str] = []
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=os.path.dirname(installer_path) or install_path,
            env=run_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            preexec_fn=preexec_fn,
        )

        try:
            stdout_data, _ = await asyncio.wait_for(process.communicate(), timeout=RUNTIME_REPAIR_STEP_TIMEOUT_SECONDS)
            text = stdout_data.decode("utf-8", errors="ignore") if stdout_data else ""
            output_lines = [line.strip() for line in text.splitlines() if line.strip()]
            if len(output_lines) > RUNTIME_REPAIR_LOG_MAX_LINES:
                output_lines = output_lines[-RUNTIME_REPAIR_LOG_MAX_LINES:]
            return_code = int(process.returncode or 0)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except Exception:
                pass
            try:
                await process.wait()
            except Exception:
                pass
            duration_ms = max(0, int((time.monotonic() - started_at) * 1000))
            excerpt_lines = [launch_context, *output_lines]
            excerpt = "\n".join(excerpt_lines[-RUNTIME_REPAIR_LOG_MAX_LINES:])
            base_result.update(
                reason="timeout",
                message=f"{package_label} 安装超时（>{RUNTIME_REPAIR_STEP_TIMEOUT_SECONDS} 秒）",
                source_type=source_type,
                source_path=installer_path,
                duration_ms=duration_ms,
                log_excerpt=excerpt,
            )
            return base_result
        except Exception as exc:
            try:
                process.kill()
            except Exception:
                pass
            try:
                await process.wait()
            except Exception:
                pass
            duration_ms = max(0, int((time.monotonic() - started_at) * 1000))
            excerpt_lines = [launch_context, *output_lines]
            excerpt = "\n".join(excerpt_lines[-RUNTIME_REPAIR_LOG_MAX_LINES:])
            base_result.update(
                reason="launch_failed",
                message=f"{package_label} 启动失败：{exc}",
                source_type=source_type,
                source_path=installer_path,
                duration_ms=duration_ms,
                log_excerpt=excerpt,
            )
            return base_result

        duration_ms = max(0, int((time.monotonic() - started_at) * 1000))
        excerpt_lines = [launch_context, *output_lines]
        excerpt = "\n".join(excerpt_lines[-RUNTIME_REPAIR_LOG_MAX_LINES:])
        base_result.update(
            return_code=return_code,
            source_type=source_type,
            source_path=installer_path,
            duration_ms=duration_ms,
            log_excerpt=excerpt,
        )

        if return_code in {0, 1641, 3010}:
            base_result["status"] = "success"
            base_result["reason"] = "restart_required" if return_code in {1641, 3010} else ""
            base_result["message"] = (
                f"{package_label} 安装完成"
                if return_code == 0
                else f"{package_label} 安装完成（Windows 侧返回需重启，通常可忽略）"
            )
            return base_result
        if return_code == 1638:
            base_result["status"] = "skipped"
            base_result["reason"] = "already_installed"
            base_result["message"] = f"{package_label} 已安装或已存在更高版本，已跳过"
            return base_result

        base_result["status"] = "failed"
        base_result["reason"] = "installer_failed"
        base_result["message"] = f"{package_label} 安装失败（退出码 {return_code}）"
        return base_result

    async def get_runtime_repair_status(self) -> Dict[str, Any]:
        """获取运行库修复任务状态。"""
        return {"state": await self._get_runtime_repair_state_snapshot()}

    async def list_runtime_repair_candidates(self) -> Dict[str, Any]:
        """列出可用于运行库修复的已安装 PC 游戏。"""
        rows = await asyncio.to_thread(self._collect_runtime_repair_candidates)
        candidates = [
            {
                "game_id": str(item.get("game_id", "") or ""),
                "title": str(item.get("title", "") or ""),
                "install_path": str(item.get("install_path", "") or ""),
                "prefix_ready": bool(item.get("prefix_ready")),
                "prefix_message": str(item.get("prefix_message", "") or ""),
                "steam_app_id": max(0, _safe_int(item.get("steam_app_id"), 0)),
                "proton_tool": str(item.get("proton_tool", "") or ""),
            }
            for item in rows
        ]
        return {
            "games": candidates,
            "ready_count": sum(1 for item in candidates if bool(item.get("prefix_ready"))),
            "total_count": len(candidates),
        }

    async def list_runtime_repair_packages(self) -> Dict[str, Any]:
        """列出支持的运行库包。"""
        steam_root = self._runtime_repair_find_steam_root()
        rows = await asyncio.to_thread(self._runtime_repair_package_definitions, steam_root)
        packages = [
            {
                "package_id": str(item.get("package_id", "") or ""),
                "label": str(item.get("label", "") or ""),
                "description": str(item.get("description", "") or ""),
                "default_selected": bool(item.get("default_selected")),
                "global_available": bool(item.get("global_available")),
                "size_bytes": max(0, _safe_int(item.get("size_bytes"), 0)),
                "source_hint": str(item.get("source_hint", "") or ""),
            }
            for item in rows
        ]
        return {
            "packages": packages,
            "default_package_ids": [str(item.get("package_id", "") or "") for item in packages if bool(item.get("default_selected"))],
        }

    async def start_runtime_repair(
        self,
        *,
        game_ids: Optional[Sequence[str]] = None,
        package_ids: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """启动运行库修复任务。"""
        normalized_game_ids: List[str] = []
        seen_game_ids: Set[str] = set()
        for raw in list(game_ids or []):
            game_id = str(raw or "").strip()
            if not game_id or game_id in seen_game_ids:
                continue
            seen_game_ids.add(game_id)
            normalized_game_ids.append(game_id)

        normalized_package_ids: List[str] = []
        seen_package_ids: Set[str] = set()
        for raw in list(package_ids or []):
            package_id = str(raw or "").strip()
            if not package_id or package_id in seen_package_ids:
                continue
            seen_package_ids.add(package_id)
            normalized_package_ids.append(package_id)

        if not normalized_game_ids:
            raise ValueError("请先至少选择一个 PC 游戏")
        if not normalized_package_ids:
            raise ValueError("请先至少选择一个运行库包")

        candidate_rows = await asyncio.to_thread(self._collect_runtime_repair_candidates)
        candidate_map = {str(item.get("game_id", "") or ""): item for item in candidate_rows}
        selected_games: List[Dict[str, Any]] = []
        blocked_titles: List[str] = []
        for game_id in normalized_game_ids:
            item = candidate_map.get(game_id)
            if not isinstance(item, dict):
                continue
            if not bool(item.get("prefix_ready")):
                blocked_titles.append(str(item.get("title", game_id) or game_id))
                continue
            selected_games.append(item)

        if not selected_games:
            if blocked_titles:
                raise ValueError(f"所选游戏尚未生成 compatdata：{blocked_titles[0]}")
            raise ValueError("未找到可修复的已安装 PC 游戏")

        package_rows = await asyncio.to_thread(self._runtime_repair_package_definitions, self._runtime_repair_find_steam_root())
        package_map = {str(item.get("package_id", "") or ""): item for item in package_rows}
        selected_packages = [package_map[item] for item in normalized_package_ids if item in package_map]
        if not selected_packages:
            raise ValueError("未找到可用的运行库包")

        async with self._runtime_repair_lock:
            current_task = self._runtime_repair_task
            if current_task and not current_task.done():
                return {
                    "started": False,
                    "message": "已有运行库修复任务正在执行",
                    "state": self._runtime_repair_state_snapshot_locked(),
                }

            self._runtime_repair_state = self._new_runtime_repair_state()
            self._runtime_repair_state.update(
                {
                    "stage": "running",
                    "message": "运行库修复任务已启动",
                    "reason": "",
                    "running": True,
                    "progress": 0.0,
                    "total_games": len(selected_games),
                    "processed_games": 0,
                    "total_steps": len(selected_games) * len(selected_packages),
                    "completed_steps": 0,
                    "succeeded_steps": 0,
                    "skipped_steps": 0,
                    "failed_steps": 0,
                    "current_game_id": "",
                    "current_game_title": "",
                    "current_package_id": "",
                    "current_package_label": "",
                    "results": [],
                    "diagnostics": {
                        "selected_game_ids": [str(item.get("game_id", "") or "") for item in selected_games],
                        "selected_package_ids": [str(item.get("package_id", "") or "") for item in selected_packages],
                        "blocked_games": blocked_titles,
                    },
                    "updated_at": _now_wall_ts(),
                }
            )
            self._runtime_repair_task = asyncio.create_task(
                self._run_runtime_repair_task(selected_games=selected_games, selected_packages=selected_packages),
                name="freedeck_runtime_repair",
            )
            snapshot = self._runtime_repair_state_snapshot_locked()

        message = "运行库修复任务已启动"
        if blocked_titles:
            message = f"{message}（已跳过 {len(blocked_titles)} 个未生成 compatdata 的游戏）"
        return {"started": True, "message": message, "state": snapshot}

    async def _run_runtime_repair_task(
        self,
        *,
        selected_games: Sequence[Dict[str, Any]],
        selected_packages: Sequence[Dict[str, Any]],
    ) -> None:
        """顺序执行多游戏、多运行库修复。"""
        current = asyncio.current_task()
        started_at = _now_wall_ts()
        started_monotonic = time.monotonic()
        total_games = len(selected_games)
        total_steps = max(0, len(selected_games) * len(selected_packages))
        results: List[Dict[str, Any]] = []
        completed_steps = 0
        succeeded_steps = 0
        skipped_steps = 0
        failed_steps = 0
        processed_games = 0

        try:
            for game in list(selected_games or []):
                game_title = str(game.get("title", "") or "")
                game_id = str(game.get("game_id", "") or "")
                for package in list(selected_packages or []):
                    package_id = str(package.get("package_id", "") or "")
                    package_label = str(package.get("label", package_id) or package_id)
                    pending_progress = ((completed_steps + 0.2) / total_steps) * 100.0 if total_steps > 0 else 0.0
                    await self._set_runtime_repair_state(
                        stage="running",
                        running=True,
                        message=f"正在为「{game_title or game_id}」安装 {package_label}",
                        current_game_id=game_id,
                        current_game_title=game_title,
                        current_package_id=package_id,
                        current_package_label=package_label,
                        progress=pending_progress,
                        processed_games=processed_games,
                        completed_steps=completed_steps,
                        succeeded_steps=succeeded_steps,
                        skipped_steps=skipped_steps,
                        failed_steps=failed_steps,
                        results=results,
                    )

                    result = await self._execute_runtime_repair_step(game=game, package=package)
                    results.append(result)
                    status = str(result.get("status", "") or "").strip().lower()
                    if status == "success":
                        succeeded_steps += 1
                    elif status == "skipped":
                        skipped_steps += 1
                    else:
                        failed_steps += 1
                    completed_steps += 1
                    progress = ((completed_steps) / total_steps) * 100.0 if total_steps > 0 else 100.0
                    await self._set_runtime_repair_state(
                        stage="running",
                        running=True,
                        message=str(result.get("message", "") or f"{package_label} 已处理"),
                        current_game_id=game_id,
                        current_game_title=game_title,
                        current_package_id=package_id,
                        current_package_label=package_label,
                        progress=progress,
                        processed_games=processed_games,
                        completed_steps=completed_steps,
                        succeeded_steps=succeeded_steps,
                        skipped_steps=skipped_steps,
                        failed_steps=failed_steps,
                        results=results,
                    )

                processed_games += 1
                await self._set_runtime_repair_state(
                    processed_games=processed_games,
                    current_game_id="",
                    current_game_title="",
                    current_package_id="",
                    current_package_label="",
                )

            duration_seconds = max(0, int(time.monotonic() - started_monotonic))
            if failed_steps <= 0:
                final_stage = "completed"
                final_reason = ""
                final_message = f"运行库修复完成：成功 {succeeded_steps}，跳过 {skipped_steps}"
            elif succeeded_steps > 0 or skipped_steps > 0:
                final_stage = "completed"
                final_reason = "partial_failures"
                final_message = f"运行库修复完成：成功 {succeeded_steps}，跳过 {skipped_steps}，失败 {failed_steps}"
            else:
                final_stage = "failed"
                final_reason = "all_failed"
                final_message = f"运行库修复失败：共 {failed_steps} 个步骤失败"

            final_payload = {
                "stage": final_stage,
                "reason": final_reason,
                "message": final_message,
                "started_at": started_at,
                "finished_at": _now_wall_ts(),
                "duration_seconds": duration_seconds,
                "total_games": total_games,
                "processed_games": processed_games,
                "total_steps": total_steps,
                "completed_steps": completed_steps,
                "succeeded_steps": succeeded_steps,
                "skipped_steps": skipped_steps,
                "failed_steps": failed_steps,
                "results": [self._copy_runtime_repair_result(item) for item in results],
            }
            await asyncio.to_thread(self.store.set_runtime_repair_last_result, final_payload)
            await self._set_runtime_repair_state(
                stage=final_stage,
                reason=final_reason,
                message=final_message,
                running=False,
                progress=100.0 if total_steps > 0 else 0.0,
                total_games=total_games,
                processed_games=processed_games,
                total_steps=total_steps,
                completed_steps=completed_steps,
                succeeded_steps=succeeded_steps,
                skipped_steps=skipped_steps,
                failed_steps=failed_steps,
                current_game_id="",
                current_game_title="",
                current_package_id="",
                current_package_label="",
                results=results,
                last_result=final_payload,
            )
        except Exception as exc:
            config.logger.exception("Runtime repair task failed: %s", exc)
            duration_seconds = max(0, int(time.monotonic() - started_monotonic))
            final_payload = {
                "stage": "failed",
                "reason": "exception",
                "message": f"运行库修复异常中断：{exc}",
                "started_at": started_at,
                "finished_at": _now_wall_ts(),
                "duration_seconds": duration_seconds,
                "total_games": total_games,
                "processed_games": processed_games,
                "total_steps": total_steps,
                "completed_steps": completed_steps,
                "succeeded_steps": succeeded_steps,
                "skipped_steps": skipped_steps,
                "failed_steps": failed_steps,
                "results": [self._copy_runtime_repair_result(item) for item in results],
            }
            await asyncio.to_thread(self.store.set_runtime_repair_last_result, final_payload)
            await self._set_runtime_repair_state(
                stage="failed",
                reason="exception",
                message=f"运行库修复异常中断：{exc}",
                running=False,
                progress=((completed_steps) / total_steps) * 100.0 if total_steps > 0 else 0.0,
                total_games=total_games,
                processed_games=processed_games,
                total_steps=total_steps,
                completed_steps=completed_steps,
                succeeded_steps=succeeded_steps,
                skipped_steps=skipped_steps,
                failed_steps=max(failed_steps, 1),
                current_game_id="",
                current_game_title="",
                current_package_id="",
                current_package_label="",
                results=results,
                last_result=final_payload,
            )
        finally:
            async with self._runtime_repair_lock:
                if self._runtime_repair_task is current:
                    self._runtime_repair_task = None

    def _extract_proton_relative_path(self, *, source_path: str, archive_rel_path: str = "") -> str:
        """从 source_path / archive_rel_path 提取 Proton 用户目录下的相对路径。"""

        patterns: Tuple[Tuple[str, ...], ...] = (
            ("Documents", "My Games"),
            ("Saved Games",),
            ("AppData", "Roaming"),
            ("AppData", "LocalLow"),
            ("AppData", "Local"),
        )

        def _match(parts: Sequence[str]) -> str:
            seq = [str(item or "").strip() for item in list(parts or []) if str(item or "").strip()]
            if not seq:
                return ""
            lowered = [item.lower() for item in seq]
            for pattern in patterns:
                token = [str(item or "").strip().lower() for item in pattern if str(item or "").strip()]
                if not token:
                    continue
                max_start = len(lowered) - len(token)
                for idx in range(max(0, max_start + 1)):
                    if lowered[idx: idx + len(token)] == token:
                        return "/".join(seq[idx:])
            return ""

        source_parts = [part for part in Path(self._normalize_dir_path(source_path)).parts if str(part or "").strip()]
        matched = _match(source_parts)
        if matched:
            return matched

        rel_parts = [
            part
            for part in str(archive_rel_path or "").replace("\\", "/").split("/")
            if part and part not in {".", ".."}
        ]
        return _match(rel_parts)

    def _resolve_current_compat_user_dir(self, game_id: str) -> str:
        """解析当前游戏对应的 Proton 用户目录。"""
        target_game_id = str(game_id or "").strip()
        if not target_game_id:
            return ""
        shortcut_result: Dict[str, Any] = {}
        try:
            shortcut_result = resolve_tianyi_shortcut_sync(game_id=target_game_id)
        except Exception:
            shortcut_result = {"ok": False, "message": "resolve_failed"}

        matched: Optional[TianyiInstalledGame] = None
        for record in list(self.store.installed_games or []):
            if str(record.game_id or "").strip() == target_game_id:
                matched = record
                break
        if matched is not None:
            compat_user_dir, _diag = self._resolve_compat_user_dir_for_record(
                record=matched,
                shortcut_result=shortcut_result,
            )
            return compat_user_dir

        return self._normalize_existing_dir(str(shortcut_result.get("compat_user_dir", "") or ""))

    def _find_steam_root_for_cloud_save(self) -> str:
        """在当前环境中推断 Steam root（含 steamapps 的目录）。"""
        homes: List[str] = []
        home_candidates = [
            str(getattr(decky, "DECKY_USER_HOME", "") or "").strip(),
            str(os.environ.get("DECKY_USER_HOME", "") or "").strip(),
            str(Path.home()),
            str(os.environ.get("HOME", "") or "").strip(),
            "/home/deck",
        ]
        for value in home_candidates:
            if not value:
                continue
            try:
                resolved = os.path.realpath(os.path.expanduser(value))
            except Exception:
                continue
            if resolved and resolved not in homes:
                homes.append(resolved)

        candidates: List[str] = []
        for home in homes:
            candidates.append(os.path.join(home, ".steam", "steam"))
            candidates.append(os.path.join(home, ".local", "share", "Steam"))

        for item in candidates:
            steamapps = os.path.join(item, "steamapps")
            if os.path.isdir(steamapps):
                return os.path.realpath(item)
        return ""

    def _resolve_compat_user_dir_for_record(
        self,
        *,
        record: TianyiInstalledGame,
        shortcut_result: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        """尝试从快捷方式/历史 appid 推断 compat user 目录。"""
        compat_user_dir = self._normalize_existing_dir(str(shortcut_result.get("compat_user_dir", "") or ""))
        diagnostics: Dict[str, Any] = {
            "strategy": "shortcut" if compat_user_dir else "",
            "steam_root": str(shortcut_result.get("steam_root", "") or "").strip(),
            "appid_unsigned": max(0, _safe_int(shortcut_result.get("appid_unsigned"), 0)),
            "compat_user_dir": str(shortcut_result.get("compat_user_dir", "") or ""),
            "reason": "",
        }
        if compat_user_dir:
            return compat_user_dir, diagnostics

        steam_root = diagnostics["steam_root"]
        if not steam_root:
            steam_root = self._find_steam_root_for_cloud_save()
            diagnostics["steam_root"] = steam_root

        app_id_unsigned = diagnostics["appid_unsigned"]
        if app_id_unsigned <= 0:
            app_id_unsigned = max(0, _safe_int(record.playtime_active_app_id, 0))
            if app_id_unsigned <= 0:
                app_id_unsigned = max(0, _safe_int(record.steam_app_id, 0))
            diagnostics["appid_unsigned"] = app_id_unsigned

        if not steam_root:
            diagnostics["reason"] = "steam_root_missing"
            return "", diagnostics
        if app_id_unsigned <= 0:
            diagnostics["reason"] = "appid_missing"
            return "", diagnostics

        compat_root = os.path.join(
            steam_root,
            "steamapps",
            "compatdata",
            str(app_id_unsigned),
            "pfx",
            "drive_c",
            "users",
        )
        diagnostics["compat_root"] = compat_root
        if not os.path.isdir(compat_root):
            diagnostics["reason"] = "compat_root_missing"
            return "", diagnostics

        named_candidates = [
            os.path.join(compat_root, "steamuser"),
            os.path.join(compat_root, "deck"),
        ]
        diagnostics["compat_candidates"] = [os.path.realpath(path) for path in named_candidates]
        for candidate in named_candidates:
            if os.path.isdir(candidate):
                diagnostics["strategy"] = "compat_named_user"
                return os.path.realpath(candidate), diagnostics

        extra_candidates: List[str] = []
        try:
            with os.scandir(compat_root) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        extra_candidates.append(entry.path)
        except Exception:
            extra_candidates = []

        if not extra_candidates:
            diagnostics["reason"] = "compat_user_missing"
            return "", diagnostics

        ignored = {"public", "default", "default user", "all users"}

        def score(path: str) -> Tuple[int, str]:
            name = os.path.basename(path).strip()
            lowered = name.lower()
            points = 0
            if lowered in ignored:
                points -= 50
            if os.path.isdir(os.path.join(path, "AppData")):
                points += 20
            if os.path.isdir(os.path.join(path, "Documents")):
                points += 10
            if os.path.isdir(os.path.join(path, "Saved Games")):
                points += 10
            if "user" in lowered:
                points += 2
            return points, lowered

        extra_candidates.sort(key=lambda item: score(item), reverse=True)
        diagnostics["strategy"] = "compat_scanned_user"
        diagnostics["compat_extra_candidates"] = [os.path.realpath(path) for path in extra_candidates]
        return os.path.realpath(extra_candidates[0]), diagnostics

    def _build_cloud_save_game_key(self, game_id: str, game_title: str) -> str:
        """生成稳定 game-key。"""
        token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_id or "").strip()).strip("_")
        if token:
            return token.lower()

        title_token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_title or "").strip()).strip("_")
        if title_token:
            return title_token.lower()

        return f"game_{_now_wall_ts()}"

    def _parse_cloud_save_version_timestamp(self, version_name: str) -> int:
        """从云端版本名中提取时间戳（秒）。"""
        raw_name = str(version_name or "").strip()
        if not raw_name:
            return 0
        stem = raw_name
        if stem.lower().endswith(".7z"):
            stem = stem[:-3]
        stem = stem.strip()
        if not re.fullmatch(r"\d{8}_\d{6}", stem):
            return 0
        try:
            return int(time.mktime(time.strptime(stem, CLOUD_SAVE_DATE_FORMAT)))
        except Exception:
            return 0

    def _format_cloud_save_version_time(self, ts: int, fallback: str) -> str:
        """格式化版本时间显示。"""
        value = _safe_int(ts, 0)
        if value <= 0:
            return str(fallback or "")
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))
        except Exception:
            return str(fallback or "")

    def _collect_cloud_restore_games(self) -> List[Dict[str, Any]]:
        """按 game_key 聚合当前已安装游戏。"""
        records = list(self.store.installed_games or [])
        records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)

        grouped: Dict[str, Dict[str, Any]] = {}
        for record in records:
            game_id = str(record.game_id or "").strip()
            game_title = str(record.game_title or "").strip() or "未命名游戏"
            game_key = self._build_cloud_save_game_key(game_id, game_title)
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            if not install_path:
                continue

            item = grouped.get(game_key)
            if item is None:
                item = {
                    "game_id": game_id,
                    "game_title": game_title,
                    "game_key": game_key,
                    "install_paths": [],
                    "updated_at": _safe_int(record.updated_at, 0),
                }
                grouped[game_key] = item

            paths = item.get("install_paths")
            if not isinstance(paths, list):
                paths = []
                item["install_paths"] = paths
            if install_path not in paths:
                paths.append(install_path)

            if game_id and not str(item.get("game_id", "") or "").strip():
                item["game_id"] = game_id
            if _safe_int(record.updated_at, 0) > _safe_int(item.get("updated_at"), 0):
                item["updated_at"] = _safe_int(record.updated_at, 0)
                if game_title:
                    item["game_title"] = game_title
                if game_id:
                    item["game_id"] = game_id

        games = list(grouped.values())
        games.sort(
            key=lambda row: (
                -_safe_int(row.get("updated_at"), 0),
                str(row.get("game_title", "") or "").lower(),
                str(row.get("game_key", "") or ""),
            )
        )
        return games

    def _normalize_cloud_restore_entries(self, *, manifest: Dict[str, Any], extract_dir: str) -> List[Dict[str, Any]]:
        """从 manifest 或归档目录生成可选存档项。"""
        result: List[Dict[str, Any]] = []
        extract_root = self._normalize_existing_dir(extract_dir)
        if not extract_root:
            return result

        entries = manifest.get("entries")
        if isinstance(entries, list):
            for idx, item in enumerate(entries, start=1):
                if not isinstance(item, dict):
                    continue
                entry_id = str(item.get("entry_id", "") or "").strip() or f"entry_{idx}"
                entry_name = str(item.get("name", "") or item.get("entry_name", "") or "").strip() or entry_id
                rel_path = str(item.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                if not rel_path:
                    rel_path = str(item.get("relative_path", "") or "").strip().replace("\\", "/").strip("/")
                abs_path = os.path.realpath(os.path.join(extract_root, rel_path)) if rel_path else extract_root
                if not abs_path.startswith(extract_root):
                    continue
                if not os.path.exists(abs_path):
                    continue
                result.append(
                    {
                        "entry_id": entry_id,
                        "entry_name": entry_name,
                        "archive_rel_path": rel_path,
                        "source_path": str(item.get("source_path", "") or ""),
                        "platform": str(item.get("platform", "") or manifest.get("platform", "") or ""),
                        "emulator_id": str(item.get("emulator_id", "") or manifest.get("emulator_id", "") or ""),
                        "title_id": str(item.get("title_id", "") or manifest.get("switch_title_id", "") or ""),
                        "profile_id": str(item.get("profile_id", "") or ""),
                        "user_id": str(item.get("user_id", "") or ""),
                        "relative_path": str(item.get("relative_path", "") or rel_path),
                        "relative_root": str(item.get("relative_root", "") or ""),
                    }
                )

        if result:
            return result

        source_paths = manifest.get("source_paths")
        if isinstance(source_paths, list):
            for idx, raw in enumerate(source_paths, start=1):
                source_path = str(raw or "").strip()
                if not source_path:
                    continue
                name = os.path.basename(source_path.rstrip("/\\")).strip() or f"entry_{idx}"
                result.append(
                    {
                        "entry_id": f"entry_{idx}",
                        "entry_name": name,
                        "archive_rel_path": "",
                        "source_path": source_path,
                        "platform": str(manifest.get("platform", "") or ""),
                        "emulator_id": str(manifest.get("emulator_id", "") or ""),
                        "title_id": str(manifest.get("switch_title_id", "") or ""),
                        "profile_id": "",
                        "user_id": "",
                        "relative_path": "",
                        "relative_root": "",
                    }
                )

        if result:
            fallback_entries: List[Dict[str, Any]] = []
            try:
                roots = sorted(os.listdir(extract_root))
            except Exception:
                roots = []
            for idx, name in enumerate(roots, start=1):
                root_name = str(name or "").strip()
                if not root_name or root_name == "manifest.json":
                    continue
                abs_path = os.path.join(extract_root, root_name)
                if not os.path.exists(abs_path):
                    continue
                fallback_entries.append(
                    {
                        "entry_id": f"entry_{idx}",
                        "entry_name": root_name,
                        "archive_rel_path": root_name.replace("\\", "/"),
                        "source_path": "",
                        "platform": str(manifest.get("platform", "") or ""),
                        "emulator_id": str(manifest.get("emulator_id", "") or ""),
                        "title_id": str(manifest.get("switch_title_id", "") or ""),
                        "profile_id": "",
                        "user_id": "",
                        "relative_path": root_name.replace("\\", "/"),
                        "relative_root": "",
                    }
                )
            if fallback_entries:
                return fallback_entries

        return [
            {
                "entry_id": "entry_all",
                "entry_name": "全部存档",
                "archive_rel_path": "",
                "source_path": "",
                "platform": str(manifest.get("platform", "") or ""),
                "emulator_id": str(manifest.get("emulator_id", "") or ""),
                "title_id": str(manifest.get("switch_title_id", "") or ""),
                "profile_id": "",
                "user_id": "",
                "relative_path": "",
                "relative_root": "",
            }
        ]

    def _extract_manifest_playtime_payload(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """从云存档 manifest 中提取游玩时长信息。"""
        if not isinstance(manifest, dict):
            return {}

        playtime_raw = manifest.get("playtime")
        playtime = playtime_raw if isinstance(playtime_raw, dict) else {}

        seconds = max(0, _safe_int(playtime.get("seconds"), _safe_int(manifest.get("playtime_seconds"), 0)))
        sessions = max(0, _safe_int(playtime.get("sessions"), _safe_int(manifest.get("playtime_sessions"), 0)))
        last_played_at = max(
            0,
            _safe_int(playtime.get("last_played_at"), _safe_int(manifest.get("playtime_last_played_at"), 0)),
        )
        captured_at = max(
            0,
            _safe_int(playtime.get("captured_at"), _safe_int(manifest.get("playtime_captured_at"), 0)),
        )

        if seconds <= 0 and sessions <= 0 and last_played_at <= 0:
            return {}

        return {
            "seconds": seconds,
            "sessions": sessions,
            "last_played_at": last_played_at,
            "captured_at": captured_at,
        }

    async def _merge_cloud_restore_playtime(
        self,
        *,
        game_id: str,
        game_key: str,
        target_dir: str,
        manifest_playtime: Dict[str, Any],
    ) -> Dict[str, Any]:
        """将云存档中的游玩时长合并到本地记录（不降级覆盖）。"""
        payload = dict(manifest_playtime or {})
        if not payload:
            return {"merged": False, "reason": "playtime_missing", "message": "manifest 未包含游玩时长"}

        cloud_seconds = max(0, _safe_int(payload.get("seconds"), 0))
        cloud_sessions = max(0, _safe_int(payload.get("sessions"), 0))
        cloud_last_played_at = max(0, _safe_int(payload.get("last_played_at"), 0))
        if cloud_seconds <= 0 and cloud_sessions <= 0 and cloud_last_played_at <= 0:
            return {"merged": False, "reason": "playtime_empty", "message": "云端游玩时长为空"}

        target_game_id = str(game_id or "").strip()
        target_game_key = str(game_key or "").strip()
        normalized_target_dir = self._normalize_dir_path(str(target_dir or "").strip())

        record = self._find_installed_record(game_id=target_game_id, install_path=normalized_target_dir)
        if record is None and target_game_id:
            record = self._find_installed_record(game_id=target_game_id)
        if record is None and target_game_key:
            for item in list(self.store.installed_games or []):
                item_key = self._build_cloud_save_game_key(
                    str(item.game_id or "").strip(),
                    str(item.game_title or "").strip(),
                )
                if item_key == target_game_key:
                    record = item
                    break

        if record is None:
            return {"merged": False, "reason": "record_not_found", "message": "未找到本地安装记录"}

        local_seconds_before = max(0, _safe_int(record.playtime_seconds, 0))
        local_sessions_before = max(0, _safe_int(record.playtime_sessions, 0))
        local_last_played_before = max(0, _safe_int(record.playtime_last_played_at, 0))

        local_seconds_after = max(local_seconds_before, cloud_seconds)
        local_sessions_after = max(local_sessions_before, cloud_sessions)
        local_last_played_after = max(local_last_played_before, cloud_last_played_at)
        changed = (
            local_seconds_after != local_seconds_before
            or local_sessions_after != local_sessions_before
            or local_last_played_after != local_last_played_before
        )

        if not changed:
            return {
                "merged": False,
                "reason": "already_up_to_date",
                "message": "本地游玩时长不低于云端",
                "local_seconds": local_seconds_before,
                "cloud_seconds": cloud_seconds,
            }

        now_ts = _now_wall_ts()
        record.playtime_seconds = local_seconds_after
        record.playtime_sessions = local_sessions_after
        record.playtime_last_played_at = local_last_played_after
        record.updated_at = now_ts
        await asyncio.to_thread(self.store.save)
        self._invalidate_panel_cache(installed=True)
        return {
            "merged": True,
            "reason": "",
            "message": "已合并云端游玩时长",
            "local_seconds_before": local_seconds_before,
            "local_seconds_after": local_seconds_after,
            "cloud_seconds": cloud_seconds,
        }

    def _build_cloud_save_match_tokens(
        self,
        *,
        game_id: str,
        game_title: str,
        install_path: str,
        source_path: str = "",
        exe_path: str = "",
    ) -> List[str]:
        """构建存档目录匹配词元。"""
        normalized_exe = str(exe_path or "").strip()
        exe_stem = ""
        if normalized_exe:
            try:
                exe_stem = os.path.splitext(os.path.basename(normalized_exe))[0]
            except Exception:
                exe_stem = ""

        source_basename = os.path.basename(str(source_path or "").strip())
        source_stem = ""
        if source_basename:
            try:
                source_stem = os.path.splitext(source_basename)[0]
            except Exception:
                source_stem = source_basename

        raw_values = [
            str(game_id or "").strip(),
            str(game_title or "").strip(),
            os.path.basename(str(install_path or "").strip()),
            str(source_stem or "").strip(),
            str(exe_stem or "").strip(),
        ]

        token_set = set()
        for raw in raw_values:
            if not raw:
                continue
            lower = raw.lower()
            compact = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", lower)
            if len(compact) >= 3:
                token_set.add(compact)
            for part in re.split(r"[^0-9a-z\u4e00-\u9fff]+", lower):
                text = str(part or "").strip()
                if len(text) >= 2:
                    token_set.add(text)

        return sorted(token_set, key=lambda item: (-len(item), item))

    def _dedupe_existing_paths(self, paths: Sequence[str]) -> List[str]:
        """目录去重并去除被父目录覆盖的子目录。"""
        return self._dedupe_paths(paths, require_existing=True)

    def _should_keep_cloud_save_dir(
        self,
        *,
        root: str,
        current: str,
        tokens: Sequence[str],
        keywords: Sequence[str],
    ) -> bool:
        """判断当前目录是否应作为存档候选。"""
        root_dir = str(root or "").strip()
        current_dir = str(current or "").strip()
        if not root_dir or not current_dir:
            return False
        if current_dir == root_dir:
            return False

        name = os.path.basename(current_dir).lower()
        try:
            rel = os.path.relpath(current_dir, root_dir).replace("\\", "/").lower()
        except Exception:
            rel = name

        token_hit = False
        for token in list(tokens or []):
            text = str(token or "").strip().lower()
            if len(text) < 2:
                continue
            if text in name or text in rel:
                token_hit = True
                break

        keyword_hit = False
        for keyword in list(keywords or []):
            text = str(keyword or "").strip().lower()
            if len(text) < 2:
                continue
            if text in name or text in rel:
                keyword_hit = True
                break

        if not token_hit and not keyword_hit:
            return False

        try:
            with os.scandir(current_dir) as it:
                for _ in it:
                    return True
        except Exception:
            return False
        return False

    def _scan_cloud_save_paths(
        self,
        *,
        root: str,
        tokens: Sequence[str],
        keywords: Sequence[str],
    ) -> List[str]:
        """在限定深度内扫描存档候选目录。"""
        root_dir = self._normalize_existing_dir(root)
        if not root_dir:
            return []

        base_depth = root_dir.count(os.sep)
        matches: List[str] = []
        for dirpath, dirnames, _filenames in os.walk(root_dir):
            depth = max(0, dirpath.count(os.sep) - base_depth)
            if depth >= CLOUD_SAVE_SCAN_MAX_DEPTH:
                dirnames[:] = []

            if self._should_keep_cloud_save_dir(
                root=root_dir,
                current=dirpath,
                tokens=tokens,
                keywords=keywords,
            ):
                matches.append(dirpath)
                if len(matches) >= CLOUD_SAVE_SCAN_MAX_MATCHES:
                    break

        return self._dedupe_existing_paths(matches)

    def _collect_cloud_save_paths_from_proton(
        self,
        *,
        compat_user_dir: str,
        tokens: Sequence[str],
    ) -> Tuple[List[str], Dict[str, Any]]:
        """从 Proton 前缀白名单目录采集存档路径。"""
        root = self._normalize_existing_dir(compat_user_dir)
        diagnostics: Dict[str, Any] = {"compat_user_dir": root, "scanned_bases": [], "matched": []}
        if not root:
            diagnostics["reason"] = "compat_user_dir_missing"
            return [], diagnostics

        collected: List[str] = []
        for parts in CLOUD_SAVE_PROTON_BASE_DIRS:
            base = os.path.join(root, *parts)
            if not os.path.isdir(base):
                continue
            diagnostics["scanned_bases"].append(base)
            collected.extend(self._scan_cloud_save_paths(root=base, tokens=tokens, keywords=()))

        merged = self._dedupe_existing_paths(collected)
        diagnostics["matched"] = list(merged)
        if not merged:
            diagnostics["reason"] = "save_path_not_found"
        return merged, diagnostics

    def _collect_cloud_save_paths_from_install(
        self,
        *,
        install_path: str,
        tokens: Sequence[str],
    ) -> Tuple[List[str], Dict[str, Any]]:
        """在安装目录白名单兜底采集存档路径。"""
        root = self._normalize_existing_dir(install_path)
        diagnostics: Dict[str, Any] = {
            "install_path": root,
            "keywords": list(CLOUD_SAVE_INSTALL_FALLBACK_DIRS),
            "matched": [],
        }
        if not root:
            diagnostics["reason"] = "install_path_missing"
            return [], diagnostics

        collected = self._scan_cloud_save_paths(
            root=root,
            tokens=tokens,
            keywords=CLOUD_SAVE_INSTALL_FALLBACK_DIRS,
        )
        diagnostics["matched"] = list(collected)
        if not collected:
            diagnostics["reason"] = "save_path_not_found"
        return collected, diagnostics

    async def _download_and_extract_cloud_restore_version(
        self,
        *,
        cookie: str,
        game_key: str,
        version_name: str,
    ) -> Dict[str, Any]:
        """下载并解压指定云存档版本，返回清单与临时路径。"""
        normalized_key = str(game_key or "").strip()
        normalized_version = str(version_name or "").strip()
        if not normalized_key:
            raise TianyiApiError("缺少 game_key")
        if not normalized_version or not normalized_version.lower().endswith(".7z"):
            raise TianyiApiError("版本文件无效，仅支持 .7z")

        listed = await list_cloud_archives(cookie=cookie, remote_folder_parts=[normalized_key])
        files = listed.get("files")
        if not isinstance(files, list):
            files = []

        target_file: Dict[str, Any] = {}
        for item in files:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "") or "").strip()
            if name == normalized_version:
                target_file = dict(item)
                break
        if not target_file:
            raise TianyiApiError("未找到指定版本，请刷新后重试")

        file_id = str(target_file.get("file_id", "") or "").strip()
        if not file_id:
            raise TianyiApiError("云端版本缺少 file_id")

        temp_dir = tempfile.mkdtemp(prefix=f"freedeck_cloudrestore_{normalized_key}_")
        archive_path = os.path.join(temp_dir, normalized_version)
        extract_dir = os.path.join(temp_dir, "extracted")
        manifest_path = os.path.join(extract_dir, "manifest.json")
        os.makedirs(extract_dir, exist_ok=True)
        try:
            await download_cloud_archive(
                cookie=cookie,
                file_id=file_id,
                local_file_path=archive_path,
            )
            await asyncio.to_thread(self.seven_zip.extract_archive, archive_path, extract_dir)

            manifest: Dict[str, Any] = {}
            if os.path.isfile(manifest_path):
                try:
                    with open(manifest_path, "r", encoding="utf-8") as fp:
                        loaded = json.load(fp)
                    if isinstance(loaded, dict):
                        manifest = loaded
                except Exception:
                    manifest = {}

            entries = self._normalize_cloud_restore_entries(manifest=manifest, extract_dir=extract_dir)
            return {
                "temp_dir": temp_dir,
                "archive_path": archive_path,
                "extract_dir": extract_dir,
                "manifest_path": manifest_path,
                "manifest": manifest,
                "entries": entries,
                "version_name": normalized_version,
                "game_key": normalized_key,
                "file_id": file_id,
                "cloud_list": listed,
            }
        except Exception:
            self._cleanup_cloud_save_temp_paths([temp_dir])
            raise

    def _resolve_cloud_restore_target_candidates(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        entries: Sequence[Dict[str, Any]] = (),
    ) -> Dict[str, Any]:
        """为恢复流程解析当前可用目标目录候选。"""
        records = list(self.store.installed_games or [])
        matched_records: List[TianyiInstalledGame] = []
        for record in records:
            record_game_id = str(record.game_id or "").strip()
            record_title = str(record.game_title or "").strip() or "未命名游戏"
            record_key = self._build_cloud_save_game_key(record_game_id, record_title)
            if game_id and record_game_id and game_id == record_game_id:
                matched_records.append(record)
                continue
            if game_key and record_key and game_key == record_key:
                matched_records.append(record)
                continue

        matched_records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)

        candidates: List[str] = []
        diagnostics: Dict[str, Any] = {
            "matched_records": [],
            "proton_scans": [],
            "fallback_scans": [],
        }

        for record in matched_records:
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            if not install_path:
                continue

            record_game_id = str(record.game_id or "").strip() or str(game_id or "").strip()
            record_title = str(record.game_title or "").strip() or str(game_title or "").strip() or "未命名游戏"

            shortcut_result: Dict[str, Any] = {}
            if record_game_id:
                try:
                    shortcut_result = resolve_tianyi_shortcut_sync(game_id=record_game_id)
                except Exception as exc:
                    shortcut_result = {"ok": False, "message": str(exc)}
            else:
                shortcut_result = {"ok": False, "message": "game_id_missing"}

            exe_path = str(shortcut_result.get("exe_path", "") or "").strip()
            tokens = self._build_cloud_save_match_tokens(
                game_id=record_game_id,
                game_title=record_title,
                install_path=install_path,
                source_path=str(record.source_path or "").strip(),
                exe_path=exe_path,
            )

            compat_user_dir, compat_diag = self._resolve_compat_user_dir_for_record(
                record=record,
                shortcut_result=shortcut_result,
            )

            diagnostics["matched_records"].append(
                {
                    "game_id": record_game_id,
                    "game_title": record_title,
                    "install_path": install_path,
                    "shortcut": {
                        "ok": bool(shortcut_result.get("ok")),
                        "message": str(shortcut_result.get("message", "") or ""),
                        "steam_root": str(shortcut_result.get("steam_root", "") or "").strip(),
                        "user_id": str(shortcut_result.get("user_id", "") or "").strip(),
                        "appid_unsigned": _safe_int(shortcut_result.get("appid_unsigned"), 0),
                        "app_name": str(shortcut_result.get("app_name", "") or ""),
                        "exe_path": exe_path,
                        "compat_user_dir": str(shortcut_result.get("compat_user_dir", "") or ""),
                        "compat_candidates": list(shortcut_result.get("compat_candidates") or []),
                    },
                    "compat": compat_diag,
                }
            )

            if compat_user_dir:
                paths, proton_diag = self._collect_cloud_save_paths_from_proton(
                    compat_user_dir=compat_user_dir,
                    tokens=tokens,
                )
                diagnostics["proton_scans"].append(proton_diag)
                candidates.extend(paths)

                inferred_candidates: List[str] = []
                for entry in list(entries or []):
                    if not isinstance(entry, dict):
                        continue
                    source_path = str(entry.get("source_path", "") or "").strip()
                    archive_rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                    relative = self._extract_proton_relative_path(
                        source_path=source_path,
                        archive_rel_path=archive_rel_path,
                    )
                    if not relative:
                        continue
                    inferred_candidates.append(
                        os.path.join(compat_user_dir, *[part for part in relative.split("/") if part]),
                    )

                if inferred_candidates:
                    candidates.extend(inferred_candidates)
                    diagnostics.setdefault("inferred_candidates", [])
                    diagnostics["inferred_candidates"].extend(
                        self._dedupe_paths(inferred_candidates, require_existing=False),
                    )
            else:
                paths, fallback_diag = self._collect_cloud_save_paths_from_install(
                    install_path=install_path,
                    tokens=tokens,
                )
                diagnostics["fallback_scans"].append(fallback_diag)
                candidates.extend(paths)

        merged = self._dedupe_paths(candidates, require_existing=False)
        diagnostics["candidate_count"] = len(merged)
        diagnostics["candidates"] = list(merged)
        return {
            "candidates": merged,
            "diagnostics": diagnostics,
        }

    def _manifest_is_switch_eden(
        self,
        *,
        manifest: Optional[Dict[str, Any]] = None,
        entries: Sequence[Dict[str, Any]] = (),
    ) -> bool:
        """判断云存档 manifest 是否来自 Switch/Eden。"""
        payload = manifest if isinstance(manifest, dict) else {}
        platform = str(payload.get("platform", "") or "").strip().lower()
        emulator_id = str(payload.get("emulator_id", "") or "").strip().lower()
        if platform == "switch" and (not emulator_id or emulator_id == "eden"):
            return True
        for item in list(entries or []):
            if not isinstance(item, dict):
                continue
            entry_platform = str(item.get("platform", "") or "").strip().lower()
            entry_emulator = str(item.get("emulator_id", "") or "").strip().lower()
            if entry_platform == "switch" and (not entry_emulator or entry_emulator == "eden"):
                return True
        return False

    def _resolve_switch_restore_save_base_candidates(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        manifest: Optional[Dict[str, Any]] = None,
        entries: Sequence[Dict[str, Any]] = (),
    ) -> Dict[str, Any]:
        """解析 Switch/Eden 恢复目标的 save 根目录候选。"""
        payload = manifest if isinstance(manifest, dict) else {}
        title_id = self._extract_switch_title_id(
            payload.get("switch_title_id"),
            *[item.get("title_id") for item in list(entries or []) if isinstance(item, dict)],
        )
        diagnostics: Dict[str, Any] = {
            "platform": "switch",
            "emulator_id": "eden",
            "title_id": title_id,
            "matched_records": [],
            "save_base_candidates": [],
        }

        records = list(self.store.installed_games or [])
        matched_records: List[TianyiInstalledGame] = []
        for record in records:
            record_game_id = str(record.game_id or "").strip()
            record_title = str(record.game_title or "").strip() or "未命名游戏"
            record_key = self._build_cloud_save_game_key(record_game_id, record_title)
            if game_id and record_game_id and game_id == record_game_id:
                matched_records.append(record)
                continue
            if game_key and record_key and game_key == record_key:
                matched_records.append(record)
                continue

        matched_records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)
        candidate_rows: List[Dict[str, Any]] = []
        seen_save_bases: Set[str] = set()

        for record in matched_records:
            shortcut_result: Dict[str, Any] = {}
            record_game_id = str(record.game_id or "").strip()
            if record_game_id:
                try:
                    shortcut_result = resolve_tianyi_shortcut_sync(game_id=record_game_id)
                except Exception as exc:
                    shortcut_result = {"ok": False, "message": str(exc)}

            metadata = self._build_switch_record_metadata(record=record, shortcut_result=shortcut_result)
            self._persist_switch_record_metadata(record, metadata)
            diagnostics["matched_records"].append(
                {
                    "game_id": record_game_id,
                    "game_title": str(record.game_title or "").strip() or str(game_title or "").strip() or "未命名游戏",
                    "install_path": str(record.install_path or "").strip(),
                    "switch_title_id": str(metadata.get("switch_title_id", "") or ""),
                    "rom_path": str(metadata.get("rom_path", "") or ""),
                    "emulator_path": str(metadata.get("emulator_path", "") or ""),
                }
            )
            for root_item in list(metadata.get("root_candidates") or []):
                if not isinstance(root_item, dict):
                    continue
                save_base = self._normalize_existing_dir(str(root_item.get("save_base", "") or ""))
                if not save_base or save_base in seen_save_bases:
                    continue
                seen_save_bases.add(save_base)
                matched_dirs = self._find_eden_title_save_dirs(save_base=save_base, title_id=title_id) if title_id else []
                candidate_rows.append(
                    {
                        "save_base": save_base,
                        "root": str(root_item.get("root", "") or ""),
                        "source": str(root_item.get("source", "") or ""),
                        "priority": max(0, _safe_int(root_item.get("priority"), 0)),
                        "matched_count": len(matched_dirs),
                    }
                )

        fallback_roots = self._build_eden_data_root_candidates(
            emulator_path=str(getattr(self.store.settings, "emulator_dir", "") or "").strip(),
            data_root_hint=str(payload.get("eden_data_root_hint", "") or ""),
        )
        for root_item in fallback_roots:
            if not isinstance(root_item, dict):
                continue
            save_base = self._normalize_existing_dir(str(root_item.get("save_base", "") or ""))
            if not save_base or save_base in seen_save_bases:
                continue
            seen_save_bases.add(save_base)
            matched_dirs = self._find_eden_title_save_dirs(save_base=save_base, title_id=title_id) if title_id else []
            candidate_rows.append(
                {
                    "save_base": save_base,
                    "root": str(root_item.get("root", "") or ""),
                    "source": str(root_item.get("source", "") or ""),
                    "priority": max(0, _safe_int(root_item.get("priority"), 0)),
                    "matched_count": len(matched_dirs),
                }
            )

        candidate_rows.sort(
            key=lambda item: (
                -1 if item.get("matched_count", 0) else 0,
                -max(0, _safe_int(item.get("priority"), 0)),
                -max(0, _safe_int(item.get("matched_count"), 0)),
                str(item.get("save_base", "") or ""),
            )
        )
        diagnostics["save_base_candidates"] = [dict(item) for item in candidate_rows]
        return {
            "title_id": title_id,
            "candidates": [str(item.get("save_base", "") or "") for item in candidate_rows],
            "diagnostics": diagnostics,
        }

    def _resolve_switch_restore_entry_target_dir(
        self,
        *,
        save_base: str,
        title_id: str,
        profile_id: str = "",
        user_id: str = "",
    ) -> str:
        """将单个 Switch 存档项映射到当前 Eden save 目录。"""
        normalized_save_base = self._normalize_dir_path(save_base)
        normalized_title_id = self._normalize_switch_title_id(title_id)
        normalized_profile_id = str(profile_id or "").strip()
        normalized_user_id = str(user_id or "").strip()
        if not normalized_save_base or not normalized_title_id:
            return ""

        if normalized_profile_id and normalized_user_id:
            exact_candidate = os.path.join(
                normalized_save_base,
                normalized_profile_id,
                normalized_user_id,
                normalized_title_id,
            )
            exact_parent = os.path.dirname(exact_candidate)
            if os.path.isdir(exact_candidate) or os.path.isdir(exact_parent):
                return exact_candidate

        existing_matches = self._find_eden_title_save_dirs(save_base=normalized_save_base, title_id=normalized_title_id)
        if existing_matches:
            if normalized_profile_id and normalized_user_id:
                for item in existing_matches:
                    if (
                        str(item.get("profile_id", "") or "").strip() == normalized_profile_id
                        and str(item.get("user_id", "") or "").strip() == normalized_user_id
                    ):
                        return str(item.get("path", "") or "")
            return str(existing_matches[0].get("path", "") or "")

        profile_dirs = [
            name
            for name in os.listdir(normalized_save_base)
            if name and name != "cache" and os.path.isdir(os.path.join(normalized_save_base, name))
        ] if os.path.isdir(normalized_save_base) else []
        profile_dirs.sort()

        if normalized_profile_id:
            profile_dir = os.path.join(normalized_save_base, normalized_profile_id)
            if os.path.isdir(profile_dir):
                user_dirs = [
                    name
                    for name in os.listdir(profile_dir)
                    if name and os.path.isdir(os.path.join(profile_dir, name))
                ]
                user_dirs.sort()
                if normalized_user_id:
                    return os.path.join(profile_dir, normalized_user_id, normalized_title_id)
                if len(user_dirs) == 1:
                    return os.path.join(profile_dir, user_dirs[0], normalized_title_id)

        if len(profile_dirs) == 1:
            only_profile = profile_dirs[0]
            only_profile_dir = os.path.join(normalized_save_base, only_profile)
            user_dirs = [
                name
                for name in os.listdir(only_profile_dir)
                if name and os.path.isdir(os.path.join(only_profile_dir, name))
            ]
            user_dirs.sort()
            if normalized_user_id:
                return os.path.join(only_profile_dir, normalized_user_id, normalized_title_id)
            if len(user_dirs) == 1:
                return os.path.join(only_profile_dir, user_dirs[0], normalized_title_id)

        if normalized_profile_id and normalized_user_id:
            return os.path.join(normalized_save_base, normalized_profile_id, normalized_user_id, normalized_title_id)
        return ""

    def _build_switch_restore_copy_plan(
        self,
        *,
        extract_dir: str,
        manifest: Optional[Dict[str, Any]],
        entries: Sequence[Dict[str, Any]],
        selected_entry_ids: Sequence[str],
        save_base: str,
    ) -> Dict[str, Any]:
        """为 Switch/Eden 云存档构建恢复复制计划。"""
        extract_root = self._normalize_existing_dir(extract_dir)
        normalized_save_base = self._normalize_dir_path(save_base)
        payload = manifest if isinstance(manifest, dict) else {}
        if not extract_root:
            raise ValueError("解压目录不存在")
        if not normalized_save_base:
            raise ValueError("目标目录无效")

        selected = {str(item or "").strip() for item in list(selected_entry_ids or []) if str(item or "").strip()}
        if not selected:
            raise ValueError("未选择任何存档项")

        normalized_entries = [dict(item) for item in list(entries or []) if isinstance(item, dict)]
        selected_entries = [item for item in normalized_entries if str(item.get("entry_id", "") or "") in selected]
        if not selected_entries:
            raise ValueError("未匹配到所选存档项")

        default_title_id = self._normalize_switch_title_id(payload.get("switch_title_id"))
        copy_pairs: List[Tuple[str, str]] = []
        plan_items: List[Dict[str, Any]] = []

        for entry in selected_entries:
            entry_id = str(entry.get("entry_id", "") or "").strip()
            entry_name = str(entry.get("entry_name", "") or entry_id).strip() or entry_id
            rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
            entry_root = os.path.realpath(os.path.join(extract_root, rel_path)) if rel_path else extract_root
            if not entry_root.startswith(extract_root):
                continue
            if not os.path.exists(entry_root):
                continue

            entry_title_id = self._extract_switch_title_id(entry.get("title_id"), default_title_id, rel_path)
            entry_profile_id = str(entry.get("profile_id", "") or "").strip()
            entry_user_id = str(entry.get("user_id", "") or "").strip()
            entry_target_root = self._resolve_switch_restore_entry_target_dir(
                save_base=normalized_save_base,
                title_id=entry_title_id,
                profile_id=entry_profile_id,
                user_id=entry_user_id,
            )
            if not entry_target_root:
                raise ValueError(f"未找到 Switch 存档目标：{entry_name}")

            pairs_for_entry: List[Tuple[str, str]] = []
            if os.path.isfile(entry_root):
                if os.path.basename(entry_root) == "manifest.json":
                    continue
                dest = os.path.join(entry_target_root, os.path.basename(entry_root))
                pairs_for_entry.append((entry_root, dest))
            else:
                for dirpath, _dirnames, filenames in os.walk(entry_root):
                    for filename in filenames:
                        if filename == "manifest.json":
                            continue
                        src_file = os.path.join(dirpath, filename)
                        rel_file = os.path.relpath(src_file, entry_root)
                        dst_file = os.path.realpath(os.path.join(entry_target_root, rel_file))
                        if not dst_file.startswith(entry_target_root):
                            continue
                        pairs_for_entry.append((src_file, dst_file))

            if not pairs_for_entry:
                continue

            copy_pairs.extend(pairs_for_entry)
            plan_items.append(
                {
                    "entry_id": entry_id,
                    "entry_name": entry_name,
                    "file_count": len(pairs_for_entry),
                    "target_dir": entry_target_root,
                    "title_id": entry_title_id,
                    "profile_id": entry_profile_id,
                    "user_id": entry_user_id,
                }
            )

        if not copy_pairs:
            raise ValueError("选中的存档项没有可恢复文件")

        conflict_paths: List[str] = []
        for _src, dst in copy_pairs:
            if os.path.exists(dst):
                conflict_paths.append(dst)

        dedup_conflicts = sorted(set(conflict_paths))
        return {
            "copy_pairs": copy_pairs,
            "plan_items": plan_items,
            "conflict_count": len(dedup_conflicts),
            "conflict_samples": dedup_conflicts[:CLOUD_SAVE_RESTORE_CONFLICT_SAMPLES],
        }

    def _build_restore_copy_plan(
        self,
        *,
        extract_dir: str,
        entries: Sequence[Dict[str, Any]],
        selected_entry_ids: Sequence[str],
        target_dir: str,
        entry_target_dirs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """将所选存档项映射为复制计划。"""
        extract_root = self._normalize_existing_dir(extract_dir)
        target_root = self._normalize_dir_path(target_dir)
        if not extract_root:
            raise ValueError("解压目录不存在")
        has_entry_targets = isinstance(entry_target_dirs, dict) and bool(entry_target_dirs)
        if not target_root and not has_entry_targets:
            raise ValueError("目标目录无效")

        selected = {str(item or "").strip() for item in list(selected_entry_ids or []) if str(item or "").strip()}
        if not selected:
            raise ValueError("未选择任何存档项")

        normalized_entries = [dict(item) for item in list(entries or []) if isinstance(item, dict)]
        selected_entries = [item for item in normalized_entries if str(item.get("entry_id", "") or "") in selected]
        if not selected_entries:
            raise ValueError("未匹配到所选存档项")

        copy_pairs: List[Tuple[str, str]] = []
        plan_items: List[Dict[str, Any]] = []
        for entry in selected_entries:
            entry_id = str(entry.get("entry_id", "") or "").strip()
            entry_name = str(entry.get("entry_name", "") or entry_id).strip() or entry_id
            rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
            entry_root = os.path.realpath(os.path.join(extract_root, rel_path)) if rel_path else extract_root
            if not entry_root.startswith(extract_root):
                continue
            if not os.path.exists(entry_root):
                continue

            preferred_target = ""
            if isinstance(entry_target_dirs, dict):
                preferred_target = str(entry_target_dirs.get(entry_id, "") or "").strip()
            entry_target_root = self._normalize_dir_path(preferred_target) or target_root
            if not entry_target_root:
                continue

            pairs_for_entry: List[Tuple[str, str]] = []
            if os.path.isfile(entry_root):
                if os.path.basename(entry_root) == "manifest.json":
                    continue
                dest = os.path.join(entry_target_root, os.path.basename(entry_root))
                pairs_for_entry.append((entry_root, dest))
            else:
                for dirpath, _dirnames, filenames in os.walk(entry_root):
                    for filename in filenames:
                        if filename == "manifest.json":
                            continue
                        src_file = os.path.join(dirpath, filename)
                        rel_file = os.path.relpath(src_file, entry_root)
                        dst_file = os.path.realpath(os.path.join(entry_target_root, rel_file))
                        if not dst_file.startswith(entry_target_root):
                            continue
                        pairs_for_entry.append((src_file, dst_file))

            if not pairs_for_entry:
                continue
            copy_pairs.extend(pairs_for_entry)
            plan_items.append(
                {
                    "entry_id": entry_id,
                    "entry_name": entry_name,
                    "file_count": len(pairs_for_entry),
                    "target_dir": entry_target_root,
                }
            )

        if not copy_pairs:
            raise ValueError("选中的存档项没有可恢复文件")

        conflict_paths: List[str] = []
        for _src, dst in copy_pairs:
            if os.path.exists(dst):
                conflict_paths.append(dst)
        dedup_conflicts = sorted(set(conflict_paths))

        return {
            "copy_pairs": copy_pairs,
            "plan_items": plan_items,
            "conflict_count": len(dedup_conflicts),
            "conflict_samples": dedup_conflicts[:CLOUD_SAVE_RESTORE_CONFLICT_SAMPLES],
        }

    def _compute_common_working_dir(self, paths: Sequence[str]) -> str:
        """计算 7z 工作目录。"""
        normalized = [
            os.path.realpath(os.path.expanduser(str(item or "").strip()))
            for item in list(paths or [])
            if str(item or "").strip()
        ]
        if not normalized:
            raise ValueError("缺少待打包路径")

        try:
            common = os.path.commonpath(normalized)
        except Exception:
            common = ""
        if common and os.path.isdir(common):
            return common

        first = normalized[0]
        if os.path.isdir(first):
            return first
        parent = os.path.dirname(first)
        if parent and os.path.isdir(parent):
            return parent
        raise ValueError("无法确定打包工作目录")

    async def _archive_single_game_saves(
        self,
        *,
        candidate: Dict[str, Any],
        timestamp: str,
    ) -> Tuple[str, Dict[str, Any]]:
        """打包单游戏存档并写入 manifest。"""
        game_key = str(candidate.get("game_key", "") or "game")
        game_id = str(candidate.get("game_id", "") or "")
        game_title = str(candidate.get("game_title", "") or "")
        source_paths = [str(path or "") for path in list(candidate.get("source_paths") or []) if str(path or "").strip()]
        platform = str(candidate.get("platform", "") or "").strip().lower()
        emulator_id = str(candidate.get("emulator_id", "") or "").strip().lower()
        diagnostics_raw = candidate.get("diagnostics")
        diagnostics = diagnostics_raw if isinstance(diagnostics_raw, dict) else {}
        playtime_raw = candidate.get("playtime")
        playtime: Dict[str, Any] = playtime_raw if isinstance(playtime_raw, dict) else {}
        if not source_paths:
            raise SevenZipError("缺少可打包的存档目录")

        temp_dir = tempfile.mkdtemp(prefix=f"freedeck_cloudsave_{game_key}_")
        archive_name = f"{game_key}_{timestamp}.7z"
        archive_path = os.path.join(temp_dir, archive_name)
        manifest_path = os.path.join(temp_dir, "manifest.json")
        try:
            working_dir = ""
            if platform == "switch" and emulator_id == "eden":
                working_dir = self._normalize_existing_dir(str(diagnostics.get("selected_save_base", "") or ""))
            if not working_dir:
                working_dir = self._compute_common_working_dir(source_paths)

            await asyncio.to_thread(
                self.seven_zip.create_archive,
                archive_path,
                source_paths,
                working_dir,
            )

            entry_items: List[Dict[str, Any]] = []
            for idx, source_path in enumerate(source_paths, start=1):
                normalized_source = os.path.realpath(os.path.expanduser(str(source_path or "").strip()))
                rel_path = ""
                try:
                    rel_path = os.path.relpath(normalized_source, working_dir).replace("\\", "/")
                except Exception:
                    rel_path = os.path.basename(normalized_source.rstrip("/\\"))
                rel_path = str(rel_path or "").replace("\\", "/").strip("./")
                if not rel_path:
                    rel_path = os.path.basename(normalized_source.rstrip("/\\"))
                entry_name = os.path.basename(normalized_source.rstrip("/\\")).strip() or f"entry_{idx}"
                entry_item: Dict[str, Any] = {
                    "entry_id": f"entry_{idx}",
                    "name": entry_name,
                    "source_path": normalized_source,
                    "archive_rel_path": rel_path,
                }
                if platform == "switch" and emulator_id == "eden":
                    eden_meta = self._extract_eden_save_path_metadata(normalized_source, working_dir)
                    entry_item.update(
                        {
                            "platform": "switch",
                            "emulator_id": "eden",
                            "title_id": str(eden_meta.get("title_id", "") or candidate.get("switch_title_id", "") or ""),
                            "profile_id": str(eden_meta.get("profile_id", "") or ""),
                            "user_id": str(eden_meta.get("user_id", "") or ""),
                            "relative_root": "eden_save_base",
                            "relative_path": str(eden_meta.get("relative_path", "") or rel_path),
                        }
                    )
                entry_items.append(entry_item)

            archive_relative_paths = [
                str(item.get("archive_rel_path", "") or "")
                for item in entry_items
                if str(item.get("archive_rel_path", "") or "").strip()
            ]
            switch_profile_ids = sorted(
                {
                    str(item.get("profile_id", "") or "").strip()
                    for item in entry_items
                    if str(item.get("profile_id", "") or "").strip()
                }
            )

            manifest = {
                "game_id": game_id,
                "game_title": game_title,
                "game_key": game_key,
                "manifest_version": 2,
                "generated_at": _now_wall_ts(),
                "platform": platform,
                "emulator_id": emulator_id,
                "source_paths": list(source_paths),
                "source_strategy": str(candidate.get("source_strategy", "") or ""),
                "install_path": str(candidate.get("install_path", "") or ""),
                "working_dir": working_dir,
                "switch_title_id": str(candidate.get("switch_title_id", "") or ""),
                "rom_path": str(candidate.get("rom_path", "") or ""),
                "emulator_path": str(candidate.get("emulator_path", "") or ""),
                "eden_data_root_hint": str(diagnostics.get("selected_root", "") or candidate.get("eden_data_root_hint", "") or ""),
                "eden_save_base": str(diagnostics.get("selected_save_base", "") or ""),
                "eden_root_strategy": str(diagnostics.get("selected_source", "") or ""),
                "eden_profile_id": switch_profile_ids[0] if len(switch_profile_ids) == 1 else "",
                "archive_relative_paths": archive_relative_paths,
                "entries": entry_items,
                "playtime": {
                    "seconds": max(0, _safe_int(playtime.get("seconds"), 0)),
                    "sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
                    "last_played_at": max(0, _safe_int(playtime.get("last_played_at"), 0)),
                    "captured_at": max(0, _safe_int(playtime.get("captured_at"), 0)) or _now_wall_ts(),
                },
            }
            with open(manifest_path, "w", encoding="utf-8") as fp:
                json.dump(manifest, fp, ensure_ascii=False, indent=2)

            await asyncio.to_thread(
                self.seven_zip.create_archive,
                archive_path,
                [manifest_path],
                temp_dir,
            )

            archive_size = 0
            try:
                archive_size = max(0, int(os.path.getsize(archive_path)))
            except Exception:
                archive_size = 0

            return archive_path, {
                "temp_dir": temp_dir,
                "manifest_path": manifest_path,
                "archive_name": archive_name,
                "archive_size_bytes": archive_size,
            }
        except Exception:
            self._cleanup_cloud_save_temp_paths([temp_dir])
            raise

    def _cleanup_cloud_save_temp_paths(self, temp_paths: Sequence[str]) -> None:
        """清理临时文件与目录。"""
        for raw in list(temp_paths or []):
            path = os.path.realpath(os.path.expanduser(str(raw or "").strip()))
            if not path:
                continue
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                elif os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

    async def _collect_cloud_save_candidates(
        self,
        *,
        records: Optional[Sequence[TianyiInstalledGame]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """基于已安装记录构建云存档候选。"""
        source_records = list(records if records is not None else (self.store.installed_games or []))
        source_records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)

        candidates: List[Dict[str, Any]] = []
        ignored: List[Dict[str, Any]] = []
        dedupe_keys = set()

        for record in source_records:
            game_id = str(record.game_id or "").strip()
            game_title = str(record.game_title or "").strip() or "未命名游戏"
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            if not install_path:
                ignored.append(
                    {
                        "game_id": game_id,
                        "game_title": game_title,
                        "reason": "install_path_missing",
                    }
                )
                continue

            dedupe_key = f"{game_id}|{install_path}"
            if dedupe_key in dedupe_keys:
                continue
            dedupe_keys.add(dedupe_key)

            game_key = self._build_cloud_save_game_key(game_id, game_title)

            shortcut_result: Dict[str, Any] = {}
            if game_id:
                try:
                    shortcut_result = await asyncio.to_thread(resolve_tianyi_shortcut_sync, game_id=game_id)
                except Exception as exc:
                    shortcut_result = {"ok": False, "message": str(exc)}
            else:
                shortcut_result = {"ok": False, "message": "game_id_missing"}

            exe_path = str(shortcut_result.get("exe_path", "") or "").strip()
            tokens = self._build_cloud_save_match_tokens(
                game_id=game_id,
                game_title=game_title,
                install_path=install_path,
                source_path=str(record.source_path or "").strip(),
                exe_path=exe_path,
            )
            launch_parsed = self._parse_switch_launch_options(str(shortcut_result.get("launch_options", "") or ""))
            is_switch_record = (
                str(getattr(record, "platform", "") or "").strip().lower() == "switch"
                or str(getattr(record, "emulator_id", "") or "").strip().lower() == "eden"
                or self._is_switch_catalog_game(game_id)
                or self._looks_like_switch_rom_path(str(launch_parsed.get("rom_path", "") or ""))
            )

            compat_user_dir, compat_diag = self._resolve_compat_user_dir_for_record(
                record=record,
                shortcut_result=shortcut_result,
            )

            source_paths: List[str] = []
            source_strategy = ""
            skip_reason = ""
            diagnostics: Dict[str, Any] = {
                "tokens": list(tokens),
                "install_path": install_path,
                "record": {
                    "source_path": str(record.source_path or "").strip(),
                    "steam_app_id": max(0, _safe_int(record.steam_app_id, 0)),
                    "playtime_active_app_id": max(0, _safe_int(record.playtime_active_app_id, 0)),
                },
                "shortcut": {
                    "ok": bool(shortcut_result.get("ok")),
                    "message": str(shortcut_result.get("message", "") or ""),
                    "steam_root": str(shortcut_result.get("steam_root", "") or "").strip(),
                    "user_id": str(shortcut_result.get("user_id", "") or "").strip(),
                    "appid_unsigned": _safe_int(shortcut_result.get("appid_unsigned"), 0),
                    "app_name": str(shortcut_result.get("app_name", "") or ""),
                    "exe_path": exe_path,
                    "compat_user_dir": str(shortcut_result.get("compat_user_dir", "") or ""),
                    "compat_candidates": list(shortcut_result.get("compat_candidates") or []),
                    "launch_options": str(shortcut_result.get("launch_options", "") or ""),
                },
                "compat": compat_diag,
                "platform": "switch" if is_switch_record else "pc",
            }

            if is_switch_record:
                switch_context = self._resolve_switch_cloud_save_context(
                    record=record,
                    shortcut_result=shortcut_result,
                )
                source_paths = [
                    str(path or "")
                    for path in list(switch_context.get("source_paths") or [])
                    if str(path or "").strip()
                ]
                source_strategy = str(switch_context.get("source_strategy", "") or "eden_nand")
                skip_reason = str(switch_context.get("reason", "") or "").strip()
                diagnostics.update(dict(switch_context.get("diagnostics") or {}))
                switch_metadata = dict(switch_context.get("metadata") or {})
                if switch_metadata:
                    diagnostics["switch_metadata"] = {
                        "platform": str(switch_metadata.get("platform", "") or ""),
                        "emulator_id": str(switch_metadata.get("emulator_id", "") or ""),
                        "switch_title_id": str(switch_metadata.get("switch_title_id", "") or ""),
                        "rom_path": str(switch_metadata.get("rom_path", "") or ""),
                        "emulator_path": str(switch_metadata.get("emulator_path", "") or ""),
                        "eden_data_root_hint": str(switch_metadata.get("eden_data_root_hint", "") or ""),
                    }
            else:
                if compat_user_dir:
                    source_paths, proton_diag = self._collect_cloud_save_paths_from_proton(
                        compat_user_dir=compat_user_dir,
                        tokens=tokens,
                    )
                    diagnostics["proton_scan"] = proton_diag
                    source_strategy = "proton_prefix"
                    if not source_paths:
                        skip_reason = "save_path_not_found"
                else:
                    source_paths, fallback_diag = self._collect_cloud_save_paths_from_install(
                        install_path=install_path,
                        tokens=tokens,
                    )
                    diagnostics["fallback_scan"] = fallback_diag
                    source_strategy = "install_fallback"
                    if not source_paths:
                        skip_reason = "prefix_unresolved"
                        diagnostics.setdefault("prefix_unresolved", {})
                        diagnostics["prefix_unresolved"] = {
                            "reason": str(compat_diag.get("reason", "") or "").strip(),
                            "strategy": str(compat_diag.get("strategy", "") or "").strip(),
                            "appid_unsigned": max(0, _safe_int(compat_diag.get("appid_unsigned"), 0)),
                        }

            playtime = self._snapshot_record_playtime(record)

            candidates.append(
                {
                    "game_id": game_id,
                    "game_title": game_title,
                    "game_key": game_key,
                    "install_path": install_path,
                    "platform": "switch" if is_switch_record else "pc",
                    "emulator_id": "eden" if is_switch_record else "",
                    "switch_title_id": str(getattr(record, "switch_title_id", "") or diagnostics.get("title_id") or ""),
                    "rom_path": str(getattr(record, "rom_path", "") or ""),
                    "emulator_path": str(getattr(record, "emulator_path", "") or exe_path),
                    "source_paths": list(source_paths),
                    "source_strategy": source_strategy,
                    "skip_reason": skip_reason,
                    "playtime": {
                        "seconds": max(0, _safe_int(playtime.get("seconds"), 0)),
                        "sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
                        "last_played_at": max(0, _safe_int(playtime.get("last_played_at"), 0)),
                        "captured_at": _now_wall_ts(),
                    },
                    "diagnostics": diagnostics,
                }
            )

        strategy_counts: Dict[str, int] = {}
        skip_reason_counts: Dict[str, int] = {}
        with_source_paths = 0
        for item in candidates:
            if not isinstance(item, dict):
                continue
            strategy = str(item.get("source_strategy", "") or "").strip() or "unknown"
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            skip_reason = str(item.get("skip_reason", "") or "").strip()
            if skip_reason:
                skip_reason_counts[skip_reason] = skip_reason_counts.get(skip_reason, 0) + 1
            paths = item.get("source_paths", [])
            if isinstance(paths, list) and any(str(p or "").strip() for p in paths):
                with_source_paths += 1

        diagnostics = {
            "installed_total": len(source_records),
            "library_total": len(self.store.installed_games or []),
            "candidate_total": len(candidates),
            "with_source_paths": with_source_paths,
            "strategy_counts": strategy_counts,
            "skip_reason_counts": skip_reason_counts,
            "ignored_total": len(ignored),
            "ignored": ignored,
        }
        return candidates, diagnostics

    async def _run_cloud_save_upload_task(
        self,
        *,
        cookie: str,
        user_account: str,
        trigger: str = "manual_all",
        records: Optional[Sequence[TianyiInstalledGame]] = None,
    ) -> None:
        """执行云存档上传任务。"""
        normalized_trigger = "auto_single" if str(trigger or "").strip().lower() == "auto_single" else "manual_all"
        started_at = _now_wall_ts()
        timestamp = time.strftime(CLOUD_SAVE_DATE_FORMAT, time.localtime(started_at))
        candidates: List[Dict[str, Any]] = []
        collect_diagnostics: Dict[str, Any] = {}
        results: List[Dict[str, Any]] = []
        uploaded = 0
        skipped = 0
        failed = 0
        final_stage = "completed"
        final_reason = ""
        final_message = "云存档上传完成"
        exception_text = ""
        cancelled = False

        try:
            candidates, collect_diagnostics = await self._collect_cloud_save_candidates(records=records)
            total_games = len(candidates)
            scoped_record = records[0] if records else None
            scoped_title = str(getattr(scoped_record, "game_title", "") or "").strip()
            scoped_id = str(getattr(scoped_record, "game_id", "") or "").strip()

            await self._set_cloud_save_state(
                stage="scanning",
                running=True,
                message=(
                    f"自动上传候选 {total_games} 个：{scoped_title or '未命名游戏'}"
                    if normalized_trigger == "auto_single"
                    else f"发现 {total_games} 个候选游戏"
                ),
                reason="",
                current_game="",
                total_games=total_games,
                processed_games=0,
                uploaded=0,
                skipped=0,
                failed=0,
                progress=0.0 if total_games > 0 else 100.0,
                results=[],
                diagnostics={
                    "started_at": started_at,
                    "timestamp": timestamp,
                    "user_account": user_account,
                    "trigger": normalized_trigger,
                    "target_game_id": scoped_id,
                    "target_game_title": scoped_title,
                    "collect": collect_diagnostics,
                },
            )

            if total_games <= 0:
                final_message = "未找到可上传的已安装游戏"

            for index, candidate in enumerate(candidates, start=1):
                game_title = str(candidate.get("game_title", "") or "未命名游戏")
                game_key = str(candidate.get("game_key", "") or "")
                source_paths = [str(path or "") for path in list(candidate.get("source_paths") or []) if str(path or "").strip()]
                skip_reason = str(candidate.get("skip_reason", "") or "").strip()
                entry: Dict[str, Any] = {
                    "game_id": str(candidate.get("game_id", "") or ""),
                    "game_title": game_title,
                    "game_key": game_key,
                    "status": "",
                    "reason": "",
                    "cloud_path": "",
                    "source_paths": source_paths,
                    "diagnostics": dict(candidate.get("diagnostics") or {}),
                }

                await self._set_cloud_save_state(
                    stage="packaging",
                    running=True,
                    message=f"正在处理 {index}/{total_games}：{game_title}",
                    current_game=game_title,
                )

                if skip_reason:
                    entry["status"] = "skipped"
                    entry["reason"] = skip_reason
                    try:
                        diag = dict(entry.get("diagnostics") or {})
                        shortcut_message = ""
                        shortcut = diag.get("shortcut")
                        if isinstance(shortcut, dict):
                            shortcut_message = str(shortcut.get("message", "") or "").strip()
                        prefix_diag = diag.get("prefix_unresolved")
                        prefix_reason = ""
                        if isinstance(prefix_diag, dict):
                            prefix_reason = str(prefix_diag.get("reason", "") or "").strip()
                        config.logger.info(
                            "Cloud save skipped: game=%s reason=%s prefix_reason=%s shortcut=%s",
                            game_title,
                            skip_reason,
                            prefix_reason,
                            shortcut_message,
                        )
                    except Exception:
                        pass
                    skipped += 1
                else:
                    cleanup_paths: List[str] = []
                    try:
                        archive_path, archive_meta = await self._archive_single_game_saves(
                            candidate=candidate,
                            timestamp=timestamp,
                        )
                        cleanup_paths.append(str(archive_meta.get("temp_dir", "") or ""))
                        archive_name = str(archive_meta.get("archive_name", "") or "")
                        archive_size = _safe_int(archive_meta.get("archive_size_bytes"), 0)

                        await self._set_cloud_save_state(
                            stage="uploading",
                            running=True,
                            message=f"正在上传 {index}/{total_games}：{game_title}",
                            current_game=game_title,
                        )

                        remote_name = f"{timestamp}.7z"
                        upload_result = await upload_archive_to_cloud(
                            cookie=cookie,
                            local_file_path=archive_path,
                            remote_folder_parts=[game_key],
                            remote_name=remote_name,
                        )

                        cloud_path = str(upload_result.get("cloud_path", "") or "").strip()
                        if not cloud_path:
                            cloud_path = f"/{CLOUD_SAVE_UPLOAD_ROOT}/{game_key}/{remote_name}"

                        entry["status"] = "uploaded"
                        entry["reason"] = ""
                        entry["cloud_path"] = cloud_path
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "archive_name": archive_name,
                            "archive_size_bytes": archive_size,
                            "upload_result": dict(upload_result),
                        }
                        uploaded += 1
                    except asyncio.CancelledError:
                        cancelled = True
                        raise
                    except SevenZipError as exc:
                        entry["status"] = "failed"
                        entry["reason"] = "package_failed"
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "exception": str(exc),
                        }
                        failed += 1
                    except TianyiApiError as exc:
                        entry["status"] = "failed"
                        entry["reason"] = "upload_failed"
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "exception": str(exc),
                            "api_diagnostics": dict(getattr(exc, "diagnostics", {}) or {}),
                        }
                        failed += 1
                    except Exception as exc:
                        entry["status"] = "failed"
                        entry["reason"] = "unexpected_error"
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "exception": str(exc),
                        }
                        failed += 1
                    finally:
                        self._cleanup_cloud_save_temp_paths(cleanup_paths)

                results.append(self._copy_cloud_save_result(entry))
                processed = len(results)
                progress = 100.0 if total_games <= 0 else (float(processed) / float(total_games)) * 100.0
                await self._set_cloud_save_state(
                    stage="uploading" if processed < total_games else "scanning",
                    running=True,
                    message=f"已处理 {processed}/{total_games}",
                    current_game=game_title,
                    processed_games=processed,
                    uploaded=uploaded,
                    skipped=skipped,
                    failed=failed,
                    progress=progress,
                    results=results,
                )

                if cancelled:
                    break
        except asyncio.CancelledError:
            cancelled = True
            final_stage = "failed"
            final_reason = "task_cancelled"
            final_message = "云存档上传任务已取消"
        except Exception as exc:
            final_stage = "failed"
            final_reason = "task_exception"
            final_message = f"云存档上传任务异常：{exc}"
            exception_text = str(exc)
            config.logger.exception("Cloud save upload task failed: %s", exc)
        else:
            if failed > 0:
                final_stage = "failed"
                final_reason = "partial_failed"
                final_message = f"云存档上传完成（成功 {uploaded}，失败 {failed}，跳过 {skipped}）"
            else:
                final_stage = "completed"
                final_reason = ""
                final_message = f"云存档上传完成（成功 {uploaded}，跳过 {skipped}）"
        finally:
            finished_at = _now_wall_ts()
            total_games = len(candidates)
            final_payload = {
                "stage": final_stage,
                "reason": final_reason,
                "message": final_message,
                "started_at": started_at,
                "finished_at": finished_at,
                "timestamp": timestamp,
                "total_games": total_games,
                "processed_games": len(results),
                "uploaded": uploaded,
                "skipped": skipped,
                "failed": failed,
                "results": results,
                "diagnostics": {
                    "trigger": normalized_trigger,
                    "collect": collect_diagnostics,
                    "exception": exception_text,
                    "cancelled": cancelled,
                },
            }

            try:
                await asyncio.to_thread(self.store.set_cloud_save_last_result, final_payload)
            except Exception as exc:
                config.logger.warning("Persist cloud save upload result failed: %s", exc)

            await self._set_cloud_save_state(
                stage=final_stage,
                running=False,
                message=final_message,
                reason=final_reason,
                current_game="",
                total_games=total_games,
                processed_games=len(results),
                uploaded=uploaded,
                skipped=skipped,
                failed=failed,
                progress=100.0 if total_games <= 0 else (float(len(results)) / float(total_games)) * 100.0,
                results=results,
                diagnostics={
                    "trigger": normalized_trigger,
                    "collect": collect_diagnostics,
                    "exception": exception_text,
                    "cancelled": cancelled,
                },
                last_result=final_payload,
            )

            async with self._cloud_save_lock:
                current = asyncio.current_task()
                if self._cloud_save_task is current:
                    self._cloud_save_task = None

            if cancelled:
                raise asyncio.CancelledError()

    async def uninstall_installed_game(
        self,
        *,
        game_id: str = "",
        install_path: str = "",
        delete_files: bool = True,
        delete_proton_files: bool = False,
    ) -> Dict[str, Any]:
        """卸载已安装游戏（删除文件并移除记录）。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = str(install_path or "").strip()
        if not target_game_id and not target_install_path:
            raise ValueError("缺少卸载目标")

        record = self._find_installed_record(game_id=target_game_id, install_path=target_install_path)
        if record is None:
            raise ValueError("未找到已安装游戏记录")

        resolved_path = os.path.realpath(os.path.expanduser(str(record.install_path or "").strip()))
        removed_files = False
        if bool(delete_files):
            allow, reason = self._can_remove_install_path(resolved_path)
            if not allow:
                raise ValueError(reason or "卸载路径不安全，已拒绝删除")

            if os.path.lexists(resolved_path):
                try:
                    if os.path.islink(resolved_path) or os.path.isfile(resolved_path):
                        os.remove(resolved_path)
                    elif os.path.isdir(resolved_path):
                        shutil.rmtree(resolved_path, ignore_errors=False)
                    else:
                        os.remove(resolved_path)
                    removed_files = True
                except Exception as exc:
                    raise RuntimeError(f"删除安装文件失败: {exc}") from exc

        removed = await asyncio.to_thread(
            self.store.remove_installed_game,
            game_id=record.game_id,
            install_path=record.install_path,
        )
        persist_warning = ""
        if removed is None:
            # 某些异常场景下 remove 可能返回空，回退为内存移除避免前端直接报错。
            removed = self._remove_installed_record_in_memory(
                game_id=record.game_id,
                install_path=record.install_path,
            )
            if removed is not None:
                try:
                    await asyncio.to_thread(self.store.save)
                except Exception as exc:
                    persist_warning = str(exc)
                    config.logger.warning("Installed record fallback save failed: %s", exc)
            else:
                raise RuntimeError("卸载记录写入失败，请重试")

        steam_cleanup: Dict[str, Any] = {}
        steam_warning = ""
        record_game_id = str(record.game_id or "").strip()
        if record_game_id:
            try:
                steam_cleanup = await remove_tianyi_shortcut(
                    game_id=record_game_id,
                    delete_compatdata=bool(delete_proton_files),
                    fallback_app_id=max(0, _safe_int(record.steam_app_id, 0)),
                )
            except Exception as exc:
                steam_cleanup = {"ok": False, "removed": False, "message": str(exc)}

            if not bool(steam_cleanup.get("ok")):
                steam_warning = f"Steam 快捷方式清理失败：{str(steam_cleanup.get('message', '') or '未知错误')}"
            elif not bool(steam_cleanup.get("cleanup_ok", True)):
                steam_warning = "Steam 快捷方式已删除，但 Proton 映射、封面或 Proton 前缀清理失败"
        else:
            steam_cleanup = {"ok": False, "removed": False, "message": "缺少 game_id，跳过 Steam 快捷方式清理"}
            steam_warning = "Steam 快捷方式清理已跳过：缺少 game_id"

        removed_key = self._installed_record_session_key(record)
        if removed_key:
            async with self._playtime_lock:
                self._playtime_sessions.pop(removed_key, None)

        self._invalidate_panel_cache(installed=True)
        summary = self._build_installed_summary(limit=60, persist=False)
        self._panel_installed_cache = {
            "total": int(summary.get("total", 0) or 0),
            "preview": list(summary.get("preview") or []),
        }
        self._panel_installed_cache_at = time.monotonic()
        response: Dict[str, Any] = {
            "removed": True,
            "game_id": str(record.game_id or ""),
            "title": str(record.game_title or ""),
            "install_path": str(record.install_path or ""),
            "files_deleted": bool(removed_files),
            "proton_files_deleted": bool(((steam_cleanup.get("compatdata") or {}) if isinstance(steam_cleanup, dict) else {}).get("removed")),
            "installed": summary,
            "steam": steam_cleanup,
        }
        warnings: List[str] = []
        if persist_warning:
            warnings.append(f"卸载记录回退保存失败：{persist_warning}")
        if steam_warning:
            warnings.append(steam_warning)
        if warnings:
            response["warning"] = "；".join(warnings)
        return response

    async def update_settings(
        self,
        *,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
        emulator_dir: Optional[str] = None,
        split_count: Optional[int] = None,
        aria2_fast_mode: Optional[bool] = None,
        force_ipv4: Optional[bool] = None,
        auto_switch_line: Optional[bool] = None,
        page_size: Optional[int] = None,
        auto_delete_package: Optional[bool] = None,
        auto_install: Optional[bool] = None,
        lsfg_enabled: Optional[bool] = None,
        show_playtime_widget: Optional[bool] = None,
        cloud_save_auto_upload: Optional[bool] = None,
        steamgriddb_enabled: Optional[bool] = None,
        steamgriddb_api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """更新下载设置。"""
        if download_dir is not None:
            path = os.path.realpath(os.path.expanduser(str(download_dir).strip()))
            if not path:
                raise ValueError("下载目录无效")
            os.makedirs(path, exist_ok=True)
            download_dir = path
            # 同步插件原有下载目录，避免路径分裂。
            self.plugin.downloads_dir = path
            await self.plugin.set_download_dir(path)
        if install_dir is not None:
            path = os.path.realpath(os.path.expanduser(str(install_dir).strip()))
            if not path:
                raise ValueError("安装目录无效")
            os.makedirs(path, exist_ok=True)
            install_dir = path

        if emulator_dir is not None:
            raw = str(emulator_dir or "").strip()
            if not raw:
                emulator_dir = ""
            else:
                path = os.path.realpath(os.path.expanduser(raw))
                if not path:
                    raise ValueError("模拟器目录无效")
                os.makedirs(path, exist_ok=True)
                emulator_dir = path

        if steamgriddb_api_key is not None:
            steamgriddb_api_key = str(steamgriddb_api_key or "").strip()

        self.store.set_settings(
            download_dir=download_dir,
            install_dir=install_dir,
            emulator_dir=emulator_dir,
            split_count=split_count,
            aria2_fast_mode=aria2_fast_mode,
            force_ipv4=force_ipv4,
            auto_switch_line=auto_switch_line,
            page_size=page_size,
            auto_delete_package=auto_delete_package,
            auto_install=True,
            lsfg_enabled=lsfg_enabled,
            show_playtime_widget=show_playtime_widget,
            cloud_save_auto_upload=cloud_save_auto_upload,
            steamgriddb_enabled=steamgriddb_enabled,
            steamgriddb_api_key=steamgriddb_api_key,
        )
        if cloud_save_auto_upload is not None:
            config.logger.info(
                "Cloud save auto upload setting updated: enabled=%s",
                bool(getattr(self.store.settings, "cloud_save_auto_upload", False)),
            )
        if force_ipv4 is not None:
            try:
                await self.aria2.change_global_options({"disable-ipv6": "true" if bool(force_ipv4) else "false"})
                config.logger.info("aria2 disable-ipv6 updated: force_ipv4=%s", bool(force_ipv4))
            except Exception as exc:
                config.logger.info("aria2 disable-ipv6 update failed: %s", exc)
        self._invalidate_panel_cache(all_data=True)
        return asdict(self.store.settings)

    async def prepare_install(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        steam_app_id: int = 0,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """生成安装前确认数据（不创建任务）。"""
        return await self._build_install_plan(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            steam_app_id=steam_app_id,
            download_dir=download_dir,
            install_dir=install_dir,
        )

    async def _resolve_share_cached(
        self,
        *,
        provider: str,
        share_url: str,
        tianyi_cookie: str = "",
        baidu_cookie: str = "",
        ctfile_token: str = "",
    ) -> Tuple[Any, Dict[str, Any], str]:
        """Resolve share listing with a short-lived in-memory cache.

        Returns: (resolved_share, share_ctx, canonical_url)
        """
        normalized_provider = str(provider or "").strip().lower()
        normalized_share_url = str(share_url or "").strip()
        if not normalized_provider or not normalized_share_url:
            raise TianyiApiError("分享链接为空", diagnostics={"provider": normalized_provider, "share_url": normalized_share_url})

        cache_key = f"{normalized_provider}::{normalized_share_url}"
        now_ts = _now_wall_ts()
        ttl = max(30, int(SHARE_RESOLVE_CACHE_SECONDS or 0))

        async with self._share_resolve_cache_lock:
            cached = self._share_resolve_cache.get(cache_key)
            if isinstance(cached, dict) and int(cached.get("expires_at", 0)) > now_ts:
                resolved = cached.get("resolved")
                ctx = cached.get("share_ctx") if isinstance(cached.get("share_ctx"), dict) else {}
                canonical = str(cached.get("canonical_url") or normalized_share_url).strip() or normalized_share_url
                try:
                    return copy.deepcopy(resolved), copy.deepcopy(ctx), canonical
                except Exception:
                    # Cache payload might be corrupted / non-copyable; fall back to re-resolve.
                    pass

            inflight = self._share_resolve_inflight.get(cache_key)
            if inflight is None or inflight.done():
                inflight = asyncio.create_task(
                    self._resolve_share_uncached(
                        provider=normalized_provider,
                        share_url=normalized_share_url,
                        tianyi_cookie=tianyi_cookie,
                        baidu_cookie=baidu_cookie,
                        ctfile_token=ctfile_token,
                    ),
                    name=f"freedeck_resolve_share:{normalized_provider}",
                )
                self._share_resolve_inflight[cache_key] = inflight

        try:
            resolved, ctx, canonical = await inflight
        except Exception:
            async with self._share_resolve_cache_lock:
                current = self._share_resolve_inflight.get(cache_key)
                if current is inflight:
                    self._share_resolve_inflight.pop(cache_key, None)
            raise

        canonical_url = str(canonical or "").strip() or normalized_share_url
        cache_value = {
            "resolved": copy.deepcopy(resolved),
            "share_ctx": copy.deepcopy(ctx) if isinstance(ctx, dict) else {},
            "canonical_url": canonical_url,
            "expires_at": int(now_ts + ttl),
        }

        async with self._share_resolve_cache_lock:
            self._share_resolve_inflight.pop(cache_key, None)
            self._share_resolve_cache[cache_key] = cache_value
            if canonical_url and canonical_url != normalized_share_url:
                canonical_key = f"{normalized_provider}::{canonical_url}"
                self._share_resolve_cache[canonical_key] = cache_value

        return copy.deepcopy(resolved), copy.deepcopy(ctx), canonical_url

    async def _resolve_share_uncached(
        self,
        *,
        provider: str,
        share_url: str,
        tianyi_cookie: str = "",
        baidu_cookie: str = "",
        ctfile_token: str = "",
    ) -> Tuple[Any, Dict[str, Any], str]:
        """Resolve share listing without cache (internal helper)."""
        normalized_provider = str(provider or "").strip().lower()
        normalized_share_url = str(share_url or "").strip()
        share_ctx: Dict[str, Any] = {}
        canonical_url = ""

        if normalized_provider == "tianyi":
            resolved = await resolve_share(normalized_share_url, str(tianyi_cookie or "").strip())
        elif normalized_provider == "baidu":
            resolved, share_ctx = await resolve_baidu_share(normalized_share_url, str(baidu_cookie or "").strip())
            canonical_url = str(share_ctx.get("canonical_url") or share_ctx.get("share_url") or "").strip()
        elif normalized_provider == "ctfile":
            resolved, share_ctx = await resolve_ctfile_share(normalized_share_url, str(ctfile_token or "").strip())
            canonical_url = str(share_ctx.get("canonical_url") or share_ctx.get("share_url") or "").strip()
        else:
            raise TianyiApiError("暂不支持的分享链接", diagnostics={"provider": normalized_provider, "share_url": normalized_share_url})

        return resolved, share_ctx, canonical_url or normalized_share_url

    async def start_install(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        steam_app_id: int = 0,
        split_count: Optional[int] = None,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """确认后创建下载任务并进入安装链路。"""
        async with self._lock:
            plan = await self._build_install_plan(
                game_id=game_id,
                share_url=share_url,
                file_ids=file_ids,
                steam_app_id=steam_app_id,
                download_dir=download_dir,
                install_dir=install_dir,
                include_sensitive=True,
            )
            if not bool(plan.get("can_install")):
                raise TianyiApiError("空间不足，无法开始安装")

            settings = self.store.settings
            split = int(split_count or settings.split_count or 16)
            split = max(1, min(64, split))
            if bool(getattr(settings, "aria2_fast_mode", False)):
                split = max(split, 32)

            created = await self._create_tasks_from_plan(plan=plan, split=split)
            self.store.upsert_tasks(created)
            self._invalidate_panel_cache(tasks=True)
            await self.refresh_tasks(sync_aria2=True)

            created_ids = {task.task_id for task in created}
            created_view = [_task_to_view(task) for task in self.store.tasks if task.task_id in created_ids]
            safe_plan = dict(plan)
            safe_plan.pop("_ctfile_token_override", None)
            return {
                "plan": safe_plan,
                "tasks": created_view,
            }

    async def create_tasks_for_game(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        steam_app_id: int = 0,
        split_count: Optional[int] = None,
        download_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """兼容旧接口：直接创建下载任务。"""
        result = await self.start_install(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            steam_app_id=steam_app_id,
            split_count=split_count,
            download_dir=download_dir,
            install_dir=None,
        )
        tasks = result.get("tasks")
        if isinstance(tasks, list):
            return [item for item in tasks if isinstance(item, dict)]
        return []

    async def _build_install_plan(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        steam_app_id: int = 0,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
        include_sensitive: bool = False,
    ) -> Dict[str, Any]:
        """构建安装计划与空间探针信息。"""
        open_path = ""
        catalog_app_id = max(0, _safe_int(steam_app_id, 0))
        is_switch_entry = False
        if not share_url:
            item = self.catalog.get_by_game_id(game_id)
            if not item:
                raise ValueError("未找到对应游戏条目")
            share_url = item.down_url
            game_title = item.title
            game_id = item.game_id
            open_path = str(item.openpath or "")
            item_app_id = max(0, _safe_int(getattr(item, "app_id", 0), 0))
            if item_app_id > 0:
                catalog_app_id = item_app_id
            is_switch_entry = str(getattr(item, "category_parent", "") or "").strip() == "527"
        else:
            normalized_share_url = str(share_url or "").strip()
            matched_item = None
            for entry in self.catalog.entries:
                if game_id and str(entry.game_id) != str(game_id):
                    continue
                if str(entry.down_url or "").strip() == normalized_share_url:
                    matched_item = entry
                    break
            if matched_item is None:
                for entry in self.catalog.entries:
                    if str(entry.down_url or "").strip() == normalized_share_url:
                        matched_item = entry
                        break

            if matched_item is not None:
                game_title = str(matched_item.title or game_id or "自定义分享")
                game_id = str(matched_item.game_id or game_id or normalized_share_url)
                open_path = str(matched_item.openpath or "")
                matched_item_app_id = max(0, _safe_int(getattr(matched_item, "app_id", 0), 0))
                if matched_item_app_id > 0:
                    catalog_app_id = matched_item_app_id
                is_switch_entry = str(getattr(matched_item, "category_parent", "") or "").strip() == "527"
                if "pwd=" not in normalized_share_url and str(matched_item.pwd or "").strip():
                    joiner = "&" if "?" in normalized_share_url else "?"
                    normalized_share_url = f"{normalized_share_url}{joiner}pwd={matched_item.pwd.strip()}"
                share_url = normalized_share_url
            else:
                game_title = game_id or "自定义分享"
                game_id = game_id or normalized_share_url

        if str(game_id or "").strip() == "switch_emulator":
            game_title = SWITCH_EMULATOR_DISPLAY_NAME
            is_switch_entry = True

        settings = self.store.settings
        target_download = (download_dir or settings.download_dir or self.plugin.downloads_dir).strip()
        if not target_download:
            raise ValueError("下载目录为空")
        target_download = os.path.realpath(os.path.expanduser(target_download))
        os.makedirs(target_download, exist_ok=True)

        target_install = (install_dir or settings.install_dir or target_download).strip()
        if not target_install:
            raise ValueError("安装目录为空")
        target_install = os.path.realpath(os.path.expanduser(target_install))
        os.makedirs(target_install, exist_ok=True)

        provider = _detect_share_provider(share_url)
        if not provider:
            raise TianyiApiError("不支持的分享链接（仅支持天翼云盘 / 百度网盘 / CTFile）", diagnostics={"share_url": share_url})

        effective_steam_app_id = max(0, _safe_int(catalog_app_id, 0))

        share_ctx: Dict[str, Any] = {}
        plan_ctfile_token_override = ""
        if provider == "tianyi":
            login_ok, _, message = await self.check_login_state()
            if not login_ok:
                raise TianyiApiError(message or "请先登录天翼账号")
            try:
                resolved, share_ctx, _canonical_url = await self._resolve_share_cached(
                    provider="tianyi",
                    share_url=share_url,
                    tianyi_cookie=self.store.login.cookie,
                )
            except TianyiApiError as exc:
                diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
                diagnostics.setdefault("stage", "resolve_share")
                raise TianyiApiError(str(exc), diagnostics=diagnostics) from exc
        elif provider == "baidu":
            try:
                resolved, share_ctx, canonical_url = await self._resolve_share_cached(
                    provider="baidu",
                    share_url=share_url,
                    baidu_cookie=getattr(self.store, "baidu_login", None) and self.store.baidu_login.cookie or "",
                )
                if canonical_url:
                    share_url = canonical_url
            except BaiduApiError as exc:
                diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
                diagnostics.setdefault("stage", "resolve_share")
                raise TianyiApiError(str(exc), diagnostics=diagnostics) from exc
        elif provider == "ctfile":
            ctfile_token = str(getattr(self.store, "ctfile_login", None) and self.store.ctfile_login.token or "").strip()
            # 支持从分享链接 query 中提取 token（不写入 state.json，仅用于本次解析/下载）。
            if not ctfile_token:
                plan_ctfile_token_override = _extract_ctfile_token_from_share_url(share_url)
                ctfile_token = plan_ctfile_token_override
            try:
                resolved, share_ctx, canonical_url = await self._resolve_share_cached(
                    provider="ctfile",
                    share_url=share_url,
                    ctfile_token=ctfile_token,
                )
            except CtfileApiError as exc:
                diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
                diagnostics.setdefault("stage", "resolve_share")
                raise TianyiApiError(str(exc), diagnostics=diagnostics) from exc
            # 解析成功后将分享链接规范化为 canonical_url，避免 token 泄漏到 plan/task 记录。
            if canonical_url:
                share_url = str(canonical_url or "").strip() or share_url
        else:
            raise TianyiApiError("暂不支持的分享链接", diagnostics={"share_url": share_url, "provider": provider})

        try:
            resolved_files = list(resolved.files or [])
            folder_count = 0
            preview_names: List[str] = []
            for item in resolved_files[:12]:
                if bool(getattr(item, "is_folder", False)):
                    folder_count += 1
                name = str(getattr(item, "name", "") or "").strip()
                if name:
                    preview_names.append(name)
            preview_text = ", ".join(preview_names[:8]) + ("..." if len(preview_names) > 8 else "")
            config.logger.info(
                "Resolve share ok: game_id=%s title=%s share_code=%s share_id=%s files=%s folders=%s preview=%s",
                game_id,
                game_title,
                str(getattr(resolved, "share_code", "") or ""),
                str(getattr(resolved, "share_id", "") or ""),
                len(resolved_files),
                folder_count,
                preview_text,
            )
        except Exception:
            pass
        selected = {str(v).strip() for v in (file_ids or []) if str(v).strip()}
        if selected:
            config.logger.info("Install plan selected file ids: game_id=%s count=%s", game_id, len(selected))

        if selected:
            # 如果用户只勾选了某一个分卷，自动扩展为整组分卷，避免“缺少主卷/缺少分卷”导致无法解压。
            by_id: Dict[str, Any] = {}
            for item in list(resolved.files or []):
                file_id = str(getattr(item, "file_id", "") or "").strip()
                if not file_id:
                    continue
                by_id[file_id] = item

            expanded = set(selected)
            for selected_id in list(selected):
                item = by_id.get(selected_id)
                if item is None:
                    continue
                name = str(getattr(item, "name", "") or "").strip()
                if not name:
                    continue
                lower_name = name.lower()

                # Pattern: foo.part1.rar / foo.part01.rar
                m = MULTIPART_RAR_PART_RE.match(name)
                if m:
                    prefix = str(m.group("prefix") or "").strip()
                    part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.part\d{{1,4}}\.rar$")
                    for candidate in list(resolved.files or []):
                        if bool(getattr(candidate, "is_folder", False)):
                            continue
                        candidate_name = str(getattr(candidate, "name", "") or "").strip()
                        if not candidate_name or not part_re.match(candidate_name):
                            continue
                        candidate_id = str(getattr(candidate, "file_id", "") or "").strip()
                        if candidate_id:
                            expanded.add(candidate_id)
                    continue

                # Pattern: foo.7z.001 / foo.zip.001 / foo.rar.001
                m = MULTIPART_NUMBERED_ARCHIVE_RE.match(name)
                if m:
                    base = str(m.group("base") or "").strip()
                    split_re = re.compile(rf"(?i)^{re.escape(base)}\.\d{{2,4}}$")
                    for candidate in list(resolved.files or []):
                        if bool(getattr(candidate, "is_folder", False)):
                            continue
                        candidate_name = str(getattr(candidate, "name", "") or "").strip()
                        if not candidate_name or not split_re.match(candidate_name):
                            continue
                        candidate_id = str(getattr(candidate, "file_id", "") or "").strip()
                        if candidate_id:
                            expanded.add(candidate_id)
                    continue

                # Pattern: foo.z01 + foo.zip
                m = MULTIPART_ZIP_Z_RE.match(name)
                if m:
                    prefix = str(m.group("prefix") or "").strip()
                    primary_name = f"{prefix}.zip"
                    part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.z\d{{2}}$")
                    for candidate in list(resolved.files or []):
                        if bool(getattr(candidate, "is_folder", False)):
                            continue
                        candidate_name = str(getattr(candidate, "name", "") or "").strip()
                        if not candidate_name:
                            continue
                        if candidate_name.lower() == primary_name.lower() or part_re.match(candidate_name):
                            candidate_id = str(getattr(candidate, "file_id", "") or "").strip()
                            if candidate_id:
                                expanded.add(candidate_id)
                    continue

                # Pattern: foo.r00 + foo.rar
                m = MULTIPART_RAR_R_RE.match(name)
                if m:
                    prefix = str(m.group("prefix") or "").strip()
                    primary_name = f"{prefix}.rar"
                    part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.r\d{{2}}$")
                    for candidate in list(resolved.files or []):
                        if bool(getattr(candidate, "is_folder", False)):
                            continue
                        candidate_name = str(getattr(candidate, "name", "") or "").strip()
                        if not candidate_name:
                            continue
                        if candidate_name.lower() == primary_name.lower() or part_re.match(candidate_name):
                            candidate_id = str(getattr(candidate, "file_id", "") or "").strip()
                            if candidate_id:
                                expanded.add(candidate_id)
                    continue

                # Primary: foo.zip has foo.z01 parts.
                if lower_name.endswith(".zip"):
                    prefix = name[:-4]
                    part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.z\d{{2}}$")
                    has_parts = False
                    for candidate in list(resolved.files or []):
                        if bool(getattr(candidate, "is_folder", False)):
                            continue
                        candidate_name = str(getattr(candidate, "name", "") or "").strip()
                        if candidate_name and part_re.match(candidate_name):
                            has_parts = True
                            break
                    if has_parts:
                        for candidate in list(resolved.files or []):
                            if bool(getattr(candidate, "is_folder", False)):
                                continue
                            candidate_name = str(getattr(candidate, "name", "") or "").strip()
                            if not candidate_name or not part_re.match(candidate_name):
                                continue
                            candidate_id = str(getattr(candidate, "file_id", "") or "").strip()
                            if candidate_id:
                                expanded.add(candidate_id)
                        continue

                # Primary: foo.rar has foo.r00 parts.
                if lower_name.endswith(".rar"):
                    prefix = name[:-4]
                    part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.r\d{{2}}$")
                    has_parts = False
                    for candidate in list(resolved.files or []):
                        if bool(getattr(candidate, "is_folder", False)):
                            continue
                        candidate_name = str(getattr(candidate, "name", "") or "").strip()
                        if candidate_name and part_re.match(candidate_name):
                            has_parts = True
                            break
                    if has_parts:
                        for candidate in list(resolved.files or []):
                            if bool(getattr(candidate, "is_folder", False)):
                                continue
                            candidate_name = str(getattr(candidate, "name", "") or "").strip()
                            if not candidate_name or not part_re.match(candidate_name):
                                continue
                            candidate_id = str(getattr(candidate, "file_id", "") or "").strip()
                            if candidate_id:
                                expanded.add(candidate_id)
                        continue

            selected = expanded

        files = [
            file_item
            for file_item in resolved.files
            if not file_item.is_folder and (not selected or file_item.file_id in selected)
        ]
        if not files:
            # CTFile 目录分享中的 “tempdir-...” file_id 可能会在两次解析间发生漂移，
            # 导致 start_install 重新 resolve 后无法匹配用户在 prepare 阶段勾选的 file_id。
            # 兜底策略：直接用用户选中的 file_id 调 getfile.php 取元信息，避免二次匹配失败。
            if provider == "ctfile" and selected:
                try:
                    ct_pwd = str(getattr(resolved, "pwd", "") or (share_ctx or {}).get("pwd") or "").strip()
                    ref_url = str((share_ctx or {}).get("canonical_url") or (share_ctx or {}).get("share_url") or share_url or "").strip()
                    recovered, recover_diag = await resolve_ctfile_file_infos(
                        list(selected),
                        pwd=ct_pwd,
                        token=ctfile_token,
                        ref_url=ref_url,
                    )
                    files = [item for item in recovered if not bool(getattr(item, "is_folder", False))]
                    if files:
                        try:
                            resolved.files = recovered
                        except Exception:
                            pass
                        try:
                            share_ctx.setdefault("selected_fallback", recover_diag)
                        except Exception:
                            pass
                except CtfileApiError as exc:
                    diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
                    diagnostics.setdefault("stage", "ctfile.batch_getfile")
                    diagnostics.setdefault("provider", "ctfile")
                    diagnostics.setdefault("selected_count", len(selected))
                    raise TianyiApiError(str(exc), diagnostics=diagnostics) from exc

            if not files:
                raise TianyiApiError("未找到可下载文件，可能所选条目是目录")

        try:
            preview = ", ".join(
                str(getattr(item, "name", "") or "").strip()
                for item in files[:8]
                if str(getattr(item, "name", "") or "").strip()
            )
            if len(files) > 8:
                preview = preview + "..."
            config.logger.info("Install plan files: game_id=%s count=%s preview=%s", game_id, len(files), preview)
        except Exception:
            pass

        required_download_bytes = sum(max(0, int(file_item.size or 0)) for file_item in files)

        # Gamebox 逆向版本会按压缩包大小估算“解压后体积”（例如 * 1.6），否则很容易出现：
        # 下载能下完，但解压/安装阶段因为空间不足导致 7z exit=2。
        # 这里用启发式估算：非压缩文件按原始体积；压缩包按系数放大。
        archive_bytes = 0
        for file_item in files:
            try:
                name = str(file_item.name or "").strip().lower()
                if not name:
                    continue
                if any(name.endswith(ext) for ext in ARCHIVE_SUFFIXES):
                    archive_bytes += max(0, int(file_item.size or 0))
                    continue
                if (
                    MULTIPART_NUMBERED_ARCHIVE_RE.match(name)
                    or MULTIPART_RAR_PART_RE.match(name)
                    or MULTIPART_ZIP_Z_RE.match(name)
                    or MULTIPART_RAR_R_RE.match(name)
                ):
                    archive_bytes += max(0, int(file_item.size or 0))
            except Exception:
                continue

        extract_factor = 1.6
        non_archive_bytes = max(0, required_download_bytes - archive_bytes)
        if archive_bytes > 0:
            estimated_extract_bytes = non_archive_bytes + int(round(archive_bytes * extract_factor))
            required_install_bytes = max(required_download_bytes, estimated_extract_bytes)
        else:
            required_install_bytes = required_download_bytes

        free_download_bytes = _disk_free_bytes(target_download)
        free_install_bytes = _disk_free_bytes(target_install)

        same_storage = False
        try:
            same_storage = os.stat(target_download).st_dev == os.stat(target_install).st_dev
        except Exception:
            same_storage = False

        if same_storage:
            total_required = required_download_bytes + required_install_bytes
            download_dir_ok = free_download_bytes >= total_required
            install_dir_ok = download_dir_ok
            can_install = bool(download_dir_ok)
        else:
            download_dir_ok = free_download_bytes >= required_download_bytes
            install_dir_ok = free_install_bytes >= required_install_bytes
            can_install = bool(download_dir_ok and install_dir_ok)

        plan_files: List[Dict[str, Any]] = []
        for file_item in files:
            plan_files.append(
                {
                    "file_id": str(file_item.file_id or ""),
                    "name": str(file_item.name or ""),
                    "size": max(0, int(file_item.size or 0)),
                    "is_folder": bool(file_item.is_folder),
                }
            )

        plan = {
            "game_id": game_id,
            "game_title": game_title,
            "openpath": open_path,
            "provider": provider,
            "share_url": share_url,
            "steam_app_id": int(effective_steam_app_id),
            "share_code": resolved.share_code,
            "share_id": resolved.share_id,
            "pwd": resolved.pwd,
            "share_ctx": share_ctx,
            "download_dir": target_download,
            "install_dir": target_install,
            "required_download_bytes": required_download_bytes,
            "required_install_bytes": required_install_bytes,
            "required_download_human": _format_size_bytes(required_download_bytes),
            "required_install_human": _format_size_bytes(required_install_bytes),
            "free_download_bytes": free_download_bytes,
            "free_install_bytes": free_install_bytes,
            "free_download_human": _format_size_bytes(free_download_bytes),
            "free_install_human": _format_size_bytes(free_install_bytes),
            "download_dir_ok": download_dir_ok,
            "install_dir_ok": install_dir_ok,
            "can_install": can_install,
            "file_count": len(plan_files),
            "files": plan_files,
        }

        if include_sensitive and provider == "ctfile" and plan_ctfile_token_override:
            # 注意：该字段仅供 start_install 内部使用，不得持久化/返回给前端。
            plan["_ctfile_token_override"] = plan_ctfile_token_override

        return plan

    async def _create_tasks_from_plan(self, *, plan: Dict[str, Any], split: int) -> List[TianyiTaskRecord]:
        """根据安装计划创建 aria2 下载任务。"""
        await self.aria2.ensure_running()
        settings = self.store.settings
        fast_mode = bool(getattr(settings, "aria2_fast_mode", False))
        max_connection_per_server = 32 if fast_mode else 16
        # 对齐 GameBox：极速模式下减少小分片抖动，并增加磁盘缓存。
        min_split_size = "20M" if fast_mode else "1M"
        disk_cache = "32M" if fast_mode else ""
        disable_ipv6 = bool(getattr(settings, "force_ipv4", True))

        provider = str(plan.get("provider", "") or "tianyi").strip() or "tianyi"
        share_ctx = plan.get("share_ctx") if isinstance(plan.get("share_ctx"), dict) else {}
        plan_share_url = str(plan.get("share_url", "") or "").strip()

        if provider == "tianyi":
            access_token = await fetch_access_token(self.store.login.cookie)
            provider_cookie = str(self.store.login.cookie or "").strip()
            referer = "https://cloud.189.cn/"
            user_agent = ""
            ctfile_token = ""
        elif provider == "baidu":
            provider_cookie = str(getattr(self.store, "baidu_login", None) and self.store.baidu_login.cookie or "").strip()
            if "BDUSS=" not in provider_cookie:
                raise TianyiApiError("请先登录百度网盘账号")
            access_token = ""
            # 百度直链下载对 UA 有要求（通常需要 User-Agent: pan.baidu.com）。
            user_agent = "pan.baidu.com"
            referer = "https://pan.baidu.com/disk/home"
            ctfile_token = ""
        elif provider == "ctfile":
            provider_cookie = ""
            access_token = ""
            ctfile_token = str(getattr(self.store, "ctfile_login", None) and self.store.ctfile_login.token or "").strip()
            if not ctfile_token:
                ctfile_token = str(plan.get("_ctfile_token_override", "") or "").strip()
            # CTFile 下载通常需要 Referer，且更偏好桌面浏览器 UA。
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            referer = "https://www.ctfile.com/"
        else:
            raise TianyiApiError("暂不支持的分享链接", diagnostics={"provider": provider, "share_url": plan_share_url})

        share_id = str(plan.get("share_id", "")).strip()
        share_code = str(plan.get("share_code", "")).strip()
        game_id = str(plan.get("game_id", "")).strip()
        game_title = str(plan.get("game_title", "")).strip() or game_id or "未命名游戏"
        open_path = str(plan.get("openpath", "") or "")
        target_dir = str(plan.get("download_dir", "")).strip()

        ctfile_limit_hint: Dict[str, Any] = {}
        created: List[TianyiTaskRecord] = []
        for file_item in plan.get("files", []):
            if not isinstance(file_item, dict):
                continue
            file_id = str(file_item.get("file_id", "")).strip()
            name = str(file_item.get("name", "")).strip() or f"file-{file_id}"
            file_size = max(0, _safe_int(file_item.get("size"), 0))
            if not file_id:
                continue

            ctfile_hint: Dict[str, Any] = {}
            if provider == "tianyi":
                direct_url = await fetch_download_url(
                    provider_cookie,
                    access_token,
                    share_id,
                    file_id,
                )
                task_cookie = provider_cookie
            elif provider == "ctfile":
                direct_url = await fetch_ctfile_download_url(ctfile_token, share_ctx, file_id)
                ctfile_hint = _ctfile_direct_url_limit_hint(direct_url)
                if not ctfile_limit_hint and bool(ctfile_hint.get("limited")):
                    ctfile_limit_hint = ctfile_hint
                    config.logger.info(
                        "CTFile direct url limited: host=%s limit=%s spd=%s spd2=%s threshold=%s",
                        str(ctfile_hint.get("host") or "-"),
                        int(ctfile_hint.get("limit") or 0),
                        int(ctfile_hint.get("spd") or 0),
                        int(ctfile_hint.get("spd2") or 0),
                        int(ctfile_hint.get("threshold") or 0),
                    )
                # 直链下载阶段不发送 token/cookie。
                task_cookie = ""
            else:
                direct_url = await fetch_baidu_download_url(provider_cookie, share_ctx, file_id)
                # 解析阶段用登录 Cookie 获取直链；下载阶段使用签名 URL，不需要把登录 Cookie 发送给下载域名。
                task_cookie = ""
            effective_split = _choose_aria2_split(size_bytes=file_size, preferred_split=split)
            effective_max_conn = max_connection_per_server
            if provider == "ctfile":
                # CTFile 普通用户直链常见“单连接 + 低速”，且 link 里可能包含 limit=1。
                # 避免 aria2 盲目开多连接导致 503/重试风暴：
                # - 若 limit=1：强制 split=1
                # - 若 limit>1：split 不超过 limit
                # - 否则：大文件按用户 split，小文件不分片
                link_limit = max(0, int(ctfile_hint.get("limit") or 0))
                mib = 1024 * 1024
                preferred = max(1, min(64, int(split or 1)))
                effective_split = preferred if file_size <= 0 or file_size >= 128 * mib else 1
                if link_limit == 1:
                    effective_split = 1
                    effective_max_conn = 1
                elif link_limit > 1:
                    effective_split = min(effective_split, link_limit)
                    effective_max_conn = min(max(effective_max_conn, effective_split), link_limit)
                else:
                    effective_max_conn = max(effective_max_conn, effective_split)
            gid = await self.aria2.add_uri(
                direct_url=direct_url,
                cookie=task_cookie,
                download_dir=target_dir,
                out_name=name,
                split=effective_split,
                max_connection_per_server=effective_max_conn,
                min_split_size=min_split_size,
                disk_cache=disk_cache,
                disable_ipv6=disable_ipv6,
                referer=referer,
                user_agent=user_agent,
            )
            now = _now_wall_ts()
            created.append(
                TianyiTaskRecord(
                    task_id=str(uuid.uuid4()),
                    gid=gid,
                    game_id=game_id,
                    game_title=game_title,
                    provider=provider,
                    share_code=share_code,
                    share_id=share_id,
                    share_url=plan_share_url,
                    share_ctx=_compact_task_share_ctx(provider, share_ctx, file_id),
                    file_id=file_id,
                    file_name=name,
                    file_size=file_size,
                    download_dir=target_dir,
                    local_path=os.path.join(target_dir, name),
                    status="waiting",
                    progress=0.0,
                    speed=0,
                    openpath=open_path,
                    created_at=now,
                    updated_at=now,
                )
            )

        # CTFile 直链限速提示（给前端展示）。
        if provider == "ctfile" and bool(ctfile_limit_hint.get("limited")):
            limit = int(ctfile_limit_hint.get("limit") or 0)
            spd = int(ctfile_limit_hint.get("spd") or 0)
            spd2 = int(ctfile_limit_hint.get("spd2") or 0)
            threshold = int(ctfile_limit_hint.get("threshold") or 0)
            parts: List[str] = []
            if threshold > 0:
                parts.append(f"超过≈{_format_size_bytes(threshold)}后")
            if spd > 0:
                parts.append(f"spd≈{max(1, int(round(spd / 1024)))}KB/s/连接")
            if spd2 > 0:
                parts.append(f"spd2≈{max(1, int(round(spd2 / 1024)))}KB/s/连接")
            if limit == 1:
                parts.append("单连接")
            elif limit > 1:
                parts.append(f"最多{limit}连接")
            info = "，".join(parts) if parts else "直链疑似限速"
            advice = "调 aria2 分片无效" if limit == 1 else "可调高 aria2 分片数/启用极速模式"
            token_note = "；若账号是 VIP 会更快"
            if not ctfile_token:
                token_note = "；若有 VIP 可在设置→账号→CTFile 配置 token"
            plan["provider_notice"] = f"CTFile 官方限速（{info}），{advice}{token_note}。"
        return created

    async def refresh_tasks(self, sync_aria2: bool = True, persist: bool = True) -> List[Dict[str, Any]]:
        """刷新任务列表并同步状态。"""
        tasks = list(self.store.tasks)
        if sync_aria2 and tasks:
            try:
                await self.aria2.ensure_running()
            except Exception as exc:
                config.logger.warning("aria2 ensure_running failed: %s", exc)

            access_token: Optional[str] = None

            async def _get_access_token() -> str:
                nonlocal access_token
                if access_token is None:
                    access_token = await fetch_access_token(self.store.login.cookie)
                return access_token

            def _can_retry(task: TianyiTaskRecord) -> Tuple[bool, str]:
                provider = str(getattr(task, "provider", "tianyi") or "tianyi").strip().lower()
                if provider != "tianyi":
                    return False, f"provider={provider}"
                task_id = str(getattr(task, "task_id", "") or "").strip()
                if not task_id:
                    return False, "task_id 为空"
                if not str(getattr(task, "share_id", "") or "").strip():
                    return False, "share_id 为空"
                if not str(getattr(task, "file_id", "") or "").strip():
                    return False, "file_id 为空"
                if not str(getattr(task, "file_name", "") or "").strip():
                    return False, "file_name 为空"
                if not str(getattr(task, "download_dir", "") or "").strip():
                    return False, "download_dir 为空"
                return True, ""

            async def _retry_task(task: TianyiTaskRecord, *, reason: str, error_message: str = "") -> bool:
                ok, why_not = _can_retry(task)
                if not ok:
                    config.logger.warning("Skip auto retry: %s (%s)", reason, why_not)
                    return False

                task_id = str(getattr(task, "task_id", "") or "").strip()
                now = time.monotonic()
                state = self._download_retry_state.get(task_id) or {"attempts": 0, "last_at": 0.0}
                attempts = max(0, _safe_int(state.get("attempts"), 0))
                last_at = float(state.get("last_at") or 0.0)
                if now - last_at < 4.0:
                    return False
                if attempts >= 3:
                    return False
                attempts += 1
                state = {"attempts": attempts, "last_at": now}
                self._download_retry_state[task_id] = state

                cookie = str(self.store.login.cookie or "").strip()
                if not cookie:
                    return False

                try:
                    os.makedirs(str(task.download_dir or "").strip(), exist_ok=True)
                except Exception:
                    pass

                settings = self.store.settings
                preferred_split = int(getattr(settings, "split_count", 16) or 16)
                if bool(getattr(settings, "aria2_fast_mode", False)):
                    preferred_split = max(preferred_split, 32)
                file_size = max(0, _safe_int(getattr(task, "file_size", 0), 0))
                effective_split = _choose_aria2_split(size_bytes=file_size, preferred_split=preferred_split)
                fast_mode = bool(getattr(settings, "aria2_fast_mode", False))
                max_connection_per_server = 32 if fast_mode else 16
                min_split_size = "20M" if fast_mode else "1M"
                disk_cache = "32M" if fast_mode else ""
                disable_ipv6 = bool(getattr(settings, "force_ipv4", True))

                old_gid = str(getattr(task, "gid", "") or "").strip()
                if old_gid:
                    try:
                        await self.aria2.remove(old_gid)
                    except Exception:
                        pass

                try:
                    token = await _get_access_token()
                    direct_url = await fetch_download_url(cookie, token, str(task.share_id or ""), str(task.file_id or ""))
                    new_gid = await self.aria2.add_uri(
                        direct_url=direct_url,
                        cookie=cookie,
                        download_dir=str(task.download_dir or ""),
                        out_name=str(task.file_name or ""),
                        split=effective_split,
                        max_connection_per_server=max_connection_per_server,
                        min_split_size=min_split_size,
                        disk_cache=disk_cache,
                        disable_ipv6=disable_ipv6,
                    )
                except Exception as exc:
                    task.status = "error"
                    task.error_reason = f"自动重试失败({attempts}/3): {exc}"
                    task.updated_at = _now_wall_ts()
                    return False

                task.gid = new_gid
                task.status = "waiting"
                task.progress = 0.0
                task.speed = 0
                task.error_reason = ""
                task.updated_at = _now_wall_ts()

                config.logger.info(
                    "Auto retry task: task_id=%s reason=%s attempt=%s/%s old_gid=%s new_gid=%s msg=%s",
                    task_id,
                    reason,
                    attempts,
                    3,
                    old_gid,
                    new_gid,
                    (error_message or "")[:180],
                )
                return True

            # 慢速检测 → 自动切线路/重取直链（可配置开关）
            SLOW_SPEED_THRESHOLD_BPS = 512 * 1024  # 512 KiB/s
            SLOW_SPEED_MIN_SECONDS = 30.0
            SLOW_SWITCH_COOLDOWN_SECONDS = 60.0
            SLOW_SWITCH_MAX_ATTEMPTS = 3
            SLOW_SWITCH_MIN_FILE_BYTES = 512 * 1024 * 1024  # 512 MiB
            SLOW_SWITCH_MIN_COMPLETED_BYTES = 16 * 1024 * 1024  # 16 MiB
            BAIDU_SLOW_SPEED_THRESHOLD_BPS = 300 * 1024  # 300 KiB/s
            BAIDU_SLOW_SPEED_MIN_SECONDS = 20.0
            BAIDU_SLOW_SWITCH_COOLDOWN_SECONDS = 60.0
            BAIDU_SLOW_SWITCH_MAX_ATTEMPTS = 10
            BAIDU_SLOW_SWITCH_MIN_FILE_BYTES = 64 * 1024 * 1024  # 64 MiB
            BAIDU_SLOW_SWITCH_MIN_COMPLETED_BYTES = 4 * 1024 * 1024  # 4 MiB

            async def _maybe_switch_line(
                task: TianyiTaskRecord,
                *,
                status: str,
                total: int,
                completed: int,
                speed: int,
            ) -> bool:
                """慢速时自动刷新直链；返回 True 表示已重建 gid（调用方应 continue）。"""
                task_id = str(getattr(task, "task_id", "") or "").strip()
                if not task_id:
                    return False
                provider = str(getattr(task, "provider", "tianyi") or "tianyi").strip().lower()
                if provider not in {"tianyi", "baidu"}:
                    return False

                def _clear_slow_notice() -> None:
                    if provider != "baidu":
                        return
                    current_notice = str(getattr(task, "notice", "") or "").strip()
                    if current_notice.startswith("疑似百度网盘限速/线路问题："):
                        task.notice = ""

                settings = self.store.settings
                if not bool(getattr(settings, "auto_switch_line", True)):
                    self._download_slow_state.pop(task_id, None)
                    _clear_slow_notice()
                    return False

                slow_speed_threshold = SLOW_SPEED_THRESHOLD_BPS
                slow_speed_min_seconds = SLOW_SPEED_MIN_SECONDS
                slow_switch_cooldown_seconds = SLOW_SWITCH_COOLDOWN_SECONDS
                slow_switch_max_attempts = SLOW_SWITCH_MAX_ATTEMPTS
                slow_switch_min_file_bytes = SLOW_SWITCH_MIN_FILE_BYTES
                slow_switch_min_completed_bytes = SLOW_SWITCH_MIN_COMPLETED_BYTES
                if provider == "baidu":
                    slow_speed_threshold = BAIDU_SLOW_SPEED_THRESHOLD_BPS
                    slow_speed_min_seconds = BAIDU_SLOW_SPEED_MIN_SECONDS
                    slow_switch_cooldown_seconds = BAIDU_SLOW_SWITCH_COOLDOWN_SECONDS
                    slow_switch_max_attempts = BAIDU_SLOW_SWITCH_MAX_ATTEMPTS
                    slow_switch_min_file_bytes = BAIDU_SLOW_SWITCH_MIN_FILE_BYTES
                    slow_switch_min_completed_bytes = BAIDU_SLOW_SWITCH_MIN_COMPLETED_BYTES

                if status != "active":
                    state = self._download_slow_state.get(task_id)
                    if state and float(state.get("slow_since") or 0.0) > 0.0:
                        state["slow_since"] = 0.0
                        self._download_slow_state[task_id] = state
                    _clear_slow_notice()
                    return False

                task_file_size = max(0, _safe_int(getattr(task, "file_size", 0), 0))
                effective_total = max(0, int(max(total, task_file_size)))
                if effective_total < slow_switch_min_file_bytes:
                    return False
                if effective_total <= 0 or completed < slow_switch_min_completed_bytes:
                    return False

                if speed >= slow_speed_threshold:
                    state = self._download_slow_state.get(task_id)
                    if state and float(state.get("slow_since") or 0.0) > 0.0:
                        state["slow_since"] = 0.0
                        self._download_slow_state[task_id] = state
                    _clear_slow_notice()
                    return False

                now = time.monotonic()
                state = self._download_slow_state.get(task_id) or {
                    "slow_since": 0.0,
                    "last_switch_at": 0.0,
                    "switch_attempts": 0,
                    "last_host": "",
                }
                slow_since = float(state.get("slow_since") or 0.0)
                if slow_since <= 0.0:
                    state["slow_since"] = now
                    self._download_slow_state[task_id] = state
                    config.logger.info(
                        "Auto switch line slow detected: task_id=%s provider=%s speed=%s completed=%s/%s",
                        task_id,
                        provider,
                        speed,
                        completed,
                        effective_total,
                    )
                    return False
                if now - slow_since < slow_speed_min_seconds:
                    self._download_slow_state[task_id] = state
                    return False

                last_switch_at = float(state.get("last_switch_at") or 0.0)
                if last_switch_at > 0.0 and now - last_switch_at < slow_switch_cooldown_seconds:
                    return False

                attempts = max(0, _safe_int(state.get("switch_attempts"), 0))
                if attempts >= slow_switch_max_attempts:
                    if provider == "baidu":
                        message = (
                            f"疑似百度网盘限速/线路问题：已自动切线路达到上限（{slow_switch_max_attempts} 次）仍低速。"
                            "可尝试：更换网络/热点、关闭“强制 IPv4”以允许 IPv6、使用百度官方客户端或会员。"
                        )
                        current_notice = str(getattr(task, "notice", "") or "").strip()
                        if current_notice != message:
                            task.notice = message
                            task.updated_at = _now_wall_ts()
                    return False

                try:
                    os.makedirs(str(task.download_dir or "").strip(), exist_ok=True)
                except Exception:
                    pass

                file_id = str(getattr(task, "file_id", "") or "").strip()
                if not file_id:
                    return False

                gid = str(getattr(task, "gid", "") or "").strip()
                if not gid:
                    return False

                # provider 相关准备（cookie / share_ctx / header）
                tianyi_cookie = ""
                baidu_cookie = ""
                share_id = str(getattr(task, "share_id", "") or "").strip()
                share_url = ""
                ctx: Dict[str, Any] = {}
                task_cookie = ""
                task_referer = ""
                task_user_agent = ""

                if provider == "tianyi":
                    tianyi_cookie = str(self.store.login.cookie or "").strip()
                    if not tianyi_cookie:
                        return False
                    if not share_id:
                        return False
                    task_cookie = tianyi_cookie
                    task_referer = "https://cloud.189.cn/"
                    task_user_agent = ""
                else:
                    baidu_cookie = str(
                        getattr(self.store, "baidu_login", None) and self.store.baidu_login.cookie or ""
                    ).strip()
                    if "BDUSS=" not in baidu_cookie:
                        return False
                    share_url = str(getattr(task, "share_url", "") or "").strip()
                    if not share_url:
                        maybe_share_url = str(getattr(task, "game_id", "") or "").strip()
                        if maybe_share_url and _detect_share_provider(maybe_share_url) == "baidu":
                            share_url = maybe_share_url
                    if not share_url:
                        return False

                    task_cookie = ""
                    task_referer = "https://pan.baidu.com/disk/home"
                    task_user_agent = "pan.baidu.com"

                    ctx_raw = getattr(task, "share_ctx", None)
                    ctx = ctx_raw if isinstance(ctx_raw, dict) else {}
                    ctx_ok = False
                    try:
                        ctx_ok = bool(
                            str(ctx.get("share_url", "") or "").strip()
                            and int(ctx.get("share_id") or 0) > 0
                            and int(ctx.get("uk") or 0) > 0
                        )
                    except Exception:
                        ctx_ok = False
                    if not ctx_ok:
                        try:
                            _, resolved_ctx = await resolve_baidu_share(share_url, baidu_cookie)
                            # 合并旧的 transfer 缓存（避免重复保存到网盘）
                            old_transfer_cache = ctx.get("_transfer_cache") if isinstance(ctx.get("_transfer_cache"), dict) else {}
                            old_transfer_meta = (
                                ctx.get("_transfer_cache_meta") if isinstance(ctx.get("_transfer_cache_meta"), dict) else {}
                            )
                            if file_id and file_id in old_transfer_cache:
                                resolved_tc = resolved_ctx.get("_transfer_cache")
                                if not isinstance(resolved_tc, dict):
                                    resolved_tc = {}
                                    resolved_ctx["_transfer_cache"] = resolved_tc
                                resolved_tc[file_id] = old_transfer_cache.get(file_id)
                            if file_id and file_id in old_transfer_meta:
                                resolved_tm = resolved_ctx.get("_transfer_cache_meta")
                                if not isinstance(resolved_tm, dict):
                                    resolved_tm = {}
                                    resolved_ctx["_transfer_cache_meta"] = resolved_tm
                                resolved_tm[file_id] = old_transfer_meta.get(file_id)
                            ctx = resolved_ctx
                        except Exception as exc:
                            config.logger.info(
                                "Auto switch line skipped: task_id=%s reason=resolve_share_failed err=%s",
                                task_id,
                                exc,
                            )
                            return False
                    else:
                        # 兜底：确保 ctx 内有 share_url，便于后续 fetch_baidu_download_url 使用
                        if share_url and not str(ctx.get("share_url", "") or "").strip():
                            ctx["share_url"] = share_url

                current_host = ""
                try:
                    current_uris = await self.aria2.get_uris(gid)
                    current_uri = str(current_uris[0] or "").strip() if current_uris else ""
                    if current_uri:
                        current_host = str(urlparse(current_uri).netloc or "").strip().lower()
                except Exception:
                    current_host = ""
                last_known_host = str(state.get("last_host") or "").strip().lower()
                if current_host:
                    state["last_host"] = current_host
                effective_current_host = current_host or last_known_host

                direct_url = ""
                try:
                    last_fetch_error: Optional[BaseException] = None
                    token = await _get_access_token() if provider == "tianyi" else ""
                    for attempt_idx in range(3):
                        try:
                            if provider == "tianyi":
                                candidate = await fetch_download_url(tianyi_cookie, token, share_id, file_id)
                            else:
                                candidate = await fetch_baidu_download_url(baidu_cookie, ctx, file_id)
                        except Exception as exc:
                            last_fetch_error = exc
                            if attempt_idx < 2:
                                await asyncio.sleep(0.3)
                            continue
                        if not candidate:
                            continue
                        candidate_host = str(urlparse(candidate).netloc or "").strip().lower()
                        if current_host and candidate_host and candidate_host != current_host:
                            direct_url = candidate
                            break
                        if not direct_url:
                            direct_url = candidate
                        if attempt_idx < 2:
                            await asyncio.sleep(0.3)

                    if not direct_url:
                        raise RuntimeError(f"fetch_download_url_failed: {last_fetch_error}")
                except Exception as exc:
                    config.logger.info(
                        "Auto switch line skipped: task_id=%s reason=fetch_url_failed err=%s",
                        task_id,
                        exc,
                    )
                    return False

                if provider == "baidu":
                    try:
                        task.share_url = share_url
                        task.share_ctx = _compact_task_share_ctx(provider, ctx, file_id)
                    except Exception:
                        pass

                new_host = ""
                try:
                    new_host = str(urlparse(direct_url).netloc or "").strip().lower()
                except Exception:
                    new_host = ""
                host_changed = bool(new_host and effective_current_host and new_host != effective_current_host)

                # 优先尝试不重建任务：替换直链并继续下载
                try:
                    try:
                        await self.aria2.pause(gid)
                    except Exception:
                        pass
                    await self.aria2.replace_uri(gid, direct_url)
                    try:
                        await self.aria2.resume(gid)
                    except Exception:
                        pass

                    if host_changed:
                        attempts += 1
                        state["switch_attempts"] = attempts
                    if new_host:
                        state["last_host"] = new_host
                    state["last_switch_at"] = now
                    state["slow_since"] = 0.0
                    self._download_slow_state[task_id] = state
                    if host_changed:
                        config.logger.info(
                            "Auto switch line: task_id=%s attempt=%s/%s provider=%s host=%s->%s speed=%s completed=%s/%s",
                            task_id,
                            attempts,
                            slow_switch_max_attempts,
                            provider,
                            effective_current_host,
                            new_host or "-",
                            speed,
                            completed,
                            total,
                        )
                    else:
                        config.logger.info(
                            "Auto switch line refreshed (host unchanged): task_id=%s provider=%s host=%s speed=%s completed=%s/%s",
                            task_id,
                            provider,
                            new_host or effective_current_host or "-",
                            speed,
                            completed,
                            total,
                        )
                    return False
                except Exception as exc:
                    config.logger.info("Auto switch line replace_uri failed: task_id=%s err=%s", task_id, exc)

                # 回退：重建 aria2 任务（依赖 continue=true 断点续传）
                try:
                    if not direct_url:
                        if provider == "tianyi":
                            token = await _get_access_token()
                            direct_url = await fetch_download_url(tianyi_cookie, token, share_id, file_id)
                        else:
                            direct_url = await fetch_baidu_download_url(baidu_cookie, ctx, file_id)
                    preferred_split = int(getattr(settings, "split_count", 16) or 16)
                    if bool(getattr(settings, "aria2_fast_mode", False)):
                        preferred_split = max(preferred_split, 32)
                    effective_split = _choose_aria2_split(size_bytes=file_size, preferred_split=preferred_split)
                    fast_mode = bool(getattr(settings, "aria2_fast_mode", False))
                    max_connection_per_server = 32 if fast_mode else 16
                    min_split_size = "20M" if fast_mode else "1M"
                    disk_cache = "32M" if fast_mode else ""
                    disable_ipv6 = bool(getattr(settings, "force_ipv4", True))

                    try:
                        await self.aria2.remove(gid)
                    except Exception:
                        pass

                    new_gid = await self.aria2.add_uri(
                        direct_url=direct_url,
                        cookie=task_cookie,
                        download_dir=str(task.download_dir or ""),
                        out_name=str(task.file_name or ""),
                        split=effective_split,
                        max_connection_per_server=max_connection_per_server,
                        min_split_size=min_split_size,
                        disk_cache=disk_cache,
                        disable_ipv6=disable_ipv6,
                        referer=task_referer,
                        user_agent=task_user_agent,
                    )
                except Exception as exc:
                    config.logger.info("Auto switch line recreate failed: task_id=%s err=%s", task_id, exc)
                    return False

                if host_changed:
                    attempts += 1
                    state["switch_attempts"] = attempts
                if new_host:
                    state["last_host"] = new_host
                state["last_switch_at"] = now
                state["slow_since"] = 0.0
                self._download_slow_state[task_id] = state

                task.gid = new_gid
                task.status = "waiting"
                task.progress = 0.0
                task.speed = 0
                task.error_reason = ""
                task.updated_at = _now_wall_ts()

                config.logger.info(
                    (
                        "Auto switch line recreated: task_id=%s attempt=%s/%s provider=%s host_changed=%s old_gid=%s new_gid=%s"
                    ),
                    task_id,
                    attempts,
                    slow_switch_max_attempts,
                    provider,
                    host_changed,
                    gid,
                    new_gid,
                )
                return True

            for task in tasks:
                if _is_terminal(task.status):
                    if task.status == "complete" and not task.post_processed:
                        self._schedule_post_process_task(task.task_id)
                    continue
                try:
                    info = await self.aria2.tell_status(task.gid)
                    status = str(info.get("status", task.status) or task.status)
                    total = _safe_int(info.get("totalLength"), 0)
                    completed = _safe_int(info.get("completedLength"), 0)
                    speed = _safe_int(info.get("downloadSpeed"), 0)
                    progress = 0.0
                    if total > 0:
                        progress = (completed * 100.0) / total
                    if status == "complete":
                        progress = 100.0

                        expected_size = max(0, _safe_int(getattr(task, "file_size", 0), 0))
                        if expected_size > 0:
                            local_path = self._resolve_task_local_path(task)
                            try:
                                actual_size = os.path.getsize(local_path) if local_path and os.path.isfile(local_path) else -1
                            except Exception:
                                actual_size = -1
                            if actual_size >= 0 and actual_size != expected_size:
                                mismatch_reason = f"下载文件大小异常 expected={expected_size} got={actual_size}"
                                retried = await _retry_task(task, reason="size_mismatch", error_message=mismatch_reason)
                                if retried:
                                    continue
                                status = "error"
                                progress = 0.0
                                info["errorMessage"] = mismatch_reason

                    if status == "error":
                        error_msg = str(info.get("errorMessage", "") or "").strip()
                        if _is_transient_download_error(error_msg):
                            retried = await _retry_task(task, reason="transient_error", error_message=error_msg)
                            if retried:
                                continue
                    else:
                        switched = await _maybe_switch_line(
                            task,
                            status=status,
                            total=total,
                            completed=completed,
                            speed=speed,
                        )
                        if switched:
                            continue
                    task.status = status
                    task.progress = round(progress, 2)
                    task.speed = speed
                    task.error_reason = str(info.get("errorMessage", "") or "")
                    task.updated_at = _now_wall_ts()
                    if status == "complete" and not task.post_processed:
                        self._schedule_post_process_task(task.task_id)
                except Aria2Error as exc:
                    message = str(exc)
                    lower = message.lower()
                    is_missing_gid = ("gid" in lower and ("not found" in lower or "cannot find" in lower)) or "找不到" in message
                    if is_missing_gid:
                        retried = await _retry_task(task, reason="gid_missing", error_message=message)
                        if retried:
                            continue

                    if task.status in {"active", "waiting", "paused"}:
                        task.status = "error"
                        task.error_reason = message
                        task.updated_at = _now_wall_ts()

            self._cleanup_tasks(tasks)
            alive_ids = {str(getattr(t, "task_id", "") or "").strip() for t in tasks if not _is_terminal(t.status)}
            for key in list(self._download_slow_state.keys()):
                if key not in alive_ids:
                    self._download_slow_state.pop(key, None)
            for key in list(self._download_retry_state.keys()):
                if key not in alive_ids:
                    self._download_retry_state.pop(key, None)
            if persist:
                self.store.replace_tasks(tasks)
            else:
                self.store.tasks = list(tasks)
        else:
            self._cleanup_tasks(tasks)
            if tasks != self.store.tasks:
                if persist:
                    self.store.replace_tasks(tasks)
                else:
                    self.store.tasks = list(tasks)

        views = [_task_to_view(t) for t in tasks]
        self._panel_tasks_cache = list(views)
        self._panel_tasks_cache_at = time.monotonic()
        self._panel_last_active_tasks = self._count_active_tasks(views)
        return views

    def _schedule_post_process_task(self, task_id: str) -> None:
        """将下载后安装流程调度为后台任务，避免阻塞面板刷新。"""
        target = str(task_id or "").strip()
        if not target:
            return
        current = self._post_process_jobs.get(target)
        if current is not None and not current.done():
            return
        job = asyncio.create_task(
            self._run_post_process_task(target),
            name=f"freedeck_post_process_{target[:8]}",
        )
        self._post_process_jobs[target] = job

        def _cleanup(done_job: asyncio.Task, key: str = target) -> None:
            if self._post_process_jobs.get(key) is done_job:
                self._post_process_jobs.pop(key, None)

        job.add_done_callback(_cleanup)

    async def _run_post_process_task(self, task_id: str) -> None:
        """执行后台安装流程并在结束后持久化状态。"""
        target = str(task_id or "").strip()
        task = self._find_task(target)
        if task is None or task.post_processed:
            self._install_cancel_events.pop(target, None)
            return
        try:
            await self._post_process_completed_task(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            config.logger.exception("Post-process failed for task %s: %s", task_id, exc)
            task.post_processed = True
            task.install_status = "failed"
            task.install_message = f"安装流程异常: {exc}"
            task.updated_at = _now_wall_ts()
            self._invalidate_panel_cache(tasks=True)
        finally:
            self._install_cancel_events.pop(target, None)
            await asyncio.to_thread(self.store.save)

    async def pause_task(self, task_id: str) -> Dict[str, Any]:
        """暂停任务。"""
        task = self._find_task(task_id)
        if task is None:
            raise ValueError("任务不存在")
        await self.aria2.pause(task.gid)
        task.status = "paused"
        task.updated_at = _now_wall_ts()
        self.store.save()
        self._invalidate_panel_cache(tasks=True)
        return _task_to_view(task)

    async def resume_task(self, task_id: str) -> Dict[str, Any]:
        """恢复任务。"""
        task = self._find_task(task_id)
        if task is None:
            raise ValueError("任务不存在")
        await self.aria2.resume(task.gid)
        task.status = "active"
        task.updated_at = _now_wall_ts()
        self.store.save()
        self._invalidate_panel_cache(tasks=True)
        return _task_to_view(task)

    async def remove_task(self, task_id: str) -> Dict[str, Any]:
        """移除任务。"""
        task = self._find_task(task_id)
        if task is None:
            raise ValueError("任务不存在")
        await self.aria2.remove(task.gid)
        task.status = "removed"
        task.updated_at = _now_wall_ts()
        self._cleanup_tasks(self.store.tasks)
        self.store.save()
        self._invalidate_panel_cache(tasks=True)
        return _task_to_view(task)

    async def cancel_task(self, task_id: str, delete_files: bool = True) -> Dict[str, Any]:
        """取消下载任务并从列表移除（用于面板快速清理进度）。"""
        target = str(task_id or "").strip()
        if not target:
            raise ValueError("任务 ID 为空")
        local_path = ""
        gid = ""
        async with self._lock:
            task = self._find_task(target)
            if task is None:
                # 任务可能已被刷新清理，直接视为取消成功，避免面板提示异常。
                return {
                    "canceled": True,
                    "task_id": target,
                    "already_gone": True,
                    "deleted_paths": [],
                    "cleanup_pending": False,
                    "delete_files": bool(delete_files),
                }

            status = str(task.status or "").strip().lower()
            if status == "complete":
                raise ValueError("下载已完成，无法取消")

            local_path = str(task.local_path or "").strip()
            if not local_path:
                local_path = os.path.join(str(task.download_dir or "").strip(), str(task.file_name or "").strip())
            local_path = os.path.realpath(os.path.expanduser(local_path)) if local_path else ""
            gid = str(task.gid or "").strip()

            self.store.tasks = [item for item in self.store.tasks if item.task_id != target]
            self.store.save()
            self._invalidate_panel_cache(tasks=True)

        cleanup_pending = bool(gid or local_path)
        if cleanup_pending:
            scheduled_local_path = local_path
            scheduled_gid = gid
            scheduled_delete_files = bool(delete_files)

            async def _cleanup_job() -> None:
                removed = False
                if scheduled_gid:
                    for attempt in range(1, 4):
                        try:
                            await asyncio.wait_for(self.aria2.remove(scheduled_gid), timeout=3.0)
                            removed = True
                            break
                        except asyncio.TimeoutError:
                            await asyncio.sleep(0.4 * attempt)
                            continue
                        except Exception:
                            break
                deleted: List[str] = []
                if scheduled_delete_files:
                    candidates = {
                        scheduled_local_path,
                        f"{scheduled_local_path}.aria2" if scheduled_local_path else "",
                    }
                    for candidate in candidates:
                        if not candidate or not os.path.exists(candidate):
                            continue
                        last_exc: Optional[Exception] = None
                        for attempt in range(1, 6):
                            try:
                                os.remove(candidate)
                                deleted.append(candidate)
                                last_exc = None
                                break
                            except Exception as exc:
                                last_exc = exc
                                # 文件可能仍被 aria2 占用，稍等后重试。
                                try:
                                    await asyncio.sleep(0.2 * attempt)
                                except Exception:
                                    break
                        if last_exc is not None:
                            continue

                if deleted:
                    config.logger.info("Cancel task cleanup deleted: task_id=%s paths=%s", target, deleted)
                if not removed and scheduled_gid:
                    config.logger.warning("Cancel task cleanup pending: gid=%s task_id=%s", scheduled_gid, target)

            asyncio.create_task(_cleanup_job(), name=f"freedeck_cancel_cleanup_{target[:8]}")

        return {
            "canceled": True,
            "task_id": target,
            "deleted_paths": [],
            "cleanup_pending": cleanup_pending,
            "delete_files": bool(delete_files),
        }

    async def dismiss_task(self, task_id: str, delete_files: bool = False) -> Dict[str, Any]:
        """从列表移除已结束任务（可选删除文件）。"""
        target = str(task_id or "").strip()
        if not target:
            raise ValueError("任务 ID 为空")

        dismiss_task_ids: Set[str] = set()
        delete_candidates: List[Tuple[str, str]] = []
        is_bundle = False

        async with self._lock:
            task = self._find_task(target)
            if task is None:
                return {
                    "dismissed": True,
                    "task_id": target,
                    "already_gone": True,
                    "delete_files": bool(delete_files),
                    "deleted_paths": [],
                    "bundle": False,
                }

            status = str(getattr(task, "status", "") or "").strip().lower()
            install_status = str(getattr(task, "install_status", "") or "").strip().lower()
            if status not in {"complete", "error", "removed"}:
                raise ValueError("任务仍在进行中，无法清除")
            if status == "complete" and install_status in {"pending", "installing"}:
                raise ValueError("任务仍在安装中，无法清除")

            dismiss_task_ids = {target}
            if delete_files:
                bundle = self._resolve_multipart_archive_bundle(task)
                if bundle and isinstance(bundle.get("members"), list) and bundle.get("members"):
                    member_names = {
                        str(getattr(member, "file_name", "") or "").strip().lower()
                        for member in (bundle.get("members") or [])
                        if str(getattr(member, "file_name", "") or "").strip()
                    }
                    if member_names:
                        is_bundle = True
                        share_id = str(getattr(task, "share_id", "") or "").strip()
                        game_id = str(getattr(task, "game_id", "") or "").strip()
                        download_dir = str(getattr(task, "download_dir", "") or "").strip()
                        normalized_dir = os.path.realpath(os.path.expanduser(download_dir)) if download_dir else ""

                        dismiss_task_ids = set()
                        for item in list(self.store.tasks):
                            try:
                                if share_id and str(getattr(item, "share_id", "") or "").strip() != share_id:
                                    continue
                                if game_id and str(getattr(item, "game_id", "") or "").strip() != game_id:
                                    continue
                                item_dir = str(getattr(item, "download_dir", "") or "").strip()
                                if not item_dir:
                                    continue
                                if normalized_dir and os.path.realpath(os.path.expanduser(item_dir)) != normalized_dir:
                                    continue
                                item_name = str(getattr(item, "file_name", "") or "").strip().lower()
                                if item_name not in member_names:
                                    continue
                            except Exception:
                                continue
                            item_id = str(getattr(item, "task_id", "") or "").strip()
                            if item_id:
                                dismiss_task_ids.add(item_id)
                            path = self._resolve_task_local_path(item)
                            root = os.path.realpath(os.path.expanduser(item_dir)) if item_dir else ""
                            if path and root:
                                delete_candidates.append((path, root))

                if not delete_candidates:
                    # 单文件任务或无法识别分卷：仅尝试删除当前任务对应的下载文件。
                    download_dir = str(getattr(task, "download_dir", "") or "").strip()
                    root = os.path.realpath(os.path.expanduser(download_dir)) if download_dir else ""
                    path = self._resolve_task_local_path(task)
                    if path and root:
                        delete_candidates.append((path, root))

            dismiss_set = {tid for tid in dismiss_task_ids if tid}
            if dismiss_set:
                self.store.tasks = [
                    item
                    for item in self.store.tasks
                    if str(getattr(item, "task_id", "") or "").strip() not in dismiss_set
                ]
            else:
                self.store.tasks = [
                    item for item in self.store.tasks if str(getattr(item, "task_id", "") or "").strip() != target
                ]
            self.store.save()
            self._invalidate_panel_cache(tasks=True)

        deleted_paths: List[str] = []
        if delete_files and delete_candidates:
            deduped: Dict[str, str] = {}
            for path, root in delete_candidates:
                p = str(path or "").strip()
                r = str(root or "").strip()
                if not p or not r:
                    continue
                try:
                    real_p = os.path.realpath(os.path.expanduser(p))
                    real_r = os.path.realpath(os.path.expanduser(r))
                except Exception:
                    continue
                if not real_p or not real_r:
                    continue
                deduped[real_p] = real_r

            def can_delete(file_path: str, root_dir: str) -> bool:
                if not file_path or not root_dir:
                    return False
                try:
                    return os.path.commonpath([file_path, root_dir]) == root_dir
                except Exception:
                    return False

            for real_path, real_root in sorted(deduped.items()):
                for candidate in (real_path, f"{real_path}.aria2"):
                    if not candidate or not os.path.exists(candidate):
                        continue
                    if not can_delete(candidate, real_root):
                        continue
                    if not os.path.isfile(candidate):
                        continue
                    last_exc: Optional[Exception] = None
                    for attempt in range(1, 6):
                        try:
                            os.remove(candidate)
                            deleted_paths.append(candidate)
                            last_exc = None
                            break
                        except Exception as exc:
                            last_exc = exc
                            try:
                                await asyncio.sleep(0.15 * attempt)
                            except Exception:
                                break
                    if last_exc is not None:
                        continue

        return {
            "dismissed": True,
            "task_id": target,
            "delete_files": bool(delete_files),
            "deleted_paths": deleted_paths,
            "bundle": bool(is_bundle),
            "dismissed_task_ids": sorted({tid for tid in dismiss_task_ids if tid}),
        }

    async def cancel_install(self, task_id: str) -> Dict[str, Any]:
        """取消安装流程（解压/导入 Steam），用于面板快速终止安装进度。"""
        target = str(task_id or "").strip()
        if not target:
            raise ValueError("任务 ID 为空")
        async with self._lock:
            task = self._find_task(target)
            if task is None:
                raise ValueError("任务不存在")

            download_status = str(task.status or "").strip().lower()
            if download_status != "complete":
                raise ValueError("任务尚未进入安装阶段")

            install_status = str(task.install_status or "").strip().lower()
            if task.post_processed or install_status in {"installed", "failed", "skipped", "canceled", "bundled"}:
                raise ValueError("安装已结束，无法取消")

            event = self._install_cancel_events.get(target)
            if event is None:
                event = threading.Event()
                self._install_cancel_events[target] = event
            event.set()

            # 立即更新 UI 提示，后台任务会在下一次检查中退出。
            task.post_processed = True
            task.install_status = "canceled"
            task.install_message = "已取消安装"
            task.updated_at = _now_wall_ts()
            self._invalidate_panel_cache(tasks=True)
            await asyncio.to_thread(self.store.save)

            job = self._post_process_jobs.get(target)
            if job is None or job.done():
                self._install_cancel_events.pop(target, None)

            return _task_to_view(task)

    async def import_task_to_steam(self, *, task_id: str = "", exe_rel_path: str = "") -> Dict[str, Any]:
        """为已安装任务手动选择启动程序并导入 Steam。"""
        target_task_id = str(task_id or "").strip()
        rel = str(exe_rel_path or "").strip().replace("\\", "/").lstrip("/")
        if not target_task_id:
            raise ValueError("任务 ID 为空")
        if not rel:
            raise ValueError("可执行文件路径为空")

        task = None
        install_root = ""
        exe_path = ""
        async with self._lock:
            task = self._find_task(target_task_id)
            if task is None:
                raise ValueError("任务不存在")
            install_root = str(getattr(task, "installed_path", "") or "").strip()
            if not install_root:
                raise ValueError("安装尚未完成，未找到安装目录")

            install_root = os.path.realpath(os.path.expanduser(install_root))
            if not install_root or not os.path.isdir(install_root):
                raise ValueError("安装目录不存在")

            parts = [part for part in rel.split("/") if part and part not in {".", ".."}]
            if not parts:
                raise ValueError("可执行文件路径无效")
            exe_path = os.path.realpath(os.path.join(install_root, *parts))
            try:
                common = os.path.commonpath([install_root, exe_path])
            except Exception:
                common = ""
            if common != install_root:
                raise ValueError("可执行文件不在安装目录内")
            base_lower = os.path.basename(exe_path).lower()
            if base_lower in {"uninstall.exe", "unins000.exe", "unins001.exe"} or re.match(r"(?i)^unins\\d{3}\\.exe$", base_lower):
                raise ValueError("不允许选择卸载程序")
            if not os.path.isfile(exe_path):
                raise ValueError("可执行文件不存在")

        steam_result = await self._auto_register_task_to_steam(
            task=task,
            target_dir=install_root,
            exe_path_override=exe_path,
        )

        async with self._lock:
            task = self._find_task(target_task_id)
            if task is None:
                return dict(steam_result)

            task.steam_exe_selected = rel
            task.steam_exe_candidates = []
            task.steam_import_status = "done" if steam_result.get("ok") else "failed"
            parts = [seg.strip() for seg in str(getattr(task, "install_message", "") or "").split("，") if seg and seg.strip()]
            cleaned: List[str] = []
            for seg in parts:
                if seg.startswith("Steam 导入失败"):
                    continue
                if seg.startswith("已加入 Steam"):
                    continue
                if seg.startswith("等待选择启动"):
                    continue
                cleaned.append(seg)

            if steam_result.get("ok"):
                app_id = _safe_int(steam_result.get("appid_unsigned"), 0)
                if app_id > 0:
                    installed_record = self._find_installed_record(
                        game_id=str(task.game_id or "").strip(),
                        install_path=str(getattr(task, "installed_path", "") or "").strip(),
                    )
                    if installed_record is not None and max(0, _safe_int(installed_record.steam_app_id, 0)) != app_id:
                        installed_record.steam_app_id = app_id
                        installed_record.updated_at = _now_wall_ts()
                    cleaned.append(f"已加入 Steam（AppID {app_id}）")
                else:
                    cleaned.append("已加入 Steam")
            else:
                reason = str(steam_result.get("message", "") or "").strip() or "未知错误"
                cleaned.append(f"Steam 导入失败: {reason}")

            task.install_message = "，".join(cleaned) if cleaned else str(getattr(task, "install_message", "") or "").strip()
            task.updated_at = _now_wall_ts()

            await asyncio.to_thread(self.store.save)
            self._invalidate_panel_cache(tasks=True, installed=True)

        return dict(steam_result)

    def _find_latest_task_for_installed_record(
        self,
        record: Optional[TianyiInstalledGame],
        *,
        tasks: Optional[Sequence[TianyiTaskRecord]] = None,
    ) -> Optional[TianyiTaskRecord]:
        """按安装记录查找最近一次关联任务。"""
        current_record = record if isinstance(record, TianyiInstalledGame) else None
        if current_record is None:
            return None

        target_game_id = str(current_record.game_id or "").strip()
        target_install_path = self._normalize_dir_path(str(current_record.install_path or "").strip())
        target_source_path = self._normalize_dir_path(str(current_record.source_path or "").strip())

        candidates = list(tasks if tasks is not None else (self.store.tasks or []))
        candidates.sort(key=lambda item: int(getattr(item, "updated_at", 0) or 0), reverse=True)

        for task in candidates:
            if not isinstance(task, TianyiTaskRecord):
                continue
            task_game_id = str(getattr(task, "game_id", "") or "").strip()
            task_install_path = self._normalize_dir_path(str(getattr(task, "installed_path", "") or "").strip())
            task_source_path = self._normalize_dir_path(str(getattr(task, "local_path", "") or "").strip())

            same_game = bool(target_game_id and task_game_id == target_game_id)
            same_install = bool(target_install_path and task_install_path == target_install_path)
            same_source = bool(target_source_path and task_source_path == target_source_path)
            if same_game or same_install or same_source:
                return task
        return None

    def _build_reimport_task_for_installed_record(
        self,
        record: TianyiInstalledGame,
        matched_task: Optional[TianyiTaskRecord] = None,
    ) -> TianyiTaskRecord:
        """基于已安装记录构造补导入 Steam 所需的伪任务。"""
        install_root = self._normalize_dir_path(str(record.install_path or "").strip())
        openpath = str(getattr(matched_task, "openpath", "") or "").strip() if matched_task is not None else ""
        steam_exe_selected = str(getattr(matched_task, "steam_exe_selected", "") or "").strip() if matched_task is not None else ""

        if not openpath:
            rom_path = self._normalize_dir_path(str(getattr(record, "rom_path", "") or "").strip())
            if rom_path:
                relative_rom = ""
                if install_root:
                    try:
                        if os.path.commonpath([install_root, rom_path]) == install_root:
                            relative_rom = os.path.relpath(rom_path, install_root).replace(os.sep, "/")
                    except Exception:
                        relative_rom = ""
                openpath = relative_rom or os.path.basename(rom_path)

        file_name = (
            os.path.basename(str(getattr(record, "rom_path", "") or "").strip())
            or os.path.basename(str(record.source_path or "").strip())
            or os.path.basename(install_root)
            or str(record.game_title or "").strip()
            or "Freedeck Game"
        )

        return TianyiTaskRecord(
            task_id=f"reimport::{str(record.game_id or '').strip() or uuid.uuid4().hex}",
            gid="",
            game_id=str(record.game_id or "").strip(),
            game_title=str(record.game_title or "").strip() or file_name,
            share_code="",
            share_id="",
            file_id="",
            file_name=file_name,
            file_size=max(0, _safe_int(record.size_bytes, 0)),
            download_dir=str(getattr(self.store.settings, "download_dir", "") or "").strip(),
            local_path=str(record.source_path or "").strip(),
            status="complete",
            progress=100.0,
            speed=0,
            provider="reimport",
            openpath=openpath,
            install_status="installed",
            install_progress=100.0,
            installed_path=install_root,
            steam_exe_selected=steam_exe_selected,
        )

    def _resolve_reimport_exe_override(
        self,
        *,
        record: TianyiInstalledGame,
        matched_task: Optional[TianyiTaskRecord],
        pseudo_task: TianyiTaskRecord,
    ) -> Tuple[str, str, str]:
        """为 PC 已安装记录解析 Steam 重新导入所需 exe。"""
        game_id = str(record.game_id or "").strip()
        platform = str(getattr(record, "platform", "") or "").strip().lower()
        if platform == "switch" or self._is_switch_catalog_game(game_id):
            return "", "", ""

        install_root = self._normalize_dir_path(str(record.install_path or "").strip())
        if not install_root or not os.path.isdir(install_root):
            return "", "", "安装目录不存在"

        selected_rel = ""
        if matched_task is not None:
            selected_rel = str(getattr(matched_task, "steam_exe_selected", "") or "").strip().replace("\\", "/").lstrip("/")
            if selected_rel:
                parts = [part for part in selected_rel.split("/") if part and part not in {".", ".."}]
                candidate = os.path.realpath(os.path.join(install_root, *parts))
                if os.path.isfile(candidate):
                    return candidate, selected_rel, ""

            candidate = self._resolve_installed_executable_path(task=matched_task, target_dir=install_root)
            if candidate and os.path.isfile(candidate):
                rel_path = ""
                try:
                    rel_path = os.path.relpath(candidate, install_root).replace(os.sep, "/")
                except Exception:
                    rel_path = ""
                return candidate, rel_path, ""

        candidate = self._resolve_installed_executable_path(task=pseudo_task, target_dir=install_root)
        if candidate and os.path.isfile(candidate):
            rel_path = ""
            try:
                rel_path = os.path.relpath(candidate, install_root).replace(os.sep, "/")
            except Exception:
                rel_path = ""
            return candidate, rel_path, ""

        exe_candidates = self._list_windows_exe_candidates(install_root, max_depth=8)
        if len(exe_candidates) == 1:
            only_rel = exe_candidates[0]
            candidate = os.path.realpath(os.path.join(install_root, *[part for part in only_rel.split("/") if part]))
            if os.path.isfile(candidate):
                return candidate, only_rel, ""
        if len(exe_candidates) > 1:
            return "", "", "安装目录包含多个可执行文件，无法自动判断启动程序"
        return "", "", "安装目录未找到可执行文件"

    def _inspect_installed_record_steam_import(self, record: TianyiInstalledGame) -> Dict[str, Any]:
        """检查已安装记录当前是否仍正确存在于 Steam 快捷方式中。"""
        game_id = str(record.game_id or "").strip()
        title = str(record.game_title or "").strip() or "未命名游戏"
        install_path = self._normalize_dir_path(str(record.install_path or "").strip())
        platform = str(getattr(record, "platform", "") or "").strip().lower()
        if not platform:
            platform = "switch" if self._is_switch_catalog_game(game_id) else "pc"

        reason = ""
        shortcut: Dict[str, Any] = {}

        if not install_path or not os.path.exists(install_path):
            reason = "安装目录不存在"
        elif not game_id:
            reason = "缺少 game_id，无法识别 Steam 快捷方式"
        else:
            try:
                shortcut = resolve_tianyi_shortcut_sync(game_id=game_id)
            except Exception as exc:
                shortcut = {"ok": False, "message": str(exc)}

            if not bool(shortcut.get("ok")):
                reason = str(shortcut.get("message", "") or "").strip() or "未找到对应 Freedeck 快捷方式"
            else:
                shortcut_exe_path = self._normalize_dir_path(str(shortcut.get("exe_path", "") or "").strip())
                if not shortcut_exe_path or not os.path.isfile(shortcut_exe_path):
                    reason = "Steam 快捷方式目标文件不存在"
                elif platform == "switch":
                    parsed_launch = self._parse_switch_launch_options(str(shortcut.get("launch_options", "") or ""))
                    rom_path = self._normalize_dir_path(str(parsed_launch.get("rom_path", "") or ""))
                    if not rom_path or not os.path.isfile(rom_path):
                        reason = "Steam 启动项未指向有效的 Switch 游戏文件"

        return {
            "game_id": game_id,
            "title": title,
            "install_path": install_path,
            "platform": platform,
            "steam_app_id": max(0, _safe_int(record.steam_app_id, 0)),
            "missing": bool(reason),
            "reason": reason,
            "shortcut_ok": bool(shortcut.get("ok")),
            "shortcut_exe_path": self._normalize_dir_path(str(shortcut.get("exe_path", "") or "").strip()),
        }

    def _build_missing_steam_import_fallback(self, record: TianyiInstalledGame, reason: str) -> Dict[str, Any]:
        """构造缺失 Steam 导入的兜底检查结果，避免单条记录异常中断整个流程。"""
        game_id = str(getattr(record, "game_id", "") or "").strip()
        title = str(getattr(record, "game_title", "") or "").strip() or "未命名游戏"
        install_path = self._normalize_dir_path(str(getattr(record, "install_path", "") or "").strip())
        platform = str(getattr(record, "platform", "") or "").strip().lower()
        if not platform:
            platform = "switch" if self._is_switch_catalog_game(game_id) else "pc"
        return {
            "game_id": game_id,
            "title": title,
            "install_path": install_path,
            "platform": platform,
            "steam_app_id": max(0, _safe_int(getattr(record, "steam_app_id", 0), 0)),
            "missing": True,
            "reason": str(reason or "检测 Steam 快捷方式失败"),
            "shortcut_ok": False,
            "shortcut_exe_path": "",
        }

    async def list_missing_steam_imports(self) -> Dict[str, Any]:
        """列出已安装但缺少/损坏 Steam 快捷方式的游戏。"""
        async with self._lock:
            records = list(self.store.installed_games or [])

        records.sort(key=lambda item: int(getattr(item, "updated_at", 0) or 0), reverse=True)
        items: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        for record in records:
            if not isinstance(record, TianyiInstalledGame):
                continue
            try:
                inspected = self._inspect_installed_record_steam_import(record)
            except Exception as exc:
                config.logger.exception(
                    "Inspect Steam import state failed: game=%s install=%s",
                    str(getattr(record, "game_id", "") or "").strip(),
                    str(getattr(record, "install_path", "") or "").strip(),
                )
                inspected = self._build_missing_steam_import_fallback(record, f"检测 Steam 快捷方式失败：{exc}")
            if not bool(inspected.get("missing")):
                continue
            dedupe_key = f"{str(inspected.get('game_id', '') or '').strip()}::{str(inspected.get('install_path', '') or '').strip()}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            items.append(inspected)

        return {
            "total": len(items),
            "items": items,
        }

    async def reimport_missing_steam_imports(self, *, game_ids: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """将已安装但未正确加入 Steam 的游戏重新导入 Steam。"""
        allowed_game_ids = {str(item or "").strip() for item in list(game_ids or []) if str(item or "").strip()}

        async with self._lock:
            records = list(self.store.installed_games or [])
            task_snapshot = list(self.store.tasks or [])

        records.sort(key=lambda item: int(getattr(item, "updated_at", 0) or 0), reverse=True)

        targets: List[Tuple[TianyiInstalledGame, Dict[str, Any]]] = []
        seen: Set[str] = set()
        for record in records:
            if not isinstance(record, TianyiInstalledGame):
                continue
            try:
                inspected = self._inspect_installed_record_steam_import(record)
            except Exception as exc:
                config.logger.exception(
                    "Inspect Steam import state failed before reimport: game=%s install=%s",
                    str(getattr(record, "game_id", "") or "").strip(),
                    str(getattr(record, "install_path", "") or "").strip(),
                )
                inspected = self._build_missing_steam_import_fallback(record, f"检测 Steam 快捷方式失败：{exc}")
            if not bool(inspected.get("missing")):
                continue
            target_game_id = str(inspected.get("game_id", "") or "").strip()
            if allowed_game_ids and target_game_id not in allowed_game_ids:
                continue
            dedupe_key = f"{target_game_id}::{str(inspected.get('install_path', '') or '').strip()}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            targets.append((record, inspected))

        if not targets:
            return {
                "requested": 0,
                "imported": 0,
                "failed": 0,
                "skipped": 0,
                "needs_restart": False,
                "items": [],
                "message": "未找到需要重新导入到 Steam 的已安装游戏",
            }

        imported = 0
        failed = 0
        skipped = 0
        result_items: List[Dict[str, Any]] = []
        state_changed = False

        for snapshot_record, inspected in targets:
            try:
                matched_task = self._find_latest_task_for_installed_record(snapshot_record, tasks=task_snapshot)
                pseudo_task = self._build_reimport_task_for_installed_record(snapshot_record, matched_task)
                exe_override, selected_rel, preflight_error = self._resolve_reimport_exe_override(
                    record=snapshot_record,
                    matched_task=matched_task,
                    pseudo_task=pseudo_task,
                )

                if preflight_error:
                    skipped += 1
                    result_items.append(
                        {
                            **inspected,
                            "status": "skipped",
                            "message": preflight_error,
                            "appid_unsigned": 0,
                        }
                    )
                    continue

                steam_result = await self._auto_register_task_to_steam(
                    task=pseudo_task,
                    target_dir=str(snapshot_record.install_path or "").strip(),
                    exe_path_override=exe_override,
                )

                ok = bool(steam_result.get("ok"))
                app_id = max(0, _safe_int(steam_result.get("appid_unsigned"), 0))
                status_text = "imported" if ok else "failed"
                message = str(steam_result.get("message", "") or "").strip()
                if ok:
                    imported += 1
                else:
                    failed += 1
                    if not message:
                        message = "Steam 导入失败"

                async with self._lock:
                    live_record = self._find_installed_record(
                        game_id=str(snapshot_record.game_id or "").strip(),
                        install_path=str(snapshot_record.install_path or "").strip(),
                    )
                    if live_record is not None:
                        if ok and app_id > 0 and max(0, _safe_int(live_record.steam_app_id, 0)) != app_id:
                            live_record.steam_app_id = app_id
                            state_changed = True
                        live_platform = str(getattr(live_record, "platform", "") or "").strip().lower()
                        if ok and (live_platform == "switch" or self._is_switch_catalog_game(str(live_record.game_id or "").strip())):
                            switch_metadata = self._build_switch_record_metadata(
                                record=live_record,
                                task=pseudo_task,
                                root_dir=str(live_record.install_path or "").strip(),
                                shortcut_result={
                                    "exe_path": steam_result.get("exe_path", ""),
                                    "launch_options": steam_result.get("launch_options", ""),
                                },
                            )
                            if isinstance(steam_result.get("switch_metadata"), dict):
                                switch_metadata.update(dict(steam_result.get("switch_metadata") or {}))
                            if self._persist_switch_record_metadata(live_record, switch_metadata):
                                state_changed = True
                        if ok:
                            live_record.updated_at = _now_wall_ts()
                            state_changed = True

                    live_task = self._find_latest_task_for_installed_record(live_record or snapshot_record)
                    if live_task is not None:
                        live_task.steam_import_status = "done" if ok else "failed"
                        if ok and selected_rel:
                            live_task.steam_exe_selected = selected_rel
                        if ok and app_id > 0:
                            live_task.install_message = f"已重新加入 Steam（AppID {app_id}）"
                        elif ok:
                            live_task.install_message = "已重新加入 Steam"
                        elif message:
                            live_task.install_message = f"Steam 重新导入失败: {message}"
                        live_task.updated_at = _now_wall_ts()
                        state_changed = True

                result_items.append(
                    {
                        **inspected,
                        "status": status_text,
                        "message": message,
                        "appid_unsigned": app_id,
                    }
                )
            except Exception as exc:
                failed += 1
                config.logger.exception(
                    "Reimport missing Steam import failed: game=%s install=%s",
                    str(getattr(snapshot_record, "game_id", "") or "").strip(),
                    str(getattr(snapshot_record, "install_path", "") or "").strip(),
                )
                result_items.append(
                    {
                        **inspected,
                        "status": "failed",
                        "message": str(exc) or "重新导入异常",
                        "appid_unsigned": 0,
                    }
                )

        if state_changed:
            await asyncio.to_thread(self.store.save)
            self._invalidate_panel_cache(tasks=True, installed=True)

        summary = f"已重新导入 {imported} 款游戏到 Steam"
        if failed > 0 or skipped > 0:
            summary = f"{summary}，失败 {failed}，跳过 {skipped}"
        if imported <= 0 and failed <= 0 and skipped > 0:
            summary = f"共检测到 {len(targets)} 款未入库游戏，但都无法自动重新导入"
        if imported <= 0 and failed > 0 and skipped <= 0:
            summary = f"重新导入失败：共 {failed} 款游戏失败"

        return {
            "requested": len(targets),
            "imported": imported,
            "failed": failed,
            "skipped": skipped,
            "needs_restart": imported > 0,
            "items": result_items,
            "message": summary,
        }

    def _find_task(self, task_id: str) -> Optional[TianyiTaskRecord]:
        """查找任务对象。"""
        target = (task_id or "").strip()
        if not target:
            return None
        for task in self.store.tasks:
            if task.task_id == target:
                return task
        return None

    def _cleanup_tasks(self, tasks: List[TianyiTaskRecord]) -> None:
        """清理过旧终态任务。"""
        now = _now_wall_ts()
        filtered: List[TianyiTaskRecord] = []
        for task in tasks:
            if not _is_terminal(task.status):
                filtered.append(task)
                continue
            if now - int(task.updated_at) <= TASK_RETENTION_SECONDS:
                filtered.append(task)
        tasks[:] = filtered

    def _build_installed_summary(self, limit: int = 8, persist: bool = True) -> Dict[str, Any]:
        """构建已安装游戏预览。"""
        normalized_limit = max(0, int(limit or 0))
        visible_items: List[Dict[str, Any]] = []
        kept_records: List[TianyiInstalledGame] = []

        # 过滤已不存在的安装目录，避免主界面展示脏数据。
        records = sorted(self.store.installed_games, key=lambda item: int(item.updated_at or 0), reverse=True)
        for record in records:
            install_path = str(record.install_path or "").strip()
            if not install_path or not os.path.exists(install_path):
                continue
            kept_records.append(record)
            visible_items.append(self._installed_record_to_view(record))

        if len(kept_records) != len(self.store.installed_games):
            self.store.installed_games = kept_records
            if persist:
                self.store.save()

        return {
            "total": len(visible_items),
            "preview": visible_items[:normalized_limit],
        }

    def _find_installed_record(self, *, game_id: str = "", install_path: str = "") -> Optional[TianyiInstalledGame]:
        """查找已安装游戏记录。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = os.path.realpath(os.path.expanduser(str(install_path or "").strip())) if install_path else ""
        for record in self.store.installed_games:
            same_game = bool(target_game_id and str(record.game_id or "") == target_game_id)
            current_install_path = os.path.realpath(os.path.expanduser(str(record.install_path or "").strip()))
            same_path = bool(target_install_path and current_install_path == target_install_path)
            if same_game or same_path:
                return record
        return None

    def _remove_installed_record_in_memory(
        self,
        *,
        game_id: str = "",
        install_path: str = "",
    ) -> Optional[TianyiInstalledGame]:
        """仅在内存中移除已安装记录，供持久化异常时回退。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = os.path.realpath(os.path.expanduser(str(install_path or "").strip())) if install_path else ""
        for idx, record in enumerate(list(self.store.installed_games)):
            same_game = bool(target_game_id and str(record.game_id or "") == target_game_id)
            current_path = os.path.realpath(os.path.expanduser(str(record.install_path or "").strip()))
            same_path = bool(target_install_path and current_path == target_install_path)
            if same_game or same_path:
                return self.store.installed_games.pop(idx)
        return None

    def _can_remove_install_path(self, path: str) -> Tuple[bool, str]:
        """卸载前校验路径安全性，避免误删根目录。"""
        raw = str(path or "").strip()
        if not raw:
            return False, "安装路径为空"
        target = os.path.realpath(os.path.expanduser(raw))
        if not target:
            return False, "安装路径无效"

        normalized = target.rstrip(os.sep)
        if not normalized:
            return False, "安装路径无效"

        home_dir = os.path.realpath(os.path.expanduser("~")).rstrip(os.sep)
        install_root = os.path.realpath(os.path.expanduser(str(self.store.settings.install_dir or "").strip())).rstrip(os.sep)
        download_root = os.path.realpath(os.path.expanduser(str(self.store.settings.download_dir or "").strip())).rstrip(os.sep)

        blocked = {"/", "/home", "/home/deck", home_dir}
        if install_root:
            blocked.add(install_root)
        if download_root:
            blocked.add(download_root)

        if normalized in blocked:
            return False, "目标路径为系统或根目录级路径，已拒绝删除"

        parts = [part for part in normalized.split(os.sep) if part]
        if len(parts) < 2:
            return False, "目标路径层级过浅，已拒绝删除"

        return True, ""

    def _installed_record_to_view(self, record: TianyiInstalledGame) -> Dict[str, Any]:
        """转换已安装游戏展示结构。"""
        size_bytes = max(0, int(record.size_bytes or 0))
        playtime = self._snapshot_record_playtime(record)
        playtime_seconds = max(0, _safe_int(playtime.get("seconds"), 0))
        return {
            "game_id": record.game_id,
            "title": record.game_title,
            "install_path": record.install_path,
            "source_path": record.source_path,
            "status": record.status,
            "size_bytes": size_bytes,
            "size_text": _format_size_bytes(size_bytes) if size_bytes > 0 else "",
            "steam_app_id": max(0, _safe_int(record.steam_app_id, 0)),
            "playtime_seconds": playtime_seconds,
            "playtime_text": _format_playtime_seconds(playtime_seconds),
            "playtime_sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
            "playtime_last_played_at": max(0, _safe_int(playtime.get("last_played_at"), 0)),
            "playtime_active": bool(playtime.get("active")),
            "updated_at": int(record.updated_at or 0),
        }

    async def _post_process_completed_task(self, task: TianyiTaskRecord) -> None:
        """下载完成后执行安装与清理。"""
        settings = self.store.settings

        task.post_processed = False
        task_id = str(getattr(task, "task_id", "") or "").strip()
        cancel_event = self._install_cancel_events.get(task_id)
        if cancel_event is None:
            cancel_event = threading.Event()
            self._install_cancel_events[task_id] = cancel_event

        def is_cancelled() -> bool:
            try:
                return bool(cancel_event.is_set())
            except Exception:
                return False

        def mark_cancelled(message: str = "已取消安装") -> None:
            task.post_processed = True
            task.install_status = "canceled"
            task.install_progress = float(getattr(task, "install_progress", 0.0) or 0.0)
            task.install_message = str(message or "已取消安装")
            task.updated_at = _now_wall_ts()
            self._invalidate_panel_cache(tasks=True)

        if is_cancelled():
            mark_cancelled()
            return

        local_path = self._resolve_task_local_path(task)
        if not local_path or not os.path.isfile(local_path):
            task.post_processed = True
            task.install_status = "failed"
            task.install_message = "下载文件不存在，无法安装"
            task.updated_at = _now_wall_ts()
            self._invalidate_panel_cache(tasks=True)
            return

        # aria2 在下载收尾阶段可能仍保留 `.aria2` 控制文件；若此时立刻解压，7z 可能报 exit=2（文件仍在写入/不完整）。
        marker_path = f"{local_path}.aria2"
        if os.path.isfile(marker_path):
            started_at = time.monotonic()
            while os.path.isfile(marker_path):
                if is_cancelled():
                    mark_cancelled()
                    return
                task.install_status = "installing"
                task.install_progress = float(getattr(task, "install_progress", 0.0) or 0.0)
                task.install_message = "下载完成收尾中，等待文件写入完成..."
                task.updated_at = _now_wall_ts()
                self._invalidate_panel_cache(tasks=True)

                try:
                    await self.refresh_tasks(sync_aria2=True, persist=False)
                except Exception:
                    pass

                if not os.path.isfile(marker_path):
                    break
                if time.monotonic() - started_at > 90.0:
                    task.post_processed = True
                    task.install_status = "failed"
                    task.install_message = "下载文件仍在写入（aria2 控制文件未释放），请稍后重试"
                    task.updated_at = _now_wall_ts()
                    self._invalidate_panel_cache(tasks=True)
                    return

                await asyncio.sleep(2.0)

        bundle = self._resolve_multipart_archive_bundle(task)
        bundle_members: List[TianyiTaskRecord] = []
        if bundle:
            bundle_members = list(bundle.get("members") or [])
            primary_task = bundle.get("primary_task")
            primary_name = str(bundle.get("primary_name", "") or "").strip()
            try:
                names = [
                    str(getattr(member, "file_name", "") or "").strip()
                    for member in bundle_members[:12]
                    if str(getattr(member, "file_name", "") or "").strip()
                ]
                preview = ", ".join(names[:8]) + ("..." if len(names) > 8 else "")
                config.logger.info(
                    "Multipart bundle detected: game_id=%s task_id=%s kind=%s primary=%s members=%s preview=%s",
                    str(task.game_id or ""),
                    str(task.task_id or ""),
                    str(bundle.get("kind", "") or ""),
                    primary_name,
                    len(bundle_members),
                    preview,
                )
            except Exception:
                pass
            is_primary = primary_task is not None and str(getattr(primary_task, "task_id", "") or "") == str(task.task_id or "")
            if not is_primary:
                primary_status = str(getattr(primary_task, "install_status", "") or "").strip().lower() if primary_task else ""
                if primary_task is None:
                    hint = f"分卷文件（缺少主卷 {primary_name}）" if primary_name else "分卷文件（缺少主卷）"
                elif primary_status == "installed":
                    hint = "分卷文件（已由主卷解压）"
                elif primary_status == "failed":
                    reason = str(getattr(primary_task, "install_message", "") or "").strip()
                    suffix = f"（{reason}）" if reason else ""
                    hint = f"分卷文件（主卷安装失败{suffix}）"
                else:
                    hint = f"分卷文件（由主卷 {primary_name or '主卷'} 统一解压，等待主卷完成解压）"
                task.post_processed = True
                task.install_status = "bundled"
                task.install_progress = 100.0
                task.install_message = hint
                task.updated_at = _now_wall_ts()
                self._invalidate_panel_cache(tasks=True)
                return

            # 主卷任务：等待所有分卷下载完成后再解压，避免只下载一个分卷就开始解压导致失败。
            while True:
                if is_cancelled():
                    mark_cancelled()
                    return
                try:
                    await self.refresh_tasks(sync_aria2=True, persist=False)
                except Exception:
                    # 刷新失败时继续等待，保留 aria2 的真实状态由下一次刷新更新。
                    pass

                # 等待过程中可能会发生“重新下载某个分卷/主卷”的情况（会生成新的 task 记录）。
                # 需要动态重算分卷成员，避免继续盯着旧任务导致永远等待缺失文件。
                try:
                    refreshed_bundle = self._resolve_multipart_archive_bundle(task) or bundle
                except Exception:
                    refreshed_bundle = bundle
                if refreshed_bundle:
                    bundle_members = list(refreshed_bundle.get("members") or bundle_members)
                    refreshed_primary = refreshed_bundle.get("primary_task")
                    refreshed_primary_name = str(refreshed_bundle.get("primary_name", "") or "").strip()
                    still_primary = (
                        refreshed_primary is not None
                        and str(getattr(refreshed_primary, "task_id", "") or "") == str(task.task_id or "")
                    )
                    if not still_primary:
                        primary_status = (
                            str(getattr(refreshed_primary, "install_status", "") or "").strip().lower()
                            if refreshed_primary
                            else ""
                        )
                        if refreshed_primary is None:
                            hint = (
                                f"分卷文件（缺少主卷 {refreshed_primary_name}）"
                                if refreshed_primary_name
                                else "分卷文件（缺少主卷）"
                            )
                        elif primary_status == "installed":
                            hint = "分卷文件（已由主卷解压）"
                        elif primary_status == "failed":
                            reason = str(getattr(refreshed_primary, "install_message", "") or "").strip()
                            suffix = f"（{reason}）" if reason else ""
                            hint = f"分卷文件（主卷安装失败{suffix}）"
                        else:
                            hint = f"分卷文件（由主卷 {refreshed_primary_name or '主卷'} 统一解压，等待主卷完成解压）"
                        task.post_processed = True
                        task.install_status = "bundled"
                        task.install_progress = 100.0
                        task.install_message = hint
                        task.updated_at = _now_wall_ts()
                        self._invalidate_panel_cache(tasks=True)
                        return

                total_parts = max(1, len(bundle_members))
                ready_parts = 0
                failed_reason = ""
                missing_files: List[str] = []

                for member in bundle_members:
                    status = str(getattr(member, "status", "") or "").strip().lower()
                    if status == "complete":
                        member_path = self._resolve_task_local_path(member)
                        if member_path and os.path.isfile(member_path):
                            # aria2 完成后通常会清理 `.aria2` 控制文件；如果仍存在，说明文件可能仍在落盘/未完全收尾。
                            marker = f"{member_path}.aria2"
                            if os.path.isfile(marker):
                                missing_files.append(f"{os.path.basename(member_path)}（仍在写入）")
                                continue

                            # 额外兜底：极少数情况下 aria2 标记 complete 但文件体积仍不足（或被外部打断）。
                            expected_size = max(0, _safe_int(getattr(member, "file_size", 0), 0))
                            if expected_size > 0:
                                try:
                                    actual_size = max(0, int(os.path.getsize(member_path)))
                                except Exception:
                                    actual_size = 0
                                if actual_size > 0 and actual_size < expected_size:
                                    missing_files.append(
                                        f"{os.path.basename(member_path)}（大小不足 {actual_size}/{expected_size}）"
                                    )
                                    continue

                            ready_parts += 1
                        else:
                            missing_files.append(str(getattr(member, "file_name", "") or member_path or "未知分卷"))
                        continue
                    if status in {"error", "removed"}:
                        name = str(getattr(member, "file_name", "") or "分卷").strip() or "分卷"
                        reason = str(getattr(member, "error_reason", "") or "").strip()
                        suffix = f"（{reason}）" if reason else ""
                        failed_reason = f"分卷下载失败: {name}{suffix}"
                        break

                if failed_reason:
                    if bundle_members:
                        for member in bundle_members:
                            if str(getattr(member, "task_id", "") or "") == str(task.task_id or ""):
                                continue
                            member.post_processed = True
                            member.install_status = "bundled"
                            member.install_progress = 100.0
                            member.install_message = f"分卷文件，主卷安装失败（{failed_reason}）"
                            member.updated_at = _now_wall_ts()
                    task.post_processed = True
                    task.install_status = "failed"
                    task.install_message = failed_reason
                    task.updated_at = _now_wall_ts()
                    self._invalidate_panel_cache(tasks=True)
                    return

                ready = bool(ready_parts >= total_parts and not missing_files)
                try:
                    ratio = ready_parts / float(total_parts)
                except Exception:
                    ratio = 0.0
                next_progress = round(max(0.0, min(99.0, ratio * 99.0)), 2)
                current_progress = float(getattr(task, "install_progress", 0.0) or 0.0)
                if next_progress < current_progress:
                    next_progress = current_progress

                task.install_status = "installing"
                task.install_progress = next_progress
                if ready:
                    task.install_message = f"分卷就绪 {ready_parts}/{total_parts}，准备解压..."
                elif missing_files:
                    task.install_message = f"等待分卷 {ready_parts}/{total_parts}，缺少文件: {missing_files[0]}"
                else:
                    task.install_message = f"等待分卷 {ready_parts}/{total_parts}..."
                task.updated_at = _now_wall_ts()
                self._invalidate_panel_cache(tasks=True)
                if ready:
                    break
                await asyncio.sleep(2.0)

        if is_cancelled():
            mark_cancelled()
            return

        install_root = str(settings.install_dir or "").strip() or str(settings.download_dir or "").strip()
        if not install_root:
            install_root = str(getattr(self.plugin, "downloads_dir", config.DOWNLOADS_DIR) or config.DOWNLOADS_DIR)
        install_root = os.path.realpath(os.path.expanduser(install_root))
        os.makedirs(install_root, exist_ok=True)

        target_dir = self._resolve_install_target_dir(task, install_root)
        os.makedirs(target_dir, exist_ok=True)

        task.install_status = "installing"
        task.install_progress = 0.0
        if bundle and len(bundle_members) > 1:
            task.install_message = f"分卷就绪 {len(bundle_members)}/{len(bundle_members)}，开始解压..."
        else:
            task.install_message = "正在安装..."
        task.updated_at = _now_wall_ts()
        self._invalidate_panel_cache(tasks=True)

        is_archive = self._is_archive_file(local_path)
        if is_archive:
            loop = asyncio.get_running_loop()
            last_sent: float = -1.0
            last_sent_at: float = 0.0

            def apply_progress(value: float) -> None:
                if is_cancelled():
                    return
                normalized = max(0.0, min(100.0, float(value or 0.0)))
                current_value = float(getattr(task, "install_progress", 0.0) or 0.0)
                task.install_progress = round(max(current_value, normalized), 2)
                task.updated_at = _now_wall_ts()

            def progress_cb(value: float) -> None:
                nonlocal last_sent, last_sent_at
                try:
                    normalized = max(0.0, min(100.0, float(value)))
                except Exception:
                    normalized = 0.0
                now = time.monotonic()
                if normalized < last_sent and normalized < 99.0:
                    return
                if last_sent >= 0 and abs(normalized - last_sent) < 1.0 and (now - last_sent_at) < 0.8:
                    return
                last_sent = normalized
                last_sent_at = now
                loop.call_soon_threadsafe(apply_progress, normalized)

            try:
                ok, reason = await asyncio.to_thread(
                    self._extract_archive_to_dir,
                    local_path,
                    target_dir,
                    progress_cb,
                    cancel_event,
                )
            except InstallCancelledError as exc:
                mark_cancelled(str(exc) or "已取消安装")
                return
            if not ok:
                if bundle_members:
                    for member in bundle_members:
                        if str(getattr(member, "task_id", "") or "") == str(task.task_id or ""):
                            continue
                        member.post_processed = True
                        member.install_status = "bundled"
                        member.install_progress = 100.0
                        member.install_message = f"分卷文件，主卷解压失败（{reason}）"
                        member.updated_at = _now_wall_ts()
                task.post_processed = True
                task.install_status = "failed"
                task.install_message = reason
                task.updated_at = _now_wall_ts()
                self._invalidate_panel_cache(tasks=True)
                return
        else:
            # 非压缩包按普通文件归档到安装目录。
            if is_cancelled():
                mark_cancelled()
                return
            task.install_progress = 15.0
            file_name = os.path.basename(local_path)
            dest_file = os.path.join(target_dir, file_name)
            try:
                if os.path.realpath(local_path) != os.path.realpath(dest_file):
                    shutil.copy2(local_path, dest_file)
            except Exception as exc:
                task.post_processed = True
                task.install_status = "failed"
                task.install_message = f"复制安装文件失败: {exc}"
                task.updated_at = _now_wall_ts()
                self._invalidate_panel_cache(tasks=True)
                return
            task.install_progress = 100.0

        if is_cancelled():
            mark_cancelled()
            return

        source_size = 0
        try:
            if bundle_members:
                total_size = 0
                for member in bundle_members:
                    member_path = self._resolve_task_local_path(member)
                    if not member_path:
                        continue
                    try:
                        total_size += max(0, int(os.path.getsize(member_path)))
                    except Exception:
                        continue
                source_size = max(0, int(total_size))
            else:
                source_size = max(0, int(os.path.getsize(local_path)))
        except Exception:
            source_size = 0

        is_switch_emulator_package = str(task.game_id or "").strip() == "switch_emulator"
        emulator_setup_result: Dict[str, Any] = {}
        if is_switch_emulator_package:
            task.game_title = SWITCH_EMULATOR_DISPLAY_NAME
            try:
                emulator_setup_result = await asyncio.to_thread(
                    self._bootstrap_eden_emulator_install,
                    target_dir,
                )
            except Exception as exc:
                config.logger.exception("Eden post-install setup failed root=%s err=%s", target_dir, exc)
                emulator_setup_result = {
                    "messages": [f"Eden 初始化失败: {exc}"],
                    "appimage_path": "",
                    "emulator_dir_updated": False,
                }
            if bool(emulator_setup_result.get("emulator_dir_updated")):
                self._invalidate_panel_cache(all_data=True)

        existing_record = self._find_installed_record(
            game_id=str(task.game_id or "").strip(),
            install_path=target_dir,
        )
        existing_playtime_seconds = 0
        existing_playtime_sessions = 0
        existing_playtime_last_played_at = 0
        existing_playtime_active_started_at = 0
        existing_playtime_active_app_id = 0
        existing_steam_app_id = 0
        if existing_record is not None:
            existing_playtime_seconds = max(0, _safe_int(existing_record.playtime_seconds, 0))
            existing_playtime_sessions = max(0, _safe_int(existing_record.playtime_sessions, 0))
            existing_playtime_last_played_at = max(0, _safe_int(existing_record.playtime_last_played_at, 0))
            existing_playtime_active_started_at = max(0, _safe_int(existing_record.playtime_active_started_at, 0))
            existing_playtime_active_app_id = max(0, _safe_int(existing_record.playtime_active_app_id, 0))
            existing_steam_app_id = max(0, _safe_int(existing_record.steam_app_id, 0))

        task.install_status = "installed"
        task.install_progress = 100.0
        task.installed_path = target_dir
        task.updated_at = _now_wall_ts()
        is_switch_entry = self._is_switch_catalog_game(str(task.game_id or "").strip())
        switch_record_metadata: Dict[str, Any] = {}
        if is_switch_entry:
            switch_record_metadata = self._build_switch_record_metadata(
                record=existing_record,
                task=task,
                root_dir=target_dir,
            )

        self.store.upsert_installed_game(
            TianyiInstalledGame(
                game_id=str(task.game_id or ""),
                game_title=str(task.game_title or task.file_name or "未命名游戏"),
                install_path=target_dir,
                source_path=local_path,
                platform=str(switch_record_metadata.get("platform", "") or ""),
                emulator_id=str(switch_record_metadata.get("emulator_id", "") or ""),
                switch_title_id=str(switch_record_metadata.get("switch_title_id", "") or ""),
                rom_path=str(switch_record_metadata.get("rom_path", "") or ""),
                emulator_path=str(switch_record_metadata.get("emulator_path", "") or ""),
                eden_data_root_hint=str(switch_record_metadata.get("eden_data_root_hint", "") or ""),
                status="installed",
                size_bytes=source_size,
                steam_app_id=existing_steam_app_id,
                playtime_seconds=existing_playtime_seconds,
                playtime_sessions=existing_playtime_sessions,
                playtime_last_played_at=existing_playtime_last_played_at,
                playtime_active_started_at=existing_playtime_active_started_at,
                playtime_active_app_id=existing_playtime_active_app_id,
            )
        )
        self._invalidate_panel_cache(tasks=True, installed=True)

        task.steam_import_status = "pending"
        task.steam_exe_candidates = []
        task.steam_exe_selected = ""

        message_parts: List[str] = ["安装完成"]
        steam_result: Dict[str, Any] = {}
        forced_exe_path = ""
        needs_exe_selection = False
        if is_switch_emulator_package:
            appimage_path = os.path.realpath(os.path.expanduser(str(emulator_setup_result.get("appimage_path", "") or "").strip()))
            if appimage_path and os.path.isfile(appimage_path):
                forced_exe_path = appimage_path
                try:
                    rel_path = os.path.relpath(appimage_path, target_dir).replace(os.sep, "/")
                except Exception:
                    rel_path = ""
                if rel_path and not rel_path.startswith("../"):
                    task.steam_exe_selected = rel_path
            for message in list(emulator_setup_result.get("messages") or []):
                text = str(message or "").strip()
                if text:
                    message_parts.append(text)

        if not is_switch_entry and not forced_exe_path and not str(task.openpath or "").strip():
            exe_candidates = self._list_windows_exe_candidates(target_dir, max_depth=8)
            if len(exe_candidates) > 1:
                needs_exe_selection = True
                task.steam_import_status = "needs_exe"
                task.steam_exe_candidates = exe_candidates
                message_parts.append("等待选择启动程序加入 Steam")
            elif len(exe_candidates) == 1:
                task.steam_exe_selected = exe_candidates[0]
                forced_exe_path = os.path.join(target_dir, *[part for part in exe_candidates[0].split("/") if part])

        if not needs_exe_selection:
            steam_result = await self._auto_register_task_to_steam(
                task=task,
                target_dir=target_dir,
                exe_path_override=forced_exe_path,
            )
            installed_record = self._find_installed_record(
                game_id=str(task.game_id or "").strip(),
                install_path=target_dir,
            )
            if installed_record is not None and is_switch_entry:
                merged_switch_metadata = dict(switch_record_metadata)
                if isinstance(steam_result.get("switch_metadata"), dict):
                    merged_switch_metadata.update(dict(steam_result.get("switch_metadata") or {}))
                self._persist_switch_record_metadata(installed_record, merged_switch_metadata)
            if steam_result.get("ok"):
                task.steam_import_status = "done"
                app_id = _safe_int(steam_result.get("appid_unsigned"), 0)
                if app_id > 0:
                    if installed_record is not None and max(0, _safe_int(installed_record.steam_app_id, 0)) != app_id:
                        installed_record.steam_app_id = app_id
                        installed_record.updated_at = _now_wall_ts()
                        await asyncio.to_thread(self.store.save)
                        self._invalidate_panel_cache(installed=True)
                    message_parts.append(f"已加入 Steam（AppID {app_id}）")
                else:
                    message_parts.append("已加入 Steam")
            else:
                task.steam_import_status = "failed"
                reason = str(steam_result.get("message", "") or "").strip() or "未知错误"
                message_parts.append(f"Steam 导入失败: {reason}")

        if bool(settings.auto_delete_package):
            delete_targets = []
            if bundle_members:
                for member in bundle_members:
                    member_path = self._resolve_task_local_path(member)
                    if member_path:
                        delete_targets.append(member_path)
            else:
                delete_targets.append(local_path)

            deleted = 0
            failed: List[str] = []
            for path in sorted(set(delete_targets)):
                try:
                    if path and os.path.isfile(path):
                        os.remove(path)
                        deleted += 1
                except Exception as exc:
                    failed.append(f"{os.path.basename(path)}: {exc}")
            if deleted > 0:
                message_parts.append("已删除下载压缩包" if not bundle_members else f"已删除分卷压缩包（{deleted} 个文件）")
            if failed:
                message_parts.append(f"删除压缩包失败: {failed[0]}")

        if bundle_members:
            # 主卷安装完成后同步更新分卷任务的提示，避免长期显示“等待主卷解压”造成误解。
            for member in bundle_members:
                if str(getattr(member, "task_id", "") or "") == str(task.task_id or ""):
                    continue
                member.post_processed = True
                member.install_status = "bundled"
                member.install_progress = 100.0
                member.install_message = "分卷文件（已由主卷解压）"
                member.updated_at = _now_wall_ts()

        task.install_message = "，".join(message_parts)
        task.post_processed = True

    def _resolve_install_target_dir(self, task: TianyiTaskRecord, install_root: str) -> str:
        """解析任务的目标安装目录。"""
        raw_openpath = str(task.openpath or "").strip().replace("\\", "/")
        if raw_openpath:
            parts = [
                self._sanitize_path_segment(part)
                for part in raw_openpath.split("/")
                if part and part not in {".", ".."}
            ]
            parts = [part for part in parts if part]
            if parts:
                # openpath 仅用于确定游戏根目录，避免把 exe 名当成目录继续拼接。
                root_part = parts[0]
                if len(parts) == 1:
                    stem, ext = os.path.splitext(root_part)
                    if ext:
                        normalized_stem = self._sanitize_path_segment(stem)
                        if normalized_stem:
                            root_part = normalized_stem
                return os.path.join(install_root, root_part)

        title = self._sanitize_path_segment(str(task.game_title or task.file_name or task.game_id or "game"))
        if not title:
            title = "game"
        game_id = self._sanitize_path_segment(str(task.game_id or ""))
        if game_id:
            return os.path.join(install_root, f"{title}_{game_id[:12]}")
        return os.path.join(install_root, title)

    async def _auto_register_task_to_steam(
        self,
        *,
        task: TianyiTaskRecord,
        target_dir: str,
        exe_path_override: str = "",
    ) -> Dict[str, Any]:
        """安装完成后自动写入 Steam 快捷方式、Proton 和封面。"""
        root_dir = os.path.realpath(os.path.expanduser(str(target_dir or "").strip()))
        if not root_dir or not os.path.isdir(root_dir):
            return {"ok": False, "message": "安装目录无效，无法导入 Steam"}

        game_id = str(task.game_id or "").strip()
        is_switch_entry = self._is_switch_catalog_game(game_id)

        exe_path = ""
        switch_game_path = ""
        if is_switch_entry:
            emulator_exe, emulator_diag = self._resolve_switch_emulator_exe_path()
            if not emulator_exe:
                return {
                    "ok": False,
                    "message": "未检测到 Switch 模拟器，请先在设置页下载/配置后再添加到 Steam",
                    "diagnostics": emulator_diag,
                }
            exe_path = emulator_exe

            switch_game_path = self._resolve_switch_game_path_for_task(task=task, root_dir=root_dir)
            if not switch_game_path:
                return {"ok": False, "message": "安装目录未找到 Switch 游戏文件（xci/nsp/nsz/xcz）"}
        else:
            exe_path = os.path.realpath(os.path.expanduser(str(exe_path_override or "").strip())) if exe_path_override else ""
            if exe_path:
                try:
                    common = os.path.commonpath([root_dir, exe_path])
                except Exception:
                    common = ""
                if common != root_dir:
                    return {"ok": False, "message": "可执行文件不在安装目录内"}
                if not os.path.isfile(exe_path):
                    return {"ok": False, "message": "选择的可执行文件不存在"}
            else:
                exe_path = self._resolve_installed_executable_path(task=task, target_dir=target_dir)
                if not exe_path:
                    return {"ok": False, "message": "安装目录未找到可执行文件"}

        launch_token = re.sub(r"[^a-zA-Z0-9._-]+", "_", game_id or task.task_id or "game").strip("_")
        if not launch_token:
            launch_token = "game"
        launch_options = f"freedeck:tianyi:{launch_token}"
        if is_switch_entry:
            escaped = str(switch_game_path or "").replace('"', '\\"')
            launch_options = f'-f -g "{escaped}" {launch_options}'
        if bool(getattr(self.store.settings, "lsfg_enabled", False)):
            lsfg_prefix = "DISABLE_VKBASALT=1 ~/lsfg %command%" if is_switch_entry else "~/lsfg %command%"
            launch_options = f"{lsfg_prefix} {launch_options}"

        display_name = self._derive_display_title_for_steam(str(task.game_title or task.file_name or "Freedeck Game"))

        categories = ""
        try:
            entry = self.catalog.get_by_game_id(game_id)
            if entry is not None:
                categories = str(entry.categories or "")
        except Exception:
            categories = ""
        terms = self._build_catalog_cover_terms(title=str(task.game_title or display_name), categories=categories)

        cover_info: Dict[str, Any] = {}
        try:
            cover_info = await self.resolve_catalog_cover(
                game_id=game_id,
                title=str(task.game_title or display_name),
                categories=categories,
            )
        except Exception as exc:
            config.logger.warning("Resolve catalog cover failed for %s: %s", game_id or display_name, exc)
            cover_info = {}

        cover_landscape = str(cover_info.get("cover_url", "") or "").strip()
        cover_portrait = str(cover_info.get("square_cover_url", "") or "").strip()
        cover_source = str(cover_info.get("source", "") or "").strip()
        steam_app_id = _safe_int(cover_info.get("app_id"), 0)
        cover_hero = ""
        cover_logo = ""
        cover_icon = ""

        if is_switch_entry and steam_app_id <= 0 and cover_source == "steamgriddb":
            cover_landscape = ""
            cover_portrait = ""

        if is_switch_entry and steam_app_id > 0 and terms:
            try:
                app_validation = await self._validate_store_app_id_match(app_id=steam_app_id, terms=terms)
            except Exception as exc:
                config.logger.warning(
                    "Validate switch Steam app id failed game=%s app=%s: %s",
                    game_id or display_name,
                    steam_app_id,
                    exc,
                )
                app_validation = {}

            if bool(app_validation.get("checked")) and not bool(app_validation.get("valid")):
                config.logger.info(
                    "Reject switch artwork app_id mismatch game=%s app=%s source=%s app_name=%s score=%s",
                    game_id or display_name,
                    steam_app_id,
                    cover_source,
                    str(app_validation.get("name", "") or "").strip(),
                    _safe_int(app_validation.get("best_score"), -1),
                )
                steam_app_id = 0
                cover_landscape = ""
                cover_portrait = ""
                cover_source = ""

        steamgriddb_key = ""
        if self._steamgriddb_available():
            steamgriddb_key = resolve_steamgriddb_api_key(getattr(self.store.settings, "steamgriddb_api_key", ""))
        if steamgriddb_key and steam_app_id > 0 and self._steamgriddb_available():
            try:
                sgdb_art = await resolve_steamgriddb_artwork(api_key=steamgriddb_key, steam_app_id=steam_app_id)
                http_status = _safe_int(sgdb_art.get("http_status"), 0)
                if http_status in {429, 500, 502, 503, 504}:
                    self._mark_steamgriddb_unavailable(http_status=http_status)
                if bool(sgdb_art.get("ok")):
                    cover_landscape = str(sgdb_art.get("landscape") or cover_landscape).strip()
                    cover_portrait = str(sgdb_art.get("portrait") or cover_portrait).strip()
                    cover_hero = str(sgdb_art.get("hero") or "").strip()
                    cover_logo = str(sgdb_art.get("logo") or "").strip()
                    cover_icon = str(sgdb_art.get("icon") or "").strip()
            except Exception as exc:
                config.logger.warning("SteamGridDB artwork resolve failed app=%s title=%s: %s", steam_app_id, display_name, exc)

        if steamgriddb_key and is_switch_entry and self._steamgriddb_available() and steam_app_id <= 0:
            try:
                if terms:
                    sgdb_terms = await self._resolve_steamgriddb_artwork_by_terms(api_key=steamgriddb_key, terms=terms)
                    http_status = _safe_int(sgdb_terms.get("http_status"), 0)
                    if http_status in {429, 500, 502, 503, 504}:
                        self._mark_steamgriddb_unavailable(http_status=http_status)
                    if bool(sgdb_terms.get("ok")):
                        sgdb_match = self._evaluate_cover_title_match(
                            terms=terms,
                            name=str(sgdb_terms.get("matched_title", "") or "").strip(),
                        )
                        if bool(sgdb_match.get("confident")):
                            cover_landscape = str(sgdb_terms.get("landscape") or cover_landscape).strip()
                            cover_portrait = str(sgdb_terms.get("portrait") or cover_portrait).strip()
                            cover_hero = str(sgdb_terms.get("hero") or cover_hero).strip()
                            cover_logo = str(sgdb_terms.get("logo") or cover_logo).strip()
                            cover_icon = str(sgdb_terms.get("icon") or cover_icon).strip()
                        else:
                            config.logger.info(
                                "Reject switch SGDB artwork mismatch game=%s matched=%s sgdb_score=%s local_score=%s",
                                game_id or display_name,
                                str(sgdb_terms.get("matched_title", "") or "").strip(),
                                _safe_int(sgdb_terms.get("match_score"), -1),
                                _safe_int(sgdb_match.get("best_score"), -1),
                            )
            except Exception as exc:
                config.logger.warning("SteamGridDB switch artwork resolve failed title=%s: %s", display_name, exc)

        if is_switch_entry and terms and steam_app_id <= 0:
            try:
                store_fallback = await self._resolve_store_cover_by_terms_strict(terms=terms)
                fallback_app_id = _safe_int(store_fallback.get("app_id"), 0) if isinstance(store_fallback, dict) else 0
                if fallback_app_id > 0:
                    steam_app_id = fallback_app_id
                    if not cover_landscape:
                        cover_landscape = str(store_fallback.get("cover_url", "") or "").strip()
                        if not cover_landscape:
                            cover_landscape = self._build_store_cover_url_from_app_id(fallback_app_id)
                    if not cover_portrait:
                        cover_portrait = self._build_store_square_cover_url(fallback_app_id)
            except Exception as exc:
                config.logger.warning("Switch Steam store artwork fallback failed title=%s: %s", display_name, exc)

        try:
            exe_lower = os.path.basename(str(exe_path or "")).lower()
            proton_tool = ""
            if exe_lower.endswith(WINDOWS_LAUNCH_SUFFIXES):
                proton_tool = "proton_experimental"
                config.logger.info(
                    "Steam shortcut compat selection: game=%s app=%s forced_tool=%s",
                    game_id or display_name,
                    steam_app_id,
                    proton_tool,
                )
            result = await add_or_update_tianyi_shortcut(
                game_id=game_id or task.task_id,
                display_name=display_name,
                exe_path=exe_path,
                launch_options=launch_options,
                proton_tool=proton_tool,
                cover_landscape_url=cover_landscape,
                cover_portrait_url=cover_portrait,
                cover_hero_url=cover_hero,
                cover_logo_url=cover_logo,
                cover_icon_url=cover_icon,
                steam_app_id=steam_app_id,
            )
            if is_switch_entry:
                switch_metadata = self._build_switch_record_metadata(
                    task=task,
                    root_dir=root_dir,
                    shortcut_result={
                        "launch_options": launch_options,
                        "exe_path": exe_path,
                    },
                )
                result["switch_metadata"] = {
                    "platform": str(switch_metadata.get("platform", "") or ""),
                    "emulator_id": str(switch_metadata.get("emulator_id", "") or ""),
                    "switch_title_id": str(switch_metadata.get("switch_title_id", "") or ""),
                    "rom_path": str(switch_metadata.get("rom_path", "") or ""),
                    "emulator_path": str(switch_metadata.get("emulator_path", "") or ""),
                    "eden_data_root_hint": str(switch_metadata.get("eden_data_root_hint", "") or ""),
                }
            if not result.get("ok"):
                config.logger.warning(
                    "Auto add to Steam failed game=%s exe=%s reason=%s",
                    game_id or display_name,
                    exe_path,
                    result.get("message", ""),
                )
            return result
        except Exception as exc:
            config.logger.exception("Auto add to Steam exception game=%s exe=%s", game_id or display_name, exe_path)
            return {"ok": False, "message": str(exc)}

    def _derive_display_title_for_steam(self, title: str) -> str:
        """优先取中文标题片段。"""
        raw = " ".join(str(title or "").replace("\u3000", " ").split())
        if not raw:
            return "Freedeck Game"

        parts = [part.strip() for part in re.split(r"[\\/|｜丨]+", raw) if part and part.strip()]
        if not parts:
            return raw

        for part in parts:
            if re.search(r"[\u4e00-\u9fff]", part):
                return part
        return parts[0]

    def _resolve_installed_executable_path(self, *, task: TianyiTaskRecord, target_dir: str) -> str:
        """基于 openpath 优先定位安装后的可执行文件。"""
        root = os.path.realpath(os.path.expanduser(str(target_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        raw_openpath = str(task.openpath or "").strip().replace("\\", "/")
        if raw_openpath:
            parts = [
                self._sanitize_path_segment(part)
                for part in raw_openpath.split("/")
                if part and part not in {".", ".."}
            ]
            parts = [part for part in parts if part]

            if parts:
                rel_parts = parts[1:] if len(parts) > 1 else parts
                if rel_parts:
                    candidate = os.path.realpath(os.path.join(root, *rel_parts))
                    if os.path.isfile(candidate):
                        return candidate
                    leaf = rel_parts[-1]
                    matched_leaf = self._find_path_by_leaf_name(root, leaf, max_depth=8)
                    if matched_leaf:
                        return matched_leaf

        fallback = self._find_first_executable_candidate(root, max_depth=8)
        if fallback:
            return fallback
        return ""

    def _list_windows_exe_candidates(self, root_dir: str, max_depth: int = 8) -> List[str]:
        """列出安装目录内可选的 Windows 可执行文件（.exe），用于用户交互选择。

        返回相对路径（使用 / 分隔），按层级与路径长度排序，且默认排除卸载程序（uninstall.exe 等）。
        """
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return []

        base_depth = root.count(os.sep)
        out: List[Tuple[Tuple[int, int], str]] = []
        seen: Set[str] = set()
        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []

            for name in filenames:
                lower = str(name).lower()
                if not lower.endswith(".exe"):
                    continue
                if lower in {"uninstall.exe", "unins000.exe", "unins001.exe"}:
                    continue
                if re.match(r"(?i)^unins\\d{3}\\.exe$", name or ""):
                    continue
                abs_path = os.path.realpath(os.path.join(dirpath, name))
                try:
                    rel_path = os.path.relpath(abs_path, root)
                except Exception:
                    continue
                rel_path = rel_path.replace(os.sep, "/")
                if not rel_path or rel_path.startswith("../"):
                    continue
                key = rel_path.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(((max(0, depth), len(rel_path)), rel_path))

        out.sort(key=lambda pair: pair[0])
        return [rel for _, rel in out]

    def _find_path_by_leaf_name(self, root_dir: str, leaf_name: str, max_depth: int = 6) -> str:
        """在目录树中按文件名查找（大小写不敏感）。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        leaf = str(leaf_name or "").strip().lower()
        if not root or not leaf or not os.path.isdir(root):
            return ""

        base_depth = root.count(os.sep)
        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []
            for name in filenames:
                if str(name).lower() != leaf:
                    continue
                return os.path.realpath(os.path.join(dirpath, name))
        return ""

    def _find_first_executable_candidate(self, root_dir: str, max_depth: int = 6) -> str:
        """回退查找首个可执行文件候选（优先 .exe）。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        best_path = ""
        best_rank: Tuple[int, int, int] = (9, 999, 99999)
        base_depth = root.count(os.sep)

        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []

            for name in filenames:
                lower = str(name).lower()
                ext_rank = 9
                if lower.endswith(".exe"):
                    if lower in {"uninstall.exe", "unins000.exe", "unins001.exe"}:
                        continue
                    if re.match(r"(?i)^unins\\d{3}\\.exe$", name or ""):
                        continue
                    ext_rank = 0
                elif lower.endswith(".bat") or lower.endswith(".cmd"):
                    ext_rank = 1
                elif lower.endswith(".sh") or lower.endswith(".x86_64"):
                    ext_rank = 2
                elif lower.endswith(".appimage"):
                    ext_rank = 3
                else:
                    continue

                candidate = os.path.realpath(os.path.join(dirpath, name))
                rank = (ext_rank, max(0, depth), len(candidate))
                if rank < best_rank:
                    best_rank = rank
                    best_path = candidate

        return best_path

    def _find_first_switch_rom_candidate(self, root_dir: str, max_depth: int = 6) -> str:
        """查找 Switch 游戏文件候选（xci/nsp/nsz/xcz）。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        ext_priority = {".xci": 0, ".nsp": 0, ".nsz": 1, ".xcz": 1}
        best_path = ""
        best_rank: Tuple[int, int, int, int] = (9, 999, 0, 99999)
        base_depth = root.count(os.sep)

        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []

            for name in filenames:
                ext = os.path.splitext(str(name))[1].lower()
                if ext not in ext_priority:
                    continue

                abs_path = os.path.realpath(os.path.join(dirpath, name))
                size = 0
                try:
                    size = max(0, int(os.path.getsize(abs_path)))
                except Exception:
                    size = 0

                rank = (int(ext_priority.get(ext, 9)), max(0, int(depth)), -size, len(abs_path))
                if rank < best_rank:
                    best_rank = rank
                    best_path = abs_path

        return best_path

    def _is_switch_catalog_game(self, game_id: str) -> bool:
        """判断 catalog 条目是否为 Switch 游戏。"""
        normalized_game_id = str(game_id or "").strip()
        if not normalized_game_id:
            return False
        try:
            entry = self.catalog.get_by_game_id(normalized_game_id)
        except Exception:
            entry = None
        return bool(entry is not None and str(getattr(entry, "category_parent", "") or "").strip() == "527")

    def _normalize_switch_title_id(self, value: Any) -> str:
        """规范化 Switch title id。"""
        text = str(value or "").strip()
        if not text:
            return ""
        matched = SWITCH_TITLE_ID_RE.search(text)
        if not matched:
            return ""
        return str(matched.group(1) or "").upper()

    def _extract_switch_title_id(self, *values: Any) -> str:
        """从多个文本源中提取 Switch title id。"""
        for value in values:
            title_id = self._normalize_switch_title_id(value)
            if title_id:
                return title_id
        return ""

    def _looks_like_switch_rom_path(self, path: str) -> bool:
        """判断路径是否像 Switch ROM。"""
        text = str(path or "").strip()
        if not text:
            return False
        ext = os.path.splitext(text)[1].lower()
        return ext in SWITCH_ROM_EXTENSIONS

    def _resolve_switch_game_path_for_task(self, *, task: TianyiTaskRecord, root_dir: str) -> str:
        """基于 openpath 或目录扫描定位 Switch ROM。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        raw_openpath = str(task.openpath or "").strip().replace("\\", "/")
        if raw_openpath:
            parts = [
                self._sanitize_path_segment(part)
                for part in raw_openpath.split("/")
                if part and part not in {".", ".."}
            ]
            parts = [part for part in parts if part]
            if parts:
                rel_parts = parts[1:] if len(parts) > 1 else parts
                if rel_parts:
                    candidate = os.path.realpath(os.path.join(root, *rel_parts))
                    if os.path.isfile(candidate) and self._looks_like_switch_rom_path(candidate):
                        return candidate
                    leaf = rel_parts[-1]
                    matched_leaf = self._find_path_by_leaf_name(root, leaf, max_depth=10)
                    if matched_leaf and self._looks_like_switch_rom_path(matched_leaf):
                        return matched_leaf

        return self._find_first_switch_rom_candidate(root, max_depth=10)

    def _parse_switch_launch_options(self, launch_options: str) -> Dict[str, Any]:
        """从 Steam 启动项中解析 Switch/Eden 参数。"""
        text = str(launch_options or "").strip()
        result: Dict[str, Any] = {
            "rom_path": "",
            "has_fullscreen_flag": False,
            "tokens": [],
        }
        if not text:
            return result

        try:
            tokens = list(shlex.split(text))
        except Exception:
            tokens = text.split()

        result["tokens"] = list(tokens)
        for index, token in enumerate(tokens):
            current = str(token or "").strip()
            if current == "-f":
                result["has_fullscreen_flag"] = True
                continue
            if current == "-g" and index + 1 < len(tokens):
                candidate = os.path.realpath(os.path.expanduser(str(tokens[index + 1] or "").strip()))
                if candidate:
                    result["rom_path"] = candidate
                break
            if current.startswith("-g="):
                candidate = os.path.realpath(os.path.expanduser(current.split("=", 1)[1].strip()))
                if candidate:
                    result["rom_path"] = candidate
                break
        return result

    def _build_eden_data_root_candidates(
        self,
        *,
        emulator_path: str = "",
        data_root_hint: str = "",
    ) -> List[Dict[str, Any]]:
        """构建 Eden 数据目录候选。"""
        candidates: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        def push(path: str, *, source: str, priority: int) -> None:
            normalized = self._normalize_dir_path(path)
            if not normalized or normalized in seen or not os.path.isdir(normalized):
                return
            seen.add(normalized)
            save_base = self._normalize_existing_dir(os.path.join(normalized, EDEN_SAVE_RELATIVE_PATH))
            candidates.append(
                {
                    "root": normalized,
                    "save_base": save_base,
                    "source": source,
                    "priority": priority,
                }
            )

        hinted = self._normalize_dir_path(str(data_root_hint or "").strip())
        if hinted:
            push(hinted, source="hint", priority=400)

        push(EDEN_DATA_DIR, source="default_data_dir", priority=300)

        emulator_file = self._normalize_dir_path(str(emulator_path or "").strip())
        emulator_root = ""
        if emulator_file and os.path.isfile(emulator_file):
            emulator_root = os.path.dirname(emulator_file)
        elif emulator_file and os.path.isdir(emulator_file):
            emulator_root = emulator_file
        if emulator_root:
            push(os.path.join(emulator_root, EDEN_USER_DIRNAME), source="adjacent_user", priority=200)
            push(emulator_root, source="emulator_root", priority=100)

        candidates.sort(
            key=lambda item: (
                -max(0, _safe_int(item.get("priority"), 0)),
                str(item.get("root", "") or ""),
            )
        )
        return candidates

    def _extract_eden_save_path_metadata(self, save_path: str, save_base: str = "") -> Dict[str, Any]:
        """解析 Eden 存档目录中的 profile/user/title 信息。"""
        normalized_path = self._normalize_dir_path(save_path)
        normalized_base = self._normalize_dir_path(save_base)
        relative = ""
        if normalized_path and normalized_base:
            try:
                relative = os.path.relpath(normalized_path, normalized_base).replace("\\", "/")
            except Exception:
                relative = ""
        relative = str(relative or "").strip("./")
        parts = [part for part in relative.split("/") if part] if relative else []
        profile_id = parts[0] if len(parts) >= 1 else ""
        user_id = parts[1] if len(parts) >= 2 else ""
        title_id = self._normalize_switch_title_id(parts[2]) if len(parts) >= 3 else ""
        mtime = 0
        try:
            mtime = max(0, int(os.path.getmtime(normalized_path))) if normalized_path and os.path.exists(normalized_path) else 0
        except Exception:
            mtime = 0
        return {
            "path": normalized_path,
            "save_base": normalized_base,
            "relative_path": relative,
            "profile_id": profile_id,
            "user_id": user_id,
            "title_id": title_id,
            "mtime": mtime,
        }

    def _find_eden_title_save_dirs(self, *, save_base: str, title_id: str) -> List[Dict[str, Any]]:
        """在 Eden save 根目录下查找指定 title id 的存档目录。"""
        normalized_base = self._normalize_existing_dir(save_base)
        normalized_title_id = self._normalize_switch_title_id(title_id)
        if not normalized_base or not normalized_title_id:
            return []

        matches: List[Dict[str, Any]] = []
        try:
            for profile_name in os.listdir(normalized_base):
                if not profile_name or profile_name == "cache":
                    continue
                profile_dir = os.path.join(normalized_base, profile_name)
                if not os.path.isdir(profile_dir):
                    continue
                for user_name in os.listdir(profile_dir):
                    user_dir = os.path.join(profile_dir, user_name)
                    if not os.path.isdir(user_dir):
                        continue
                    for child_name in os.listdir(user_dir):
                        child_dir = os.path.join(user_dir, child_name)
                        if not os.path.isdir(child_dir):
                            continue
                        if self._normalize_switch_title_id(child_name) != normalized_title_id:
                            continue
                        matches.append(
                            self._extract_eden_save_path_metadata(child_dir, normalized_base)
                        )
        except Exception:
            return []

        matches.sort(
            key=lambda item: (
                -max(0, _safe_int(item.get("mtime"), 0)),
                str(item.get("relative_path", "") or ""),
            )
        )
        return matches

    def _build_switch_record_metadata(
        self,
        *,
        record: Optional[TianyiInstalledGame] = None,
        task: Optional[TianyiTaskRecord] = None,
        root_dir: str = "",
        shortcut_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """构建 Switch/Eden 元数据。"""
        shortcut = dict(shortcut_result or {})
        current_record = record if isinstance(record, TianyiInstalledGame) else None
        current_task = task if isinstance(task, TianyiTaskRecord) else None

        install_root = self._normalize_dir_path(
            root_dir
            or (str(getattr(current_record, "install_path", "") or "").strip() if current_record else "")
            or (str(getattr(current_task, "installed_path", "") or "").strip() if current_task else "")
        )

        launch_parsed = self._parse_switch_launch_options(str(shortcut.get("launch_options", "") or ""))
        shortcut_rom_path = self._normalize_dir_path(str(launch_parsed.get("rom_path", "") or ""))

        rom_path = ""
        if current_record is not None:
            rom_path = self._normalize_dir_path(str(getattr(current_record, "rom_path", "") or "").strip())
        if not rom_path and shortcut_rom_path and self._looks_like_switch_rom_path(shortcut_rom_path):
            rom_path = shortcut_rom_path
        if not rom_path and current_task is not None and install_root:
            candidate = self._resolve_switch_game_path_for_task(task=current_task, root_dir=install_root)
            if candidate:
                rom_path = candidate

        emulator_path = ""
        if current_record is not None:
            emulator_path = self._normalize_dir_path(str(getattr(current_record, "emulator_path", "") or "").strip())
        if not emulator_path:
            emulator_path = self._normalize_dir_path(str(shortcut.get("exe_path", "") or "").strip())
        if not emulator_path and current_task is not None:
            resolved_emulator_path, _ = self._resolve_switch_emulator_exe_path()
            emulator_path = self._normalize_dir_path(resolved_emulator_path)

        data_root_hint = ""
        if current_record is not None:
            data_root_hint = self._normalize_dir_path(str(getattr(current_record, "eden_data_root_hint", "") or "").strip())

        title_id = self._extract_switch_title_id(
            str(getattr(current_record, "switch_title_id", "") or "").strip() if current_record else "",
            str(getattr(current_task, "file_name", "") or "").strip() if current_task else "",
            str(getattr(current_task, "openpath", "") or "").strip() if current_task else "",
            rom_path,
            os.path.basename(rom_path) if rom_path else "",
            str(getattr(current_record, "source_path", "") or "").strip() if current_record else "",
            str(getattr(current_record, "game_title", "") or "").strip() if current_record else "",
            str(getattr(current_task, "game_title", "") or "").strip() if current_task else "",
        )

        root_candidates = self._build_eden_data_root_candidates(
            emulator_path=emulator_path,
            data_root_hint=data_root_hint,
        )
        if not data_root_hint:
            for candidate in root_candidates:
                normalized_root = self._normalize_dir_path(str(candidate.get("root", "") or ""))
                if normalized_root:
                    data_root_hint = normalized_root
                    break

        return {
            "platform": "switch",
            "emulator_id": "eden",
            "switch_title_id": title_id,
            "rom_path": rom_path if self._looks_like_switch_rom_path(rom_path) else "",
            "emulator_path": emulator_path,
            "eden_data_root_hint": data_root_hint,
            "root_candidates": root_candidates,
            "launch": launch_parsed,
        }

    def _persist_switch_record_metadata(self, record: TianyiInstalledGame, metadata: Dict[str, Any]) -> bool:
        """将 Switch/Eden 元数据回填到安装记录。"""
        if not isinstance(record, TianyiInstalledGame):
            return False

        changed = False

        def apply(field: str) -> None:
            nonlocal changed
            next_value = str(metadata.get(field, "") or "").strip()
            if not next_value:
                return
            current_value = str(getattr(record, field, "") or "").strip()
            if current_value == next_value:
                return
            setattr(record, field, next_value)
            changed = True

        apply("platform")
        apply("emulator_id")
        apply("switch_title_id")
        apply("rom_path")
        apply("emulator_path")
        apply("eden_data_root_hint")

        if changed:
            record.updated_at = _now_wall_ts()
            self.store.upsert_installed_game(record)
        return changed

    def _resolve_switch_cloud_save_context(
        self,
        *,
        record: TianyiInstalledGame,
        shortcut_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """解析 Switch/Eden 云存档上下文。"""
        metadata = self._build_switch_record_metadata(record=record, shortcut_result=shortcut_result)
        self._persist_switch_record_metadata(record, metadata)

        title_id = self._normalize_switch_title_id(metadata.get("switch_title_id"))
        root_candidates = [dict(item) for item in list(metadata.get("root_candidates") or []) if isinstance(item, dict)]
        diagnostics: Dict[str, Any] = {
            "platform": "switch",
            "emulator_id": "eden",
            "title_id": title_id,
            "rom_path": str(metadata.get("rom_path", "") or ""),
            "emulator_path": str(metadata.get("emulator_path", "") or ""),
            "root_candidates": [],
        }

        ranked_candidates: List[Dict[str, Any]] = []
        for item in root_candidates:
            root = self._normalize_dir_path(str(item.get("root", "") or ""))
            save_base = self._normalize_existing_dir(str(item.get("save_base", "") or ""))
            source = str(item.get("source", "") or "").strip()
            priority = max(0, _safe_int(item.get("priority"), 0))
            matched_dirs = self._find_eden_title_save_dirs(save_base=save_base, title_id=title_id) if title_id and save_base else []
            latest_mtime = 0
            if matched_dirs:
                latest_mtime = max(max(0, _safe_int(row.get("mtime"), 0)) for row in matched_dirs)
            ranked_candidates.append(
                {
                    "root": root,
                    "save_base": save_base,
                    "source": source,
                    "priority": priority,
                    "matched_dirs": matched_dirs,
                    "matched_count": len(matched_dirs),
                    "latest_mtime": latest_mtime,
                }
            )

        ranked_candidates.sort(
            key=lambda item: (
                -1 if item.get("matched_count", 0) else 0,
                -max(0, _safe_int(item.get("priority"), 0)),
                -max(0, _safe_int(item.get("matched_count"), 0)),
                -max(0, _safe_int(item.get("latest_mtime"), 0)),
                str(item.get("root", "") or ""),
            )
        )

        diagnostics["root_candidates"] = [
            {
                "root": str(item.get("root", "") or ""),
                "save_base": str(item.get("save_base", "") or ""),
                "source": str(item.get("source", "") or ""),
                "matched_count": max(0, _safe_int(item.get("matched_count"), 0)),
                "matched_dirs": [str(row.get("path", "") or "") for row in list(item.get("matched_dirs") or []) if isinstance(row, dict)],
            }
            for item in ranked_candidates
        ]

        if not title_id:
            diagnostics["reason"] = "title_id_missing"
            return {
                "ok": False,
                "reason": "title_id_missing",
                "diagnostics": diagnostics,
                "metadata": metadata,
                "source_paths": [],
                "source_strategy": "eden_nand",
            }

        selected = ranked_candidates[0] if ranked_candidates else {}
        selected_root = self._normalize_dir_path(str(selected.get("root", "") or ""))
        selected_save_base = self._normalize_existing_dir(str(selected.get("save_base", "") or ""))
        matched_dirs = [dict(row) for row in list(selected.get("matched_dirs") or []) if isinstance(row, dict)]
        source_paths = [
            str(row.get("path", "") or "")
            for row in matched_dirs[:CLOUD_SAVE_MAX_SOURCE_PATHS]
            if str(row.get("path", "") or "").strip()
        ]
        source_paths = self._dedupe_existing_paths(source_paths)

        if selected_root and selected_root != str(getattr(record, "eden_data_root_hint", "") or "").strip():
            self._persist_switch_record_metadata(record, {"eden_data_root_hint": selected_root})

        diagnostics["selected_root"] = selected_root
        diagnostics["selected_save_base"] = selected_save_base
        diagnostics["selected_source"] = str(selected.get("source", "") or "")
        diagnostics["source_paths"] = list(source_paths)

        if not selected_save_base:
            diagnostics["reason"] = "eden_data_root_unresolved"
            return {
                "ok": False,
                "reason": "eden_data_root_unresolved",
                "diagnostics": diagnostics,
                "metadata": metadata,
                "source_paths": [],
                "source_strategy": "eden_nand",
            }

        if not source_paths:
            diagnostics["reason"] = "save_path_not_found"
            return {
                "ok": False,
                "reason": "save_path_not_found",
                "diagnostics": diagnostics,
                "metadata": metadata,
                "source_paths": [],
                "source_strategy": "eden_nand",
            }

        return {
            "ok": True,
            "reason": "",
            "diagnostics": diagnostics,
            "metadata": metadata,
            "source_paths": source_paths,
            "source_strategy": "eden_nand",
            "selected_root": selected_root,
            "selected_save_base": selected_save_base,
        }

    def _find_first_appimage_candidate(self, root_dir: str, max_depth: int = 6) -> str:
        """优先查找 AppImage，可用于原生模拟器导入 Steam。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        best_path = ""
        best_rank: Tuple[int, int] = (999, 99999)
        base_depth = root.count(os.sep)

        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []

            for name in filenames:
                if not str(name).lower().endswith(".appimage"):
                    continue
                candidate = os.path.realpath(os.path.join(dirpath, name))
                rank = (max(0, depth), len(candidate))
                if rank < best_rank:
                    best_rank = rank
                    best_path = candidate

        return best_path

    def _list_eden_key_candidates(self, root_dir: str, max_depth: int = 4) -> List[str]:
        """列出安装目录中的 Eden key 文件候选。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return []

        base_depth = root.count(os.sep)
        ranked: List[Tuple[Tuple[int, int, int], str]] = []
        seen: Set[str] = set()
        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []

            for name in filenames:
                lower = str(name).lower().strip()
                if not lower:
                    continue

                priority = -1
                if lower == "prod.keys":
                    priority = 0
                elif lower == "title.keys":
                    priority = 1
                elif lower.endswith(".keys"):
                    priority = 2
                elif lower.endswith(".key"):
                    priority = 3
                elif lower in {"key", "keys"}:
                    priority = 4
                else:
                    continue

                candidate = os.path.realpath(os.path.join(dirpath, name))
                if not os.path.isfile(candidate):
                    continue
                key = candidate.lower()
                if key in seen:
                    continue
                seen.add(key)
                ranked.append(((priority, max(0, depth), len(candidate)), candidate))

        ranked.sort(key=lambda item: item[0])
        return [path for _, path in ranked]

    def _resolve_eden_key_target_name(self, source_path: str) -> str:
        """根据源文件名推导 Eden keys 目标文件名。"""
        lower = os.path.basename(str(source_path or "")).lower().strip()
        if "title" in lower:
            return "title.keys"
        return "prod.keys"

    def _write_eden_qt_config_chinese(self) -> Dict[str, Any]:
        """写入 Eden 的中文配置。"""
        os.makedirs(EDEN_CONFIG_DIR, mode=0o755, exist_ok=True)

        parser = configparser.RawConfigParser(interpolation=None)
        parser.optionxform = str

        existed = os.path.isfile(EDEN_QT_CONFIG_PATH)
        if existed:
            parser.read(EDEN_QT_CONFIG_PATH, encoding="utf-8")

        if not parser.has_section("System"):
            parser.add_section("System")
        if not parser.has_section("UI"):
            parser.add_section("UI")

        parser.set("System", r"language_index\default", "false")
        parser.set("System", "language_index", EDEN_SYSTEM_LANGUAGE_INDEX)
        parser.set("UI", r"Paths\language\default", "false")
        parser.set("UI", r"Paths\language", EDEN_UI_LANGUAGE)

        backup_path = ""
        if existed:
            backup_path = f"{EDEN_QT_CONFIG_PATH}.bak.{time.strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(EDEN_QT_CONFIG_PATH, backup_path)

        fd, tmp_path = tempfile.mkstemp(prefix="eden_qt_", suffix=".ini", dir=EDEN_CONFIG_DIR)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                parser.write(handle, space_around_delimiters=False)
            os.replace(tmp_path, EDEN_QT_CONFIG_PATH)
        except Exception:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            raise

        return {
            "config_path": EDEN_QT_CONFIG_PATH,
            "backup_path": backup_path,
        }

    def _bootstrap_eden_emulator_install(self, root_dir: str) -> Dict[str, Any]:
        """安装 Eden 后自动完成 AppImage、keys 与中文配置初始化。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        result: Dict[str, Any] = {
            "appimage_path": "",
            "key_targets": [],
            "config_path": EDEN_QT_CONFIG_PATH,
            "config_backup_path": "",
            "messages": [],
            "emulator_dir_updated": False,
        }
        if not root or not os.path.isdir(root):
            result["messages"].append("Eden 初始化失败: 安装目录无效")
            return result

        appimage_path = self._find_first_appimage_candidate(root, max_depth=6)
        if appimage_path and os.path.isfile(appimage_path):
            result["appimage_path"] = appimage_path
            try:
                current_mode = os.stat(appimage_path).st_mode
                os.chmod(appimage_path, current_mode | 0o111)
            except Exception as exc:
                result["messages"].append(f"已找到 Eden AppImage，但设置执行权限失败: {exc}")
            try:
                self.store.set_settings(emulator_dir=appimage_path)
                result["emulator_dir_updated"] = True
            except Exception as exc:
                result["messages"].append(f"Eden 模拟器路径保存失败: {exc}")
        else:
            result["messages"].append("未找到 Eden AppImage，已跳过模拟器路径更新")

        try:
            os.makedirs(EDEN_KEYS_DIR, mode=0o755, exist_ok=True)
            copied_targets: List[str] = []
            seen_targets: Set[str] = set()
            for source_path in self._list_eden_key_candidates(root, max_depth=4):
                target_name = self._resolve_eden_key_target_name(source_path)
                if target_name in seen_targets:
                    continue
                target_path = os.path.join(EDEN_KEYS_DIR, target_name)
                shutil.copy2(source_path, target_path)
                try:
                    os.chmod(target_path, 0o600)
                except Exception:
                    pass
                copied_targets.append(target_path)
                seen_targets.add(target_name)
            result["key_targets"] = copied_targets
            if copied_targets:
                result["messages"].append("Eden 密钥已导入")
            else:
                result["messages"].append("未找到可导入的 Eden key 文件")
        except Exception as exc:
            result["messages"].append(f"Eden 密钥导入失败: {exc}")

        try:
            config_result = self._write_eden_qt_config_chinese()
            result["config_path"] = str(config_result.get("config_path", EDEN_QT_CONFIG_PATH) or EDEN_QT_CONFIG_PATH)
            result["config_backup_path"] = str(config_result.get("backup_path", "") or "")
            result["messages"].append("Eden 已配置为中文")
        except Exception as exc:
            result["messages"].append(f"Eden 中文配置失败: {exc}")

        return result

    def _resolve_switch_emulator_exe_path(self) -> Tuple[str, Dict[str, Any]]:
        """解析 Switch 模拟器可执行文件路径（用于 Steam 启动项生成）。"""
        diagnostics: Dict[str, Any] = {"checked": [], "source": "", "root_dir": "", "exe_path": ""}

        def try_dir(root_dir: str, *, source: str) -> str:
            root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
            if not root:
                return ""
            diagnostics["checked"].append({"source": source, "root_dir": root})
            if os.path.isfile(root):
                diagnostics["source"] = source
                diagnostics["root_dir"] = os.path.dirname(root)
                diagnostics["exe_path"] = root
                return root
            if not os.path.isdir(root):
                return ""
            exe = self._find_first_executable_candidate(root, max_depth=8)
            if exe and os.path.isfile(exe):
                diagnostics["source"] = source
                diagnostics["root_dir"] = root
                diagnostics["exe_path"] = exe
                return exe
            return ""

        settings_dir = str(getattr(self.store.settings, "emulator_dir", "") or "").strip()
        exe = try_dir(settings_dir, source="settings.emulator_dir") if settings_dir else ""
        if exe:
            return exe, diagnostics

        installed_candidates: List[Tuple[int, str]] = []
        for record in list(getattr(self.store, "installed_games", []) or []):
            if not record:
                continue
            if str(getattr(record, "game_id", "") or "").strip() != "switch_emulator":
                continue
            root = str(getattr(record, "install_path", "") or "").strip()
            updated_at = _safe_int(getattr(record, "updated_at", 0), 0)
            if root:
                installed_candidates.append((updated_at, root))

        installed_candidates.sort(key=lambda row: row[0], reverse=True)
        for _, root_dir in installed_candidates:
            exe = try_dir(root_dir, source="installed.switch_emulator")
            if exe:
                return exe, diagnostics

        task_candidates: List[Tuple[int, str]] = []
        for task in list(getattr(self.store, "tasks", []) or []):
            if not task:
                continue
            if str(getattr(task, "game_id", "") or "").strip() != "switch_emulator":
                continue
            if str(getattr(task, "install_status", "") or "").strip().lower() != "installed":
                continue
            root = str(getattr(task, "installed_path", "") or "").strip()
            updated_at = _safe_int(getattr(task, "updated_at", 0), 0)
            if root:
                task_candidates.append((updated_at, root))

        task_candidates.sort(key=lambda row: row[0], reverse=True)
        for _, root_dir in task_candidates:
            exe = try_dir(root_dir, source="tasks.switch_emulator")
            if exe:
                return exe, diagnostics

        return "", diagnostics

    async def get_switch_emulator_status(self) -> Dict[str, Any]:
        """返回 Switch 模拟器就绪状态（用于前端提示与导入 Steam）。"""
        exe_path, diagnostics = self._resolve_switch_emulator_exe_path()
        ok = bool(exe_path and os.path.isfile(exe_path))
        message = "已检测到 Switch 模拟器" if ok else "未检测到 Switch 模拟器，请先在设置页下载/配置"
        return {
            "installed": ok,
            "exe_path": str(exe_path or ""),
            "message": message,
            "diagnostics": diagnostics,
        }

    def _sanitize_path_segment(self, text: str) -> str:
        """清理路径片段，避免非法字符。"""
        raw = str(text or "").strip()
        if not raw:
            return ""
        cleaned_chars: List[str] = []
        for ch in raw:
            if ord(ch) < 32:
                continue
            if ch in '<>:"/\\|?*':
                continue
            cleaned_chars.append(ch)
        value = "".join(cleaned_chars).strip().strip(".")
        return value

    def _resolve_task_local_path(self, task: TianyiTaskRecord) -> str:
        local_path = str(getattr(task, "local_path", "") or "").strip()
        if not local_path:
            local_path = os.path.join(str(getattr(task, "download_dir", "") or "").strip(), str(getattr(task, "file_name", "") or "").strip())
        if not local_path:
            return ""
        try:
            return os.path.realpath(os.path.expanduser(local_path))
        except Exception:
            return str(local_path)

    def _resolve_multipart_archive_bundle(self, task: TianyiTaskRecord) -> Optional[Dict[str, Any]]:
        """解析分卷压缩包分组信息（同一组分卷只应解压一次）。

        返回结构：
        - kind: 类型标识
        - members: List[TianyiTaskRecord] 分卷成员任务（含主卷）
        - primary_task: TianyiTaskRecord | None 主卷任务
        - primary_name: str 期望主卷文件名（用于提示）
        """
        file_name = str(getattr(task, "file_name", "") or "").strip()
        if not file_name:
            return None

        share_id = str(getattr(task, "share_id", "") or "").strip()
        download_dir = str(getattr(task, "download_dir", "") or "").strip()
        game_id = str(getattr(task, "game_id", "") or "").strip()
        if not share_id or not download_dir:
            return None

        normalized_dir = os.path.realpath(os.path.expanduser(download_dir))
        scope: List[TianyiTaskRecord] = []
        for item in list(self.store.tasks):
            try:
                if str(getattr(item, "share_id", "") or "").strip() != share_id:
                    continue
                if game_id and str(getattr(item, "game_id", "") or "").strip() != game_id:
                    continue
                item_dir = str(getattr(item, "download_dir", "") or "").strip()
                if not item_dir:
                    continue
                if os.path.realpath(os.path.expanduser(item_dir)) != normalized_dir:
                    continue
            except Exception:
                continue
            scope.append(item)

        # 同一份文件允许重复下载（例如重试/重新下载），此时 tasks 里会存在多个同名记录。
        # 分卷判断与等待逻辑只应基于“最新的一份”，否则会被旧任务（已删除压缩包/失败任务）干扰，
        # 导致主卷一直等待缺失文件或直接判定失败。
        deduped_scope: Dict[str, TianyiTaskRecord] = {}
        for item in scope:
            key = str(getattr(item, "file_name", "") or "").strip().lower()
            if not key:
                continue
            current = deduped_scope.get(key)
            if current is None:
                deduped_scope[key] = item
                continue
            item_created = _safe_int(getattr(item, "created_at", 0), 0)
            current_created = _safe_int(getattr(current, "created_at", 0), 0)
            if item_created > current_created:
                deduped_scope[key] = item
                continue
            if item_created < current_created:
                continue
            item_updated = _safe_int(getattr(item, "updated_at", 0), 0)
            current_updated = _safe_int(getattr(current, "updated_at", 0), 0)
            if item_updated > current_updated:
                deduped_scope[key] = item
        if deduped_scope:
            scope = list(deduped_scope.values())

        name = file_name
        lower = name.lower()

        def by_name(value: str) -> Optional[TianyiTaskRecord]:
            target = str(value or "").strip().lower()
            if not target:
                return None
            for item in scope:
                if str(getattr(item, "file_name", "") or "").strip().lower() == target:
                    return item
            return None

        # Pattern: foo.part1.rar / foo.part01.rar
        m = MULTIPART_RAR_PART_RE.match(name)
        if m:
            prefix = str(m.group("prefix") or "").strip()
            part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.part(?P<idx>\d{{1,4}})\.rar$")
            parts: List[Tuple[int, TianyiTaskRecord]] = []
            for item in scope:
                mm = part_re.match(str(getattr(item, "file_name", "") or "").strip())
                if not mm:
                    continue
                try:
                    idx = int(mm.group("idx"))
                except Exception:
                    idx = 0
                parts.append((idx, item))
            if not parts:
                return None
            parts.sort(key=lambda pair: (pair[0] <= 0, pair[0], str(getattr(pair[1], "file_name", ""))))
            primary_task = next((item for idx, item in parts if idx == 1), None)
            return {
                "kind": "rar_part",
                "members": [item for _, item in parts],
                "primary_task": primary_task,
                "primary_name": str(getattr(primary_task, "file_name", "") or f"{prefix}.part1.rar").strip(),
            }

        # Pattern: foo.7z.001 / foo.zip.001 / foo.rar.001
        m = MULTIPART_NUMBERED_ARCHIVE_RE.match(name)
        if m:
            base = str(m.group("base") or "").strip()
            split_re = re.compile(rf"(?i)^{re.escape(base)}\.(?P<idx>\d{{2,4}})$")
            parts: List[Tuple[int, TianyiTaskRecord]] = []
            for item in scope:
                mm = split_re.match(str(getattr(item, "file_name", "") or "").strip())
                if not mm:
                    continue
                try:
                    idx = int(mm.group("idx"))
                except Exception:
                    idx = 0
                parts.append((idx, item))
            if not parts:
                # 至少包含当前任务自身
                return None
            parts.sort(key=lambda pair: (pair[0] <= 0, pair[0], str(getattr(pair[1], "file_name", ""))))
            primary_task = next((item for idx, item in parts if idx == 1), None)
            if primary_task is None:
                primary_task = next((item for idx, item in parts if idx == 0), None)
            return {
                "kind": "numbered",
                "members": [item for _, item in parts],
                "primary_task": primary_task,
                "primary_name": str(getattr(primary_task, "file_name", "") or f"{base}.001").strip(),
            }

        # Pattern: foo.z01 + foo.zip
        m = MULTIPART_ZIP_Z_RE.match(name)
        if m:
            prefix = str(m.group("prefix") or "").strip()
            primary_name = f"{prefix}.zip"
            part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.z(?P<idx>\d{{2}})$")
            parts: List[Tuple[int, TianyiTaskRecord]] = []
            for item in scope:
                mm = part_re.match(str(getattr(item, "file_name", "") or "").strip())
                if not mm:
                    continue
                try:
                    idx = int(mm.group("idx"))
                except Exception:
                    idx = 0
                parts.append((idx, item))
            if not parts:
                return None
            primary_task = by_name(primary_name)
            members: List[TianyiTaskRecord] = []
            if primary_task is not None:
                members.append(primary_task)
            members.extend(item for _, item in sorted(parts, key=lambda pair: (pair[0] <= 0, pair[0])))
            return {
                "kind": "zip_z",
                "members": members,
                "primary_task": primary_task,
                "primary_name": primary_name,
            }

        # Pattern: foo.r00 + foo.rar
        m = MULTIPART_RAR_R_RE.match(name)
        if m:
            prefix = str(m.group("prefix") or "").strip()
            primary_name = f"{prefix}.rar"
            part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.r(?P<idx>\d{{2}})$")
            parts: List[Tuple[int, TianyiTaskRecord]] = []
            for item in scope:
                mm = part_re.match(str(getattr(item, "file_name", "") or "").strip())
                if not mm:
                    continue
                try:
                    idx = int(mm.group("idx"))
                except Exception:
                    idx = 0
                parts.append((idx, item))
            if not parts:
                return None
            primary_task = by_name(primary_name)
            members: List[TianyiTaskRecord] = []
            if primary_task is not None:
                members.append(primary_task)
            members.extend(item for _, item in sorted(parts, key=lambda pair: (pair[0] <= 0, pair[0])))
            return {
                "kind": "rar_r",
                "members": members,
                "primary_task": primary_task,
                "primary_name": primary_name,
            }

        # Primary detection: foo.zip has foo.z01 parts.
        if lower.endswith(".zip"):
            prefix = name[:-4]
            part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.z(?P<idx>\d{{2}})$")
            parts: List[Tuple[int, TianyiTaskRecord]] = []
            for item in scope:
                mm = part_re.match(str(getattr(item, "file_name", "") or "").strip())
                if not mm:
                    continue
                try:
                    idx = int(mm.group("idx"))
                except Exception:
                    idx = 0
                parts.append((idx, item))
            if parts:
                primary_task = by_name(name) or task
                members = [primary_task] + [item for _, item in sorted(parts, key=lambda pair: (pair[0] <= 0, pair[0]))]
                return {
                    "kind": "zip_z",
                    "members": members,
                    "primary_task": primary_task,
                    "primary_name": str(getattr(primary_task, "file_name", "") or "").strip(),
                }

        # Primary detection: foo.rar has foo.r00 parts.
        if lower.endswith(".rar"):
            prefix = name[:-4]
            part_re = re.compile(rf"(?i)^{re.escape(prefix)}\.r(?P<idx>\d{{2}})$")
            parts: List[Tuple[int, TianyiTaskRecord]] = []
            for item in scope:
                mm = part_re.match(str(getattr(item, "file_name", "") or "").strip())
                if not mm:
                    continue
                try:
                    idx = int(mm.group("idx"))
                except Exception:
                    idx = 0
                parts.append((idx, item))
            if parts:
                primary_task = by_name(name) or task
                members = [primary_task] + [item for _, item in sorted(parts, key=lambda pair: (pair[0] <= 0, pair[0]))]
                return {
                    "kind": "rar_r",
                    "members": members,
                    "primary_task": primary_task,
                    "primary_name": str(getattr(primary_task, "file_name", "") or "").strip(),
                }

        return None

    def _is_archive_file(self, file_path: str) -> bool:
        """判断文件是否为支持的压缩包。"""
        normalized = str(file_path or "").strip().lower()
        if any(normalized.endswith(ext) for ext in ARCHIVE_SUFFIXES):
            return True
        # 支持 7z/zip/rar 的 .001 这种分卷命名（例如 xxx.7z.001）
        leaf = os.path.basename(normalized)
        return bool(MULTIPART_NUMBERED_ARCHIVE_RE.match(leaf))

    def _extract_archive_to_dir(
        self,
        archive_path: str,
        target_dir: str,
        progress_cb: Optional[Callable[[float], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, str]:
        """解压压缩包到目标目录。"""
        normalized = str(archive_path or "").strip().lower()
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as exc:
            return False, f"创建安装目录失败: {exc}"

        parent_dir = os.path.dirname(target_dir)
        staging_parent = parent_dir if parent_dir and os.path.isdir(parent_dir) else None
        try:
            staging_dir = tempfile.mkdtemp(prefix="freedeck_extract_", dir=staging_parent)
        except Exception:
            staging_dir = tempfile.mkdtemp(prefix="freedeck_extract_")

        try:
            if cancel_event is not None and cancel_event.is_set():
                raise InstallCancelledError("已取消安装")

            leaf = os.path.basename(normalized)
            use_7z = (
                normalized.endswith(".7z")
                or normalized.endswith(".rar")
                or normalized.endswith(".zip")
                or bool(MULTIPART_NUMBERED_ARCHIVE_RE.match(leaf))
            )
            if not use_7z and normalized.endswith(".zip"):
                # zip 分卷（.z01/.z02）由 7z 处理，Python 的 zipfile 不稳定支持。
                try:
                    real_archive = os.path.realpath(os.path.expanduser(str(archive_path or "").strip()))
                    if real_archive and len(real_archive) > 4:
                        prefix = real_archive[:-4]
                        if os.path.isfile(prefix + ".z01") or os.path.isfile(prefix + ".Z01"):
                            use_7z = True
                except Exception:
                    pass

            if use_7z:
                # 在运行 7z 前做一个轻量的文件头校验，避免下载到错误内容（例如 HTML 错误页）导致 7z exit=2。
                try:
                    real_archive = os.path.realpath(os.path.expanduser(str(archive_path or "").strip()))
                    leaf_for_check = os.path.basename(real_archive).lower() if real_archive else leaf
                    expected_magic: Optional[bytes] = None
                    expected_kind = ""

                    if leaf_for_check.endswith(".zip"):
                        expected_magic = b"PK"
                        expected_kind = "zip"
                    elif leaf_for_check.endswith(".rar") or MULTIPART_RAR_PART_RE.match(leaf_for_check) or MULTIPART_RAR_R_RE.match(leaf_for_check):
                        expected_magic = b"Rar!"
                        expected_kind = "rar"
                    elif leaf_for_check.endswith(".7z"):
                        expected_magic = b"7z\xBC\xAF\x27\x1C"
                        expected_kind = "7z"
                    else:
                        mm = MULTIPART_NUMBERED_ARCHIVE_RE.match(leaf_for_check)
                        if mm:
                            base = str(mm.group("base") or "").lower()
                            if base.endswith(".zip"):
                                expected_magic = b"PK"
                                expected_kind = "zip"
                            elif base.endswith(".rar"):
                                expected_magic = b"Rar!"
                                expected_kind = "rar"
                            elif base.endswith(".7z"):
                                expected_magic = b"7z\xBC\xAF\x27\x1C"
                                expected_kind = "7z"

                    if expected_magic is not None and real_archive and os.path.isfile(real_archive):
                        try:
                            with open(real_archive, "rb") as fp:
                                head = fp.read(16)
                        except Exception as exc:
                            return False, f"读取压缩包失败: {exc}"

                        stripped = head.lstrip().lower()
                        if stripped.startswith(b"<!doctype") or stripped.startswith(b"<html") or stripped.startswith(b"<head"):
                            return False, "下载内容疑似网页错误页（HTML），压缩包无效，请重试下载"

                        ok_magic = False
                        if expected_kind == "zip":
                            # 注意：zip 分卷（.z01/.z02 + .zip）主卷 `.zip` 不一定以 `PK` 开头，
                            # 因为它可能处于整个 zip 数据流的中间/尾部。此处只做“明显的 HTML 错误页”拦截，
                            # 不强行要求 `.zip` 的文件头签名，否则会误判为“格式异常”。
                            try:
                                has_zip_z_parts = False
                                z01_path = ""
                                if leaf_for_check.endswith(".zip") and real_archive and len(real_archive) > 4:
                                    prefix = real_archive[:-4]
                                    cand1 = prefix + ".z01"
                                    cand2 = prefix + ".Z01"
                                    if os.path.isfile(cand1):
                                        has_zip_z_parts = True
                                        z01_path = cand1
                                    elif os.path.isfile(cand2):
                                        has_zip_z_parts = True
                                        z01_path = cand2

                                if has_zip_z_parts:
                                    # 额外确认首分卷不是错误页/明显异常（尽量不做强校验，避免误伤）。
                                    if z01_path:
                                        try:
                                            with open(z01_path, "rb") as fp:
                                                z_head = fp.read(16)
                                            z_stripped = z_head.lstrip().lower()
                                            if z_stripped.startswith(b"<!doctype") or z_stripped.startswith(b"<html") or z_stripped.startswith(b"<head"):
                                                return False, "下载内容疑似网页错误页（HTML），压缩包无效，请重试下载"
                                        except Exception:
                                            pass
                                    ok_magic = True
                                else:
                                    ok_magic = head.startswith(b"PK")
                            except Exception:
                                ok_magic = head.startswith(b"PK")
                        elif expected_kind == "rar":
                            ok_magic = head.startswith(b"Rar!")
                        else:
                            ok_magic = head.startswith(expected_magic)
                        if not ok_magic:
                            return False, "压缩包格式异常（可能下载不完整或文件损坏），无法解压"
                except Exception:
                    # 头部校验仅用于优化提示，失败时不阻断后续解压。
                    pass

                try:
                    if progress_cb:
                        def _mapped(percent: float) -> None:
                            try:
                                value = max(0.0, min(100.0, float(percent)))
                            except Exception:
                                value = 0.0
                            # 预留部分进度给解压后的目录整理。
                            progress_cb(min(90.0, value * 0.9))
                        self.seven_zip.extract_archive(
                            archive_path,
                            staging_dir,
                            progress_cb=_mapped,
                            cancel_event=cancel_event,
                        )
                    else:
                        self.seven_zip.extract_archive(archive_path, staging_dir, cancel_event=cancel_event)
                except SevenZipCancelledError as exc:
                    raise InstallCancelledError("已取消安装") from exc
                except SevenZipError as exc:
                    # RAR 在少数情况下 7z 可能兼容性不足，尝试回退到系统 unrar。
                    if normalized.endswith(".rar"):
                        ok, reason = self._extract_rar_with_unrar(
                            archive_path=archive_path,
                            output_dir=staging_dir,
                            progress_cb=progress_cb,
                            cancel_event=cancel_event,
                        )
                        if not ok:
                            return False, f"{exc}；unrar 回退失败: {reason}"
                    else:
                        return False, str(exc)
                except Exception as exc:
                    return False, f"7z 解压异常: {exc}"
            else:
                try:
                    if progress_cb:
                        progress_cb(15.0)
                    shutil.unpack_archive(archive_path, staging_dir)
                    if progress_cb:
                        progress_cb(85.0)
                except Exception as exc:
                    return False, f"解压失败: {exc}"

            try:
                if cancel_event is not None and cancel_event.is_set():
                    raise InstallCancelledError("已取消安装")
                if progress_cb:
                    progress_cb(92.0)
                self._merge_extracted_content(staging_dir, target_dir)
                if progress_cb:
                    progress_cb(100.0)
            except Exception as exc:
                return False, f"整理解压目录失败: {exc}"
            return True, ""
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _extract_rar_with_unrar(
        self,
        *,
        archive_path: str,
        output_dir: str,
        progress_cb: Optional[Callable[[float], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, str]:
        """使用系统 unrar 解压（主要用于 RAR 分卷兼容性回退）。"""
        binary = shutil.which("unrar")
        if not binary:
            return False, "未找到 unrar"

        archive = os.path.realpath(os.path.expanduser(str(archive_path or "").strip()))
        target = os.path.realpath(os.path.expanduser(str(output_dir or "").strip()))
        if not archive or not os.path.isfile(archive):
            return False, "待解压文件不存在"
        if not target:
            return False, "安装目录无效"
        os.makedirs(target, exist_ok=True)
        if cancel_event is not None and cancel_event.is_set():
            raise InstallCancelledError("已取消安装")

        # `-idq` 安静模式；保留错误输出用于诊断。
        args = [binary, "x", "-o+", "-idq", archive, target + os.sep]
        output_tail: List[str] = []
        cwd = os.path.dirname(archive) or None
        try:
            process = subprocess.Popen(
                args,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )
        except Exception as exc:
            return False, f"启动 unrar 失败: {exc}"

        if cancel_event is not None:
            def _watch_cancel() -> None:
                try:
                    while True:
                        try:
                            if process.poll() is not None:
                                return
                        except Exception:
                            return
                        if cancel_event.wait(timeout=0.25):
                            break
                except Exception:
                    return
                try:
                    if process.poll() is not None:
                        return
                    try:
                        process.terminate()
                    except Exception:
                        pass
                    try:
                        process.wait(timeout=3.0)
                    except Exception:
                        try:
                            process.kill()
                        except Exception:
                            pass
                except Exception:
                    return

            threading.Thread(target=_watch_cancel, name="freedeck_unrar_cancel", daemon=True).start()

        if progress_cb:
            try:
                progress_cb(10.0)
            except Exception:
                pass

        try:
            if process.stdout is not None:
                for line in process.stdout:
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    text = str(line or "").strip()
                    if not text:
                        continue
                    output_tail.append(text)
                    if len(output_tail) > 12:
                        output_tail.pop(0)
        finally:
            return_code = process.wait()

        if cancel_event is not None and cancel_event.is_set():
            raise InstallCancelledError("已取消安装")
        if return_code != 0:
            hint = " | ".join(output_tail[-6:]) if output_tail else "no output"
            return False, f"unrar 解压失败，exit={return_code}，诊断={hint}"

        if progress_cb:
            try:
                progress_cb(90.0)
            except Exception:
                pass
        return True, ""

    def _merge_extracted_content(self, staging_dir: str, target_dir: str) -> None:
        """将临时解压目录内容合并到目标目录，并尽量去掉一层包内同名根目录。"""
        if not staging_dir or not os.path.isdir(staging_dir):
            return
        os.makedirs(target_dir, exist_ok=True)

        entries = [name for name in os.listdir(staging_dir) if name not in {".", ".."}]
        if not entries:
            return

        source_root = staging_dir
        target_base = os.path.basename(os.path.normpath(target_dir))
        preferred = os.path.join(staging_dir, target_base)
        if target_base and os.path.isdir(preferred):
            source_root = preferred
        else:
            dir_entries = [name for name in entries if os.path.isdir(os.path.join(staging_dir, name))]
            file_entries = [name for name in entries if not os.path.isdir(os.path.join(staging_dir, name))]
            if len(dir_entries) == 1 and not file_entries:
                source_root = os.path.join(staging_dir, dir_entries[0])

        for name in os.listdir(source_root):
            source_path = os.path.join(source_root, name)
            target_path = os.path.join(target_dir, name)
            self._merge_path(source_path, target_path)

    def _merge_path(self, source_path: str, target_path: str) -> None:
        """将 source_path 合并到 target_path，目录递归合并，文件冲突时覆盖。"""
        if not os.path.exists(source_path):
            return

        if not os.path.exists(target_path):
            shutil.move(source_path, target_path)
            return

        source_is_dir = os.path.isdir(source_path) and not os.path.islink(source_path)
        target_is_dir = os.path.isdir(target_path) and not os.path.islink(target_path)
        if source_is_dir and target_is_dir:
            for child in os.listdir(source_path):
                self._merge_path(
                    os.path.join(source_path, child),
                    os.path.join(target_path, child),
                )
            try:
                os.rmdir(source_path)
            except Exception:
                pass
            return

        if target_is_dir:
            shutil.rmtree(target_path, ignore_errors=True)
        else:
            try:
                os.remove(target_path)
            except Exception:
                pass
        shutil.move(source_path, target_path)

    async def _set_qr_login_state(
        self,
        *,
        session_id: str,
        stage: str,
        message: str,
        reason: str,
        next_action: str,
        user_account: str,
        image_url: str,
        expires_at: int,
        diagnostics: Optional[Dict[str, Any]],
    ) -> None:
        """更新二维码登录状态。"""
        self._qr_login_state = {
            "session_id": str(session_id or ""),
            "stage": str(stage or "idle"),
            "message": str(message or ""),
            "reason": str(reason or ""),
            "next_action": str(next_action or ""),
            "user_account": str(user_account or ""),
            "image_url": str(image_url or ""),
            "expires_at": int(expires_at or 0),
            "updated_at": _now_wall_ts(),
            "diagnostics": diagnostics or {},
        }

    async def _safe_close_client_session(self, client: Optional[aiohttp.ClientSession]) -> None:
        """安全关闭 aiohttp 会话。"""
        if not isinstance(client, aiohttp.ClientSession):
            return
        if client.closed:
            return
        try:
            await client.close()
        except Exception:
            pass

    async def _close_qr_login_context_locked(self) -> None:
        """关闭并清理二维码登录上下文（需持有锁）。"""
        context = self._qr_login_context
        self._qr_login_context = None
        if not isinstance(context, dict):
            return
        await self._safe_close_client_session(context.get("client"))

    def _build_qr_ssl_context(self) -> Tuple[ssl.SSLContext, Dict[str, Any]]:
        """构建二维码登录用 TLS 上下文并输出证书链诊断。"""
        diagnostics: Dict[str, Any] = {
            "mode": "verify",
            "selected_ca_file": "",
            "candidate_ca_files": [],
            "candidate_errors": [],
        }

        env_cert_file = str(os.environ.get("SSL_CERT_FILE", "") or "").strip()
        candidates: List[str] = []
        if env_cert_file:
            candidates.append(env_cert_file)
        candidates.extend(list(QR_CA_CANDIDATE_FILES))

        try:
            import certifi  # type: ignore

            certifi_path = str(certifi.where() or "").strip()
            if certifi_path:
                candidates.append(certifi_path)
        except Exception:
            pass

        dedup_candidates: List[str] = []
        seen = set()
        for raw in candidates:
            path = os.path.realpath(os.path.expanduser(str(raw).strip()))
            if not path or path in seen:
                continue
            seen.add(path)
            dedup_candidates.append(path)

        diagnostics["candidate_ca_files"] = dedup_candidates
        for path in dedup_candidates:
            if not os.path.isfile(path):
                continue
            try:
                context = ssl.create_default_context(cafile=path)
                diagnostics["selected_ca_file"] = path
                return context, diagnostics
            except Exception as exc:
                diagnostics["candidate_errors"].append({"path": path, "error": str(exc)})

        context = ssl.create_default_context()
        diagnostics["selected_ca_file"] = "system_default"

        # 仅用于紧急排障，默认不关闭校验。
        insecure_flag = str(os.environ.get("FREEDECK_QR_INSECURE_TLS", "") or "").strip().lower()
        if insecure_flag in {"1", "true", "yes"}:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            diagnostics["mode"] = "insecure"
            diagnostics["selected_ca_file"] = "insecure_env_override"

        return context, diagnostics

    def _build_qr_headers(self, *, req_id: str, lt: str, referer: str) -> Dict[str, str]:
        """构建二维码相关请求头。"""
        headers: Dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0)",
            "Accept": "application/json, text/plain, */*",
            "Referer": referer or "https://open.e.189.cn/",
        }
        if req_id:
            headers["reqId"] = req_id
            headers["REQID"] = req_id
        if lt:
            headers["lt"] = lt
        return headers

    def _parse_json_like_text(self, raw_text: str) -> Dict[str, Any]:
        """解析 text/html 包裹的 JSON 返回。"""
        text = str(raw_text or "").strip()
        if not text:
            raise TianyiApiError("接口返回为空")
        try:
            payload = json.loads(text)
        except Exception as exc:
            raise TianyiApiError(f"JSON 解析失败: {exc}") from exc
        if not isinstance(payload, dict):
            raise TianyiApiError("接口返回结构异常")
        return payload

    def _extract_qr_status_code(self, payload: Dict[str, Any]) -> int:
        """提取二维码轮询状态码。"""
        for key in ("status", "result", "code", "res_code"):
            if key not in payload:
                continue
            try:
                return int(str(payload.get(key)))
            except Exception:
                continue
        return -99999

    def _extract_qr_redirect_url(self, payload: Dict[str, Any]) -> str:
        """提取扫码成功后的跳转地址。"""
        direct_keys = ("redirectUrl", "redirectURL", "url", "targetUrl", "jumpUrl")
        for key in direct_keys:
            value = str(payload.get(key, "") or "").strip()
            if value:
                return value
        data_obj = payload.get("data")
        if isinstance(data_obj, dict):
            for key in direct_keys:
                value = str(data_obj.get(key, "") or "").strip()
                if value:
                    return value
        return ""

    def _build_tianyi_cookie_from_cookie_jar(self, jar: aiohttp.CookieJar) -> str:
        """从 aiohttp CookieJar 组装 189 域 Cookie 头。"""
        kv_map: Dict[str, str] = {}

        # 优先按目标域筛选，兼容 host-only cookie（无 Domain 属性）。
        for target in (
            URL("https://cloud.189.cn/"),
            URL("https://h5.cloud.189.cn/"),
            URL("https://open.e.189.cn/"),
        ):
            try:
                scoped = jar.filter_cookies(target)
            except Exception:
                scoped = {}
            for key, morsel in scoped.items():
                name = str(key or "").strip()
                value = str(getattr(morsel, "value", "") or "").strip()
                if not name or not value:
                    continue
                # 保留 cloud/h5 首次命中的同名值，避免被 open.e 同名 cookie 覆盖。
                if name not in kv_map:
                    kv_map[name] = value

        # 兜底：补充所有 189 域 cookie（防止某些环境 filter 丢失）。
        for morsel in jar:
            try:
                domain = str(morsel["domain"] or "").lower()
            except Exception:
                domain = ""
            if domain and "189.cn" not in domain:
                continue
            name = str(getattr(morsel, "key", "") or "").strip()
            value = str(getattr(morsel, "value", "") or "").strip()
            if not name or not value:
                continue
            if name not in kv_map:
                kv_map[name] = value

        if not kv_map:
            return ""
        ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda item: item[0].lower())]
        return "; ".join(ordered)

    async def _bootstrap_qr_login_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """初始化二维码登录上下文。"""
        client = context.get("client")
        if not isinstance(client, aiohttp.ClientSession):
            raise TianyiApiError("二维码会话未初始化")

        redirect_url = quote("https://cloud.189.cn/web/main/", safe="")
        bootstrap_url = f"https://cloud.189.cn/api/portal/loginUrl.action?redirectURL={redirect_url}&pageId=1"

        base_headers = {
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        async with client.get(bootstrap_url, headers=base_headers, allow_redirects=False) as resp:
            if resp.status in {301, 302, 303, 307, 308}:
                login_entry_url = str(resp.headers.get("Location", "") or "").strip()
            else:
                login_entry_url = str(resp.url)
            if not login_entry_url:
                raise TianyiApiError("未获取到天翼登录入口地址")

        async with client.get(login_entry_url, headers=base_headers, allow_redirects=True) as resp:
            login_page_url = str(resp.url)
            await resp.text()

        query = parse_qs(urlparse(login_page_url).query or "", keep_blank_values=True)
        app_id = str((query.get("appId") or [""])[0] or "").strip()
        lt = str((query.get("lt") or [""])[0] or "").strip()
        req_id = str((query.get("reqId") or [""])[0] or "").strip()
        if not app_id or not lt or not req_id:
            raise TianyiApiError("登录页面参数缺失（appId/lt/reqId）")

        api_headers = self._build_qr_headers(req_id=req_id, lt=lt, referer=login_page_url)

        async with client.post(
            "https://open.e.189.cn/api/logbox/oauth2/appConf.do",
            data={"version": "2.0", "appKey": app_id},
            headers=api_headers,
        ) as resp:
            app_conf_text = await resp.text()
            if resp.status >= 400:
                raise TianyiApiError(f"appConf 请求失败 status={resp.status}")
        app_conf_payload = self._parse_json_like_text(app_conf_text)
        app_conf_data = app_conf_payload.get("data")
        if not isinstance(app_conf_data, dict):
            raise TianyiApiError("appConf 返回异常")

        async with client.post(
            "https://open.e.189.cn/api/logbox/oauth2/getUUID.do",
            data={"appId": app_id},
            headers=api_headers,
        ) as resp:
            uuid_text = await resp.text()
            if resp.status >= 400:
                raise TianyiApiError(f"getUUID 请求失败 status={resp.status}")
        uuid_payload = self._parse_json_like_text(uuid_text)
        if self._extract_qr_status_code(uuid_payload) != QR_STATUS_SUCCESS:
            msg = str(uuid_payload.get("msg", "") or "二维码生成失败")
            raise TianyiApiError(msg)

        uuid_value = str(uuid_payload.get("uuid", "") or "").strip()
        encryuuid = str(uuid_payload.get("encryuuid", "") or "").strip()
        encodeuuid = str(uuid_payload.get("encodeuuid", "") or "").strip()
        if not uuid_value or not encryuuid or not encodeuuid:
            raise TianyiApiError("二维码参数缺失（uuid/encryuuid/encodeuuid）")

        image_remote_url = f"https://open.e.189.cn/api/logbox/oauth2/image.do?uuid={encodeuuid}&REQID={req_id}"
        state_payload = {
            "appId": app_id,
            "encryuuid": encryuuid,
            "date": str(int(time.time() * 1000)),
            "uuid": uuid_value,
            "returnUrl": str(app_conf_data.get("returnUrl") or ""),
            "clientType": str(app_conf_data.get("clientType") or "1"),
            "timeStamp": str(int(time.time() * 1000)),
            "cb_SaveName": str(app_conf_data.get("defaultSaveName") or ""),
            "isOauth2": "false" if str(app_conf_data.get("isOauth2")).lower() == "false" else "true",
            "state": str(app_conf_data.get("state") or ""),
            "paramId": str(app_conf_data.get("paramId") or ""),
        }

        return {
            "app_id": app_id,
            "lt": lt,
            "req_id": req_id,
            "login_page_url": login_page_url,
            "image_remote_url": image_remote_url,
            "state_payload": state_payload,
        }

    async def _finalize_qr_login_success(
        self,
        *,
        context: Dict[str, Any],
        redirect_url: str,
    ) -> Tuple[str, str, str]:
        """扫码成功后拉起回调并验证账号。"""
        client = context.get("client")
        if not isinstance(client, aiohttp.ClientSession):
            return "", "", "qr_client_invalid"

        req_id = str(context.get("req_id", ""))
        lt = str(context.get("lt", ""))
        login_page_url = str(context.get("login_page_url", ""))
        headers = self._build_qr_headers(req_id=req_id, lt=lt, referer=login_page_url)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        candidate_urls: List[str] = []
        redirect = str(redirect_url or "").strip()
        if redirect:
            candidate_urls.append(redirect)
        candidate_urls.append("https://cloud.189.cn/web/main/")

        for url in candidate_urls:
            try:
                async with client.get(url, headers=headers, allow_redirects=True) as resp:
                    await resp.read()
            except Exception:
                continue

        cloud_scoped_count = 0
        open_scoped_count = 0
        try:
            cloud_scoped_count = len(client.cookie_jar.filter_cookies(URL("https://cloud.189.cn/")).items())
        except Exception:
            cloud_scoped_count = 0
        try:
            open_scoped_count = len(client.cookie_jar.filter_cookies(URL("https://open.e.189.cn/")).items())
        except Exception:
            open_scoped_count = 0

        cookie = self._build_tianyi_cookie_from_cookie_jar(client.cookie_jar)
        if not cookie:
            return "", "", f"cookie_missing:cloud={cloud_scoped_count},open={open_scoped_count}"

        account, verify_reason = await self._verify_cookie_candidate(cookie)
        if not account:
            scoped = f"cloud={cloud_scoped_count},open={open_scoped_count}"
            return "", cookie, f"{verify_reason or 'account_verify_failed'}:{scoped}"
        return account, cookie, ""

    async def _set_capture_state(
        self,
        *,
        stage: str,
        message: str,
        reason: str,
        next_action: str,
        user_account: str,
        diagnostics: Optional[Dict[str, Any]],
        source_attempts: Optional[List[str]] = None,
        success_source: str = "",
        source_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """更新自动采集状态。"""
        attempts = [str(item) for item in (source_attempts or []) if str(item).strip()]
        source_diag = dict(source_diagnostics or {})
        self._capture_state = {
            "stage": str(stage or "idle"),
            "message": str(message or ""),
            "reason": str(reason or ""),
            "next_action": str(next_action or ""),
            "user_account": str(user_account or ""),
            "updated_at": _now_wall_ts(),
            "diagnostics": diagnostics or {},
            "source_attempts": attempts,
            "success_source": str(success_source or ""),
            "source_diagnostics": source_diag,
        }

    async def _set_baidu_capture_state(
        self,
        *,
        stage: str,
        message: str,
        reason: str,
        next_action: str,
        user_account: str,
        diagnostics: Optional[Dict[str, Any]],
        source_attempts: Optional[List[str]] = None,
        success_source: str = "",
        source_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """更新百度网盘自动采集状态。"""
        attempts = [str(item) for item in (source_attempts or []) if str(item).strip()]
        source_diag = dict(source_diagnostics or {})
        self._baidu_capture_state = {
            "stage": str(stage or "idle"),
            "message": str(message or ""),
            "reason": str(reason or ""),
            "next_action": str(next_action or ""),
            "user_account": str(user_account or ""),
            "updated_at": _now_wall_ts(),
            "diagnostics": diagnostics or {},
            "source_attempts": attempts,
            "success_source": str(success_source or ""),
            "source_diagnostics": source_diag,
        }

    def _normalize_capture_host(self, host: str) -> str:
        """归一化采集探测中的 host，便于识别互跳。"""
        raw = str(host or "").strip().lower()
        if not raw:
            return ""
        if raw.endswith(".h5.cloud.189.cn") or raw == "h5.cloud.189.cn":
            return "h5.cloud.189.cn"
        if raw.endswith(".cloud.189.cn") or raw == "cloud.189.cn":
            return "cloud.189.cn"
        return raw

    def _extract_probe_hosts(self, probe: Dict[str, Any]) -> List[str]:
        """从探针结果提取 host 列表。"""
        hosts: List[str] = []
        pages = probe.get("page_candidates")
        if isinstance(pages, list):
            for item in pages:
                if not isinstance(item, dict):
                    continue
                host = self._normalize_capture_host(item.get("host", ""))
                if host and host not in hosts:
                    hosts.append(host)
        matched = probe.get("matched_page")
        if isinstance(matched, dict):
            host = self._normalize_capture_host(matched.get("host", ""))
            if host and host not in hosts:
                hosts.insert(0, host)
        return hosts

    def _is_capture_redirect_loop(self, host_history: List[str]) -> bool:
        """判断是否命中 cloud/h5 互跳。"""
        if len(host_history) < CAPTURE_LOOP_WINDOW:
            return False

        tail = host_history[-CAPTURE_LOOP_WINDOW:]
        if any(host not in CAPTURE_LOOP_CORE_HOSTS for host in tail):
            return False
        if len(set(tail)) != 2:
            return False
        for idx in range(1, len(tail)):
            if tail[idx] == tail[idx - 1]:
                return False
        return True

    def _build_capture_diag_payload(
        self,
        *,
        reason: str,
        source_diagnostics: Dict[str, Any],
        main_landing_detected: bool,
        host_history: Optional[List[str]] = None,
        elapsed_seconds: Optional[int] = None,
        remaining_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """构建统一诊断结构，便于前端展示。"""
        payload: Dict[str, Any] = {
            "reason": str(reason or ""),
            "main_landing_detected": bool(main_landing_detected),
            "source_diagnostics": dict(source_diagnostics or {}),
        }
        if host_history:
            payload["host_history"] = list(host_history)
        if elapsed_seconds is not None:
            payload["elapsed_seconds"] = int(elapsed_seconds)
        if remaining_seconds is not None:
            payload["remaining_seconds"] = int(remaining_seconds)
        return payload

    async def _verify_cookie_candidate(self, cookie: str) -> Tuple[str, str]:
        """校验候选 Cookie，返回账号与失败原因。"""
        normalized = str(cookie or "").strip()
        if not normalized:
            return "", "empty_cookie"
        try:
            account = await get_user_account(normalized)
        except Exception as exc:
            return "", f"account_verify_exception:{exc}"
        if not account:
            return "", "account_verify_failed"
        return account, ""

    def _derive_capture_failure_reason(
        self,
        source_diagnostics: Dict[str, Any],
        *,
        main_landing_detected: bool,
    ) -> str:
        """根据来源级探测信息生成失败原因。"""
        if main_landing_detected:
            return "main_landing_verify_failed"

        cdp_reason = str((source_diagnostics.get("cdp") or {}).get("reason", "")).strip()
        cookie_db_reason = str((source_diagnostics.get("cookie_db") or {}).get("reason", "")).strip()

        cdp_unavailable_reasons = {"cdp_endpoints_unreachable", "cdp_no_pages", "cdp_probe_exception"}
        cookie_db_unavailable_reasons = {"cookie_db_not_found", "cookie_db_read_failed", "cookie_db_probe_exception"}

        if cdp_reason in cdp_unavailable_reasons and cookie_db_reason in cookie_db_unavailable_reasons:
            return "all_sources_unavailable"
        if cdp_reason in cdp_unavailable_reasons and not cookie_db_reason:
            return "cdp_unavailable"
        if cookie_db_reason in cookie_db_unavailable_reasons and not cdp_reason:
            return "cookie_db_unavailable"
        return "no_valid_cookie"

    async def _attempt_capture_sources_once(self) -> Dict[str, Any]:
        """执行一次双来源采集并做统一账号校验。"""
        source_attempts: List[str] = []
        source_diagnostics: Dict[str, Any] = {}
        main_landing_detected = False

        for source in COOKIE_CAPTURE_SOURCES:
            source_attempts.append(source)
            try:
                if source == "cdp":
                    cookie, diag = await self._collect_tianyi_cookie_from_cdp()
                    if bool((diag or {}).get("main_landing_detected")):
                        main_landing_detected = True
                else:
                    cookie, diag = await self._collect_tianyi_cookie_from_cookie_db()
            except Exception as exc:
                cookie = ""
                diag = {
                    "ok": False,
                    "reason": f"{source}_probe_exception",
                    "error": str(exc),
                }

            source_diag = dict(diag or {})
            source_diagnostics[source] = source_diag

            if not cookie:
                continue

            source_diag["cookie_found"] = True
            account, verify_reason = await self._verify_cookie_candidate(cookie)
            source_diag["verify_reason"] = verify_reason
            if account:
                source_diag["ok"] = True
                source_diag["reason"] = ""
                return {
                    "success": True,
                    "cookie": cookie,
                    "account": account,
                    "reason": "",
                    "success_source": source,
                    "source_attempts": source_attempts,
                    "source_diagnostics": source_diagnostics,
                    "main_landing_detected": main_landing_detected,
                }
            if not str(source_diag.get("reason", "")).strip():
                source_diag["reason"] = "account_verify_failed"

        return {
            "success": False,
            "cookie": "",
            "account": "",
            "reason": self._derive_capture_failure_reason(
                source_diagnostics,
                main_landing_detected=main_landing_detected,
            ),
            "success_source": "",
            "source_attempts": source_attempts,
            "source_diagnostics": source_diagnostics,
            "main_landing_detected": main_landing_detected,
        }

    async def _attempt_capture_baidu_sources_once(self) -> Dict[str, Any]:
        """执行一次双来源采集百度网盘 Cookie（以 BDUSS 为登录标记）。"""
        source_attempts: List[str] = []
        source_diagnostics: Dict[str, Any] = {}

        for source in COOKIE_CAPTURE_SOURCES:
            source_attempts.append(source)
            try:
                if source == "cdp":
                    cookie, diag = await self._collect_baidu_cookie_from_cdp()
                else:
                    cookie, diag = await self._collect_baidu_cookie_from_cookie_db()
            except Exception as exc:
                cookie = ""
                diag = {
                    "ok": False,
                    "reason": f"{source}_probe_exception",
                    "error": str(exc),
                }

            source_diag = dict(diag or {})
            source_diagnostics[source] = source_diag

            if not cookie:
                continue

            source_diag["cookie_found"] = True
            source_diag["ok"] = True
            source_diag["reason"] = ""
            return {
                "success": True,
                "cookie": cookie,
                "account": str(source_diag.get("user_account", "") or "").strip(),
                "reason": "",
                "success_source": source,
                "source_attempts": source_attempts,
                "source_diagnostics": source_diagnostics,
            }

        return {
            "success": False,
            "cookie": "",
            "account": "",
            "reason": self._derive_capture_failure_reason(source_diagnostics, main_landing_detected=False),
            "success_source": "",
            "source_attempts": source_attempts,
            "source_diagnostics": source_diagnostics,
        }

    async def _baidu_capture_loop(
        self,
        timeout_seconds: int,
        seed_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """持续采集百度网盘 Cookie，直到超时或成功。"""
        start = time.monotonic()
        seed = dict(seed_diagnostics or {})

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= float(timeout_seconds):
                await self._set_baidu_capture_state(
                    stage="failed",
                    message="采集超时，请确认已在浏览器登录百度网盘后重试",
                    reason="timeout",
                    next_action="retry",
                    user_account=str(getattr(self.store.baidu_login, "user_account", "") or "").strip(),
                    diagnostics={
                        **seed,
                        "elapsed_seconds": round(elapsed, 3),
                    },
                )
                return

            attempt = await self._attempt_capture_baidu_sources_once()
            if bool(attempt.get("success")):
                cookie = str(attempt.get("cookie", "") or "").strip()
                account = str(attempt.get("account", "") or "").strip()
                success_source = str(attempt.get("success_source", "") or "")
                if cookie:
                    self.store.set_baidu_login(cookie, account)
                    self._invalidate_panel_cache(all_data=True)
                    await self._set_baidu_capture_state(
                        stage="completed",
                        message=f"登录成功：{account or '百度网盘'}",
                        reason="",
                        next_action="",
                        user_account=account,
                        diagnostics={
                            **seed,
                            "elapsed_seconds": round(elapsed, 3),
                        },
                        source_attempts=list(attempt.get("source_attempts") or []),
                        success_source=success_source,
                        source_diagnostics=dict(attempt.get("source_diagnostics") or {}),
                    )
                    return

            await asyncio.sleep(1.6)

    async def _capture_loop(
        self,
        timeout_seconds: int,
        seed_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """后台采集循环。"""
        diagnostics: Dict[str, Any] = {"timeout_seconds": int(timeout_seconds)}
        if isinstance(seed_diagnostics, dict):
            diagnostics["entry_seed"] = seed_diagnostics
        await self._set_capture_state(
            stage="running",
            message="请在网页中完成天翼登录，系统将自动采集登录态...",
            reason="",
            next_action="",
            user_account="",
            diagnostics=diagnostics,
        )

        deadline = time.monotonic() + float(timeout_seconds)
        last_source_diagnostics: Dict[str, Any] = {}
        last_source_attempts: List[str] = []
        last_reason = ""
        host_history: List[str] = []
        main_landing_seen = False

        try:
            while time.monotonic() < deadline:
                attempt = await self._attempt_capture_sources_once()
                last_reason = str(attempt.get("reason", "") or "")
                last_source_attempts = list(attempt.get("source_attempts") or [])
                last_source_diagnostics = dict(attempt.get("source_diagnostics") or {})
                main_landing_seen = bool(main_landing_seen or attempt.get("main_landing_detected"))

                cdp_probe = dict((last_source_diagnostics.get("cdp") or {}))
                probe_hosts = self._extract_probe_hosts(cdp_probe)
                if probe_hosts:
                    host_history.append(probe_hosts[0])
                    if len(host_history) > CAPTURE_LOOP_WINDOW + 4:
                        host_history = host_history[-(CAPTURE_LOOP_WINDOW + 4):]

                if self._is_capture_redirect_loop(host_history):
                    reason = "redirect_loop_detected"
                    loop_diag = self._build_capture_diag_payload(
                        reason=reason,
                        source_diagnostics=last_source_diagnostics,
                        main_landing_detected=main_landing_seen,
                        host_history=host_history[-CAPTURE_LOOP_WINDOW:],
                    )
                    await self._set_capture_state(
                        stage="failed",
                        message="检测到 cloud.189.cn 与 h5.cloud.189.cn 持续互跳",
                        reason=reason,
                        next_action="manual_cookie",
                        user_account="",
                        diagnostics=loop_diag,
                        source_attempts=last_source_attempts,
                        success_source="",
                        source_diagnostics=last_source_diagnostics,
                    )
                    return

                if bool(attempt.get("success")):
                    resolved_cookie = str(attempt.get("cookie", "") or "")
                    resolved_account = str(attempt.get("account", "") or "")
                    success_source = str(attempt.get("success_source", "") or "")
                    if resolved_cookie and resolved_account:
                        self.store.set_login(resolved_cookie, resolved_account)
                        done_diag = self._build_capture_diag_payload(
                            reason="",
                            source_diagnostics=last_source_diagnostics,
                            main_landing_detected=main_landing_seen,
                            host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
                        )
                        await self._set_capture_state(
                            stage="completed",
                            message=f"登录成功：{resolved_account}",
                            reason="",
                            next_action="",
                            user_account=resolved_account,
                            diagnostics=done_diag,
                            source_attempts=last_source_attempts,
                            success_source=success_source,
                            source_diagnostics=last_source_diagnostics,
                        )
                        return

                elapsed = int(max(0.0, float(timeout_seconds) - max(0.0, deadline - time.monotonic())))
                remaining = int(max(0.0, deadline - time.monotonic()))
                running_diag = self._build_capture_diag_payload(
                    reason=last_reason,
                    source_diagnostics=last_source_diagnostics,
                    main_landing_detected=main_landing_seen,
                    host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
                    elapsed_seconds=elapsed,
                    remaining_seconds=remaining,
                )
                running_message = "等待登录完成并同步登录态..."
                if last_reason == "main_landing_verify_failed":
                    running_message = "检测到已到达主站，正在持续校验账号登录态..."
                await self._set_capture_state(
                    stage="running",
                    message=running_message,
                    reason=last_reason,
                    next_action="",
                    user_account="",
                    diagnostics=running_diag,
                    source_attempts=last_source_attempts,
                    success_source="",
                    source_diagnostics=last_source_diagnostics,
                )
                await asyncio.sleep(1.8)

            timeout_reason = "capture_timeout"
            timeout_message = "自动采集超时，请改用手动 Cookie"
            if main_landing_seen or last_reason == "main_landing_verify_failed":
                timeout_reason = "main_landing_verify_failed"
                timeout_message = "检测到已跳转主站，但账号校验未通过，请重试登录或改用手动 Cookie"
            timeout_diag = self._build_capture_diag_payload(
                reason=timeout_reason,
                source_diagnostics=last_source_diagnostics,
                main_landing_detected=main_landing_seen,
                host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
            )
            await self._set_capture_state(
                stage="failed",
                message=timeout_message,
                reason=timeout_reason,
                next_action="manual_cookie",
                user_account="",
                diagnostics=timeout_diag,
                source_attempts=last_source_attempts,
                success_source="",
                source_diagnostics=last_source_diagnostics,
            )
        except asyncio.CancelledError:
            stopped_diag = self._build_capture_diag_payload(
                reason="capture_stopped",
                source_diagnostics=last_source_diagnostics,
                main_landing_detected=main_landing_seen,
                host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
            )
            await self._set_capture_state(
                stage="stopped",
                message="采集已停止，可改用手动 Cookie",
                reason="capture_stopped",
                next_action="manual_cookie",
                user_account="",
                diagnostics=stopped_diag,
                source_attempts=last_source_attempts,
                success_source="",
                source_diagnostics=last_source_diagnostics,
            )
            raise
        except Exception as exc:
            error_diag = self._build_capture_diag_payload(
                reason="capture_exception",
                source_diagnostics=last_source_diagnostics,
                main_landing_detected=main_landing_seen,
                host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
            )
            error_diag["exception"] = str(exc)
            await self._set_capture_state(
                stage="failed",
                message=f"自动采集异常：{exc}",
                reason="capture_exception",
                next_action="manual_cookie",
                user_account="",
                diagnostics=error_diag,
                source_attempts=last_source_attempts,
                success_source="",
                source_diagnostics=last_source_diagnostics,
            )

    async def _collect_baidu_cookie_from_cdp(self) -> tuple[str, Dict[str, Any]]:
        """从 CEF DevTools 端点尝试提取百度网盘 cookie。"""
        diagnostics: Dict[str, Any] = {
            "source": "cdp",
            "candidate_ports": list(CDP_ENDPOINT_PORTS),
            "probe_results": [],
            "page_candidates": [],
            "ok": False,
            "reason": "",
        }
        pages: List[Dict[str, Any]] = []

        timeout = aiohttp.ClientTimeout(total=LOCAL_WEB_PROBE_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for port in CDP_ENDPOINT_PORTS:
                url = f"http://127.0.0.1:{port}/json"
                probe_item: Dict[str, Any] = {"port": port, "url": url, "ok": False}
                try:
                    async with session.get(url) as resp:
                        probe_item["status"] = int(resp.status)
                        text = await resp.text()
                        if 200 <= resp.status < 300:
                            payload = json.loads(text)
                            if isinstance(payload, list):
                                for item in payload:
                                    if isinstance(item, dict):
                                        item["_cdp_port"] = port
                                        pages.append(item)
                                probe_item["ok"] = True
                                probe_item["page_count"] = len(payload)
                            else:
                                probe_item["error"] = "cdp_json_not_list"
                        else:
                            probe_item["error"] = "http_status_not_ok"
                except Exception as exc:
                    probe_item["error"] = str(exc)
                diagnostics["probe_results"].append(probe_item)

        if not pages:
            probe_results = diagnostics.get("probe_results", [])
            if isinstance(probe_results, list) and probe_results and all(
                not bool(item.get("ok")) for item in probe_results if isinstance(item, dict)
            ):
                diagnostics["reason"] = "cdp_endpoints_unreachable"
            else:
                diagnostics["reason"] = "cdp_no_pages"
            return "", diagnostics

        preferred_pages: List[Dict[str, Any]] = []
        fallback_pages: List[Dict[str, Any]] = []
        all_hosts: List[str] = []
        for page in pages:
            page_url = str(page.get("url", "") or "")
            ws_url = str(page.get("webSocketDebuggerUrl", "") or "")
            if not ws_url:
                continue

            host = ""
            try:
                host = str(urlparse(page_url).hostname or "").lower()
            except Exception:
                host = ""
            if host and host not in all_hosts:
                all_hosts.append(host)

            entry = {
                "title": str(page.get("title", "") or ""),
                "url": page_url,
                "host": host,
                "ws_url": ws_url,
                "port": int(page.get("_cdp_port") or 0),
            }
            if host and any(key in host for key in BAIDU_HOST_KEYWORDS):
                preferred_pages.append(entry)
            else:
                fallback_pages.append(entry)

        diagnostics["page_candidates"] = preferred_pages[:6]
        diagnostics["all_hosts"] = all_hosts[:12]
        selected_pages = preferred_pages + fallback_pages[:3]

        for page in selected_pages:
            ws_url = str(page.get("ws_url", "") or "")
            try:
                cookies = await self._get_all_cookies_from_ws(ws_url)
                cookie_str = self._build_baidu_cookie_string(cookies)
                if cookie_str:
                    diagnostics["matched_page"] = {
                        "host": page.get("host", ""),
                        "url": page.get("url", ""),
                        "port": page.get("port", 0),
                    }
                    diagnostics["ok"] = True
                    diagnostics["reason"] = ""
                    return cookie_str, diagnostics
            except Exception as exc:
                diagnostics.setdefault("ws_errors", []).append(
                    {
                        "port": page.get("port", 0),
                        "url": page.get("url", ""),
                        "error": str(exc),
                    }
                )

        diagnostics["reason"] = "cdp_no_baidu_cookie"
        return "", diagnostics

    async def _collect_tianyi_cookie_from_cdp(self) -> tuple[str, Dict[str, Any]]:
        """从 CEF DevTools 端点尝试提取天翼 cookie。"""
        diagnostics: Dict[str, Any] = {
            "source": "cdp",
            "candidate_ports": list(CDP_ENDPOINT_PORTS),
            "probe_results": [],
            "page_candidates": [],
            "main_landing_detected": False,
            "ok": False,
            "reason": "",
        }
        pages: List[Dict[str, Any]] = []

        timeout = aiohttp.ClientTimeout(total=LOCAL_WEB_PROBE_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for port in CDP_ENDPOINT_PORTS:
                url = f"http://127.0.0.1:{port}/json"
                probe_item: Dict[str, Any] = {"port": port, "url": url, "ok": False}
                try:
                    async with session.get(url) as resp:
                        probe_item["status"] = int(resp.status)
                        text = await resp.text()
                        if 200 <= resp.status < 300:
                            payload = json.loads(text)
                            if isinstance(payload, list):
                                for item in payload:
                                    if isinstance(item, dict):
                                        item["_cdp_port"] = port
                                        pages.append(item)
                                probe_item["ok"] = True
                                probe_item["page_count"] = len(payload)
                            else:
                                probe_item["error"] = "cdp_json_not_list"
                        else:
                            probe_item["error"] = "http_status_not_ok"
                except Exception as exc:
                    probe_item["error"] = str(exc)
                diagnostics["probe_results"].append(probe_item)

        if not pages:
            probe_results = diagnostics.get("probe_results", [])
            if isinstance(probe_results, list) and probe_results and all(not bool(item.get("ok")) for item in probe_results if isinstance(item, dict)):
                diagnostics["reason"] = "cdp_endpoints_unreachable"
            else:
                diagnostics["reason"] = "cdp_no_pages"
            return "", diagnostics

        preferred_pages: List[Dict[str, Any]] = []
        fallback_pages: List[Dict[str, Any]] = []
        all_hosts: List[str] = []
        for page in pages:
            page_url = str(page.get("url", "") or "")
            ws_url = str(page.get("webSocketDebuggerUrl", "") or "")
            if not ws_url:
                continue

            host = ""
            try:
                host = str(urlparse(page_url).hostname or "").lower()
            except Exception:
                host = ""
            if host and host not in all_hosts:
                all_hosts.append(host)
            if page_url.startswith("https://cloud.189.cn/web/main/") or page_url.startswith("http://cloud.189.cn/web/main/"):
                diagnostics["main_landing_detected"] = True
            entry = {
                "title": str(page.get("title", "") or ""),
                "url": page_url,
                "host": host,
                "ws_url": ws_url,
                "port": int(page.get("_cdp_port") or 0),
            }
            if host and any(key in host for key in TIANYI_HOST_KEYWORDS):
                preferred_pages.append(entry)
            else:
                fallback_pages.append(entry)

        diagnostics["page_candidates"] = preferred_pages[:6]
        diagnostics["all_hosts"] = all_hosts[:12]
        selected_pages = preferred_pages + fallback_pages[:3]

        for page in selected_pages:
            ws_url = str(page.get("ws_url", "") or "")
            try:
                cookies = await self._get_all_cookies_from_ws(ws_url)
                cookie_str = self._build_tianyi_cookie_string(cookies)
                if cookie_str:
                    diagnostics["matched_page"] = {
                        "host": page.get("host", ""),
                        "url": page.get("url", ""),
                        "port": page.get("port", 0),
                    }
                    diagnostics["ok"] = True
                    diagnostics["reason"] = ""
                    return cookie_str, diagnostics
            except Exception as exc:
                diagnostics.setdefault("ws_errors", []).append(
                    {
                        "port": page.get("port", 0),
                        "url": page.get("url", ""),
                        "error": str(exc),
                    }
                )

        diagnostics["reason"] = "cdp_no_tianyi_cookie"
        return "", diagnostics

    def _cookie_db_candidate_paths(self) -> List[str]:
        """生成 CookieDB 候选路径列表。"""
        home_dir = str(Path.home())
        raw_paths = [
            os.path.join(home_dir, ".local", "share", "Steam", "config", "htmlcache", "Cookies"),
            os.path.join(home_dir, ".steam", "steam", "config", "htmlcache", "Cookies"),
            os.path.join(home_dir, ".steam", "root", "config", "htmlcache", "Cookies"),
            os.path.join(home_dir, ".steam", "steam", "config", "htmlcache", "Default", "Cookies"),
            os.path.join(home_dir, ".config", "chromium", "Default", "Cookies"),
            os.path.join(
                home_dir,
                ".var",
                "app",
                "com.valvesoftware.Steam",
                ".local",
                "share",
                "Steam",
                "config",
                "htmlcache",
                "Cookies",
            ),
        ]
        dedup: List[str] = []
        seen = set()
        for item in raw_paths:
            path = os.path.realpath(os.path.expanduser(item))
            if path in seen:
                continue
            seen.add(path)
            dedup.append(path)
        return dedup

    def _read_cookie_db_rows(self, db_path: str, host_like: str = "%189.cn%") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """从 CookieDB 快照读取指定域行。"""
        diagnostics: Dict[str, Any] = {
            "db_path": db_path,
            "host_like": host_like,
            "snapshot_path": "",
            "row_count": 0,
        }
        temp_path = ""
        conn: Optional[sqlite3.Connection] = None
        try:
            with tempfile.NamedTemporaryFile(prefix="freedeck_cookie_", suffix=".db", delete=False) as temp_file:
                temp_path = temp_file.name
            diagnostics["snapshot_path"] = temp_path

            shutil.copy2(db_path, temp_path)

            conn = sqlite3.connect(f"file:{temp_path}?mode=ro", uri=True)
            cursor = conn.execute("PRAGMA table_info(cookies)")
            columns = [str(row[1]) for row in cursor.fetchall()]
            diagnostics["columns"] = columns
            if "host_key" not in columns or "name" not in columns:
                diagnostics["reason"] = "cookie_db_schema_invalid"
                return [], diagnostics

            select_cols = ["host_key", "name"]
            if "value" in columns:
                select_cols.append("value")
            if "encrypted_value" in columns:
                select_cols.append("encrypted_value")
            order_col = "last_access_utc" if "last_access_utc" in columns else "rowid"

            sql = (
                f"SELECT {', '.join(select_cols)} "
                f"FROM cookies "
                f"WHERE host_key LIKE ? "
                f"ORDER BY {order_col} DESC "
                f"LIMIT {COOKIE_DB_MAX_ROWS}"
            )
            rows_raw = conn.execute(sql, (host_like,)).fetchall()
            rows: List[Dict[str, Any]] = []
            for raw in rows_raw:
                item: Dict[str, Any] = {}
                for idx, col_name in enumerate(select_cols):
                    item[col_name] = raw[idx]
                rows.append(item)
            diagnostics["row_count"] = len(rows)
            diagnostics["reason"] = ""
            return rows, diagnostics
        except Exception as exc:
            diagnostics["reason"] = "cookie_db_read_failed"
            diagnostics["error"] = str(exc)
            return [], diagnostics
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if temp_path:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    async def _collect_tianyi_cookie_from_cookie_db(self) -> tuple[str, Dict[str, Any]]:
        """从本地 CookieDB 尝试提取天翼 Cookie。"""
        candidate_paths = self._cookie_db_candidate_paths()
        diagnostics: Dict[str, Any] = {
            "source": "cookie_db",
            "candidate_paths": candidate_paths,
            "probe_results": [],
            "ok": False,
            "reason": "",
        }

        any_existing = False
        any_read_failed = False
        for path in candidate_paths:
            probe_item: Dict[str, Any] = {"path": path, "exists": False}
            if not os.path.isfile(path):
                diagnostics["probe_results"].append(probe_item)
                continue

            any_existing = True
            probe_item["exists"] = True
            rows, row_diag = await asyncio.to_thread(self._read_cookie_db_rows, path, "%189.cn%")
            probe_item.update(row_diag)
            if str(row_diag.get("reason", "")).strip():
                any_read_failed = True
                diagnostics["probe_results"].append(probe_item)
                continue

            kv_map: Dict[str, str] = {}
            encrypted_only_count = 0
            for row in rows:
                name = str(row.get("name", "") or "").strip()
                if not name:
                    continue

                raw_value = row.get("value", "")
                value = str(raw_value or "").strip()
                if not value:
                    encrypted_value = row.get("encrypted_value", b"")
                    if isinstance(encrypted_value, memoryview):
                        encrypted_value = encrypted_value.tobytes()
                    if isinstance(encrypted_value, (bytes, bytearray)) and encrypted_value:
                        blob = bytes(encrypted_value)
                        if blob.startswith(b"v10") or blob.startswith(b"v11"):
                            encrypted_only_count += 1
                            continue
                        try:
                            decoded = blob.decode("utf-8", errors="ignore").strip()
                        except Exception:
                            decoded = ""
                        if not decoded:
                            encrypted_only_count += 1
                            continue
                        value = decoded
                if not value:
                    continue
                if name not in kv_map:
                    kv_map[name] = value

            probe_item["cookie_name_count"] = len(kv_map)
            probe_item["encrypted_only_count"] = encrypted_only_count
            diagnostics["probe_results"].append(probe_item)

            if kv_map:
                diagnostics["ok"] = True
                diagnostics["reason"] = ""
                diagnostics["selected_path"] = path
                diagnostics["cookie_name_count"] = len(kv_map)
                ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda item: item[0].lower())]
                return "; ".join(ordered), diagnostics

        if not any_existing:
            diagnostics["reason"] = "cookie_db_not_found"
        elif any_read_failed:
            diagnostics["reason"] = "cookie_db_read_failed"
        else:
            diagnostics["reason"] = "cookie_db_no_tianyi_cookie"
        return "", diagnostics

    async def _collect_baidu_cookie_from_cookie_db(self) -> tuple[str, Dict[str, Any]]:
        """从本地 CookieDB 尝试提取百度网盘 Cookie（以 BDUSS 为登录标记）。"""
        candidate_paths = self._cookie_db_candidate_paths()
        diagnostics: Dict[str, Any] = {
            "source": "cookie_db",
            "candidate_paths": candidate_paths,
            "probe_results": [],
            "ok": False,
            "reason": "",
        }

        any_existing = False
        any_read_failed = False
        for path in candidate_paths:
            probe_item: Dict[str, Any] = {"path": path, "exists": False}
            if not os.path.isfile(path):
                diagnostics["probe_results"].append(probe_item)
                continue

            any_existing = True
            probe_item["exists"] = True
            rows, row_diag = await asyncio.to_thread(self._read_cookie_db_rows, path, "%baidu.com%")
            probe_item.update(row_diag)
            if str(row_diag.get("reason", "")).strip():
                any_read_failed = True
                diagnostics["probe_results"].append(probe_item)
                continue

            kv_map: Dict[str, str] = {}
            encrypted_only_count = 0
            for row in rows:
                name = str(row.get("name", "") or "").strip()
                if not name:
                    continue

                raw_value = row.get("value", "")
                value = str(raw_value or "").strip()
                if not value:
                    encrypted_value = row.get("encrypted_value", b"")
                    if isinstance(encrypted_value, memoryview):
                        encrypted_value = encrypted_value.tobytes()
                    if isinstance(encrypted_value, (bytes, bytearray)) and encrypted_value:
                        blob = bytes(encrypted_value)
                        if blob.startswith(b"v10") or blob.startswith(b"v11"):
                            encrypted_only_count += 1
                            continue
                        try:
                            decoded = blob.decode("utf-8", errors="ignore").strip()
                        except Exception:
                            decoded = ""
                        if not decoded:
                            encrypted_only_count += 1
                            continue
                        value = decoded
                if not value:
                    continue
                if name not in kv_map:
                    kv_map[name] = value

            probe_item["cookie_name_count"] = len(kv_map)
            probe_item["encrypted_only_count"] = encrypted_only_count
            probe_item["bduss_found"] = bool(kv_map.get("BDUSS"))
            diagnostics["probe_results"].append(probe_item)

            bduss = str(kv_map.get("BDUSS", "") or "").strip()
            if bduss:
                diagnostics["ok"] = True
                diagnostics["reason"] = ""
                diagnostics["selected_path"] = path
                diagnostics["cookie_name_count"] = len(kv_map)
                ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda item: item[0].lower())]
                return "; ".join(ordered), diagnostics

        if not any_existing:
            diagnostics["reason"] = "cookie_db_not_found"
        elif any_read_failed:
            diagnostics["reason"] = "cookie_db_read_failed"
        else:
            diagnostics["reason"] = "cookie_db_no_baidu_cookie"
        return "", diagnostics

    async def _get_all_cookies_from_ws(self, ws_url: str) -> List[Dict[str, Any]]:
        """通过 CDP WebSocket 获取所有 cookie。"""
        timeout = aiohttp.ClientTimeout(total=6.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, autoping=True, heartbeat=10.0) as ws:
                request_id = 1

                async def cdp_call(method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
                    nonlocal request_id
                    current_id = request_id
                    request_id += 1
                    await ws.send_json({"id": current_id, "method": method, "params": params or {}})

                    while True:
                        msg = await ws.receive(timeout=5.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(str(msg.data))
                            if int(payload.get("id", 0) or 0) != current_id:
                                continue
                            if "error" in payload:
                                raise RuntimeError(str(payload.get("error")))
                            return payload.get("result") or {}
                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                            raise RuntimeError("cdp websocket closed")
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError("cdp websocket error")

                await cdp_call("Network.enable")
                result = await cdp_call("Network.getAllCookies")
                cookies = result.get("cookies")
                if isinstance(cookies, list):
                    return [item for item in cookies if isinstance(item, dict)]
                return []

    def _build_tianyi_cookie_string(self, cookies: List[Dict[str, Any]]) -> str:
        """从 cookies 中筛出天翼域并组装 Cookie 头。"""
        kv_map: Dict[str, str] = {}
        for item in cookies:
            domain = str(item.get("domain", "") or "").lower()
            if "189.cn" not in domain:
                continue
            name = str(item.get("name", "") or "").strip()
            value = str(item.get("value", "") or "").strip()
            if not name or not value:
                continue
            kv_map[name] = value

        if not kv_map:
            return ""

        ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda x: x[0].lower())]
        return "; ".join(ordered)

    def _build_baidu_cookie_string(self, cookies: List[Dict[str, Any]]) -> str:
        """从 cookies 中筛出 baidu 域并组装 Cookie 头（必须包含 BDUSS 才认为已登录）。"""
        kv_map: Dict[str, str] = {}
        for item in cookies:
            domain = str(item.get("domain", "") or "").lower()
            if "baidu.com" not in domain:
                continue
            name = str(item.get("name", "") or "").strip()
            value = str(item.get("value", "") or "").strip()
            if not name or not value:
                continue
            kv_map[name] = value

        bduss = str(kv_map.get("BDUSS", "") or "").strip()
        if not bduss:
            return ""

        ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda x: x[0].lower())]
        return "; ".join(ordered)

    async def _ensure_local_web_ready(self, route_path: str) -> str:
        """确保本地网页服务与目标页面可访问。"""
        diagnostics: Dict[str, Any] = {
            "host_candidates": ["127.0.0.1"],
            "route": route_path,
            "probe_results": [],
        }

        status = await self.plugin.get_server_status()
        diagnostics["status_before"] = dict(status)
        if not bool(status.get("running")):
            start_result = await self.plugin.start_server(self.plugin.server_port)
            diagnostics["start_result"] = dict(start_result)
            if start_result.get("status") != "success":
                raise LocalWebNotReadyError(
                    str(start_result.get("message", "本地网页服务启动失败")),
                    reason="local_server_start_failed",
                    diagnostics=diagnostics,
                )
            status = await self.plugin.get_server_status()
            diagnostics["status_after"] = dict(status)

        if not bool(status.get("running")):
            raise LocalWebNotReadyError(
                "本地网页服务未就绪，请稍后再试",
                reason="local_server_not_running",
                diagnostics=diagnostics,
            )

        port = int(status.get("port") or self.plugin.server_port)
        diagnostics["port"] = port

        health_probe = await self._probe_local_route(port, "/_healthz")
        page_probe = await self._probe_local_route(port, route_path.split("?", 1)[0])
        diagnostics["probe_results"].append(health_probe)
        diagnostics["probe_results"].append(page_probe)

        if not health_probe.get("ok"):
            raise LocalWebNotReadyError(
                "本地网页基础探针未通过，请稍后再试",
                reason="health_probe_failed",
                diagnostics=diagnostics,
            )

        if not page_probe.get("ok"):
            raise LocalWebNotReadyError(
                "本地网页页面探针未通过，请稍后再试",
                reason="page_probe_failed",
                diagnostics=diagnostics,
            )

        return f"http://127.0.0.1:{port}{route_path}"

    async def _peek_local_web_url(self, route_path: str) -> str:
        """仅检查当前服务状态，不主动拉起服务。"""
        status = await self.plugin.get_server_status()
        if not bool(status.get("running")):
            return ""

        port = int(status.get("port") or self.plugin.server_port)
        probe = await self._probe_local_route(port, route_path.split("?", 1)[0])
        if not probe.get("ok"):
            return ""

        return f"http://127.0.0.1:{port}{route_path}"

    async def _probe_local_route(self, port: int, path: str) -> Dict[str, Any]:
        """探测本地路由是否可访问。"""
        url = f"http://127.0.0.1:{int(port)}{path}"
        result: Dict[str, Any] = {
            "url": url,
            "path": path,
            "ok": False,
        }

        timeout = aiohttp.ClientTimeout(total=LOCAL_WEB_READY_TIMEOUT_SECONDS)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, allow_redirects=False) as resp:
                    result["status"] = int(resp.status)
                    result["ok"] = 200 <= resp.status < 400
        except Exception as exc:
            result["error"] = str(exc)
        return result
