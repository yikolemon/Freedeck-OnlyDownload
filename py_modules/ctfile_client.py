# ctfile_client.py - 城通网盘（CTFile）分享解析与直链获取
#
# Phase 1: 仅供 Freedeck “自定义源导入”使用（分享解析 + 直链下载）。

from __future__ import annotations

import asyncio
import json
import html
import os
import random
import re
import ssl
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import aiohttp

from tianyi_client import ResolvedFile, ResolvedShare


class CtfileApiError(RuntimeError):
    """CTFile 相关异常。"""

    def __init__(self, message: str, *, diagnostics: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.diagnostics = diagnostics or {}


CTFILE_CLIENT_REV = "2026-03-06.6"

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
_DEFAULT_REF = "https://www.ctfile.com"
_API_BASE = "https://webapi.ctfile.com"

_CA_CANDIDATE_FILES = (
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/cert.pem",
    "/etc/openssl/certs/ca-certificates.crt",
)

_URL_TRAIL_STRIP_RE = re.compile(r"[)\]】>＞,，。;；!！?？]+$")
_CTFILE_FILEID_RE = re.compile(r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+){1,4}$")
_CTFILE_DIR_ROW_ID_RE = re.compile(r'(?i)\bvalue="(?P<id>[df]\d{3,})"')
_CTFILE_DIR_ROW_NAME_RE = re.compile(r"(?is)<a\b[^>]*>(?P<name>.*?)</a>")
_CTFILE_DIR_ROW_HREF_RE = re.compile(r'(?i)\bhref="(?P<href>[^"]+)"')
# 兼容 load_subdir(123,'fk') / load_subdir('123',"fk") 等多种格式
_CTFILE_DIR_ROW_SUBDIR_RE = re.compile(
    r"(?i)load_subdir\(\s*['\"]?(?P<id>\d+)['\"]?\s*,\s*(?P<q>['\"])(?P<fk>[^'\"]*)(?P=q)"
)

_HTML_TAG_RE = re.compile(r"(?is)<[^>]+>")


def _redact_url_token(raw: str) -> str:
    """脱敏 URL/文本中的 token/session_id 参数，避免泄漏。"""
    text = str(raw or "").strip()
    if not text:
        return ""

    # 先做一遍正则脱敏，兜底处理“非标准 URL”或“拼接文本”。
    text = re.sub(r"(?i)\b(token|session_id)=([A-Za-z0-9%._-]+)", r"\1=***", text)

    if "://" not in text:
        return text

    try:
        parsed = urlparse(text)
    except Exception:
        return text

    query_pairs = parse_qsl(parsed.query or "", keep_blank_values=True)
    if not query_pairs:
        return text

    next_pairs = []
    changed = False
    for key, value in query_pairs:
        key_text = str(key or "")
        if key_text.lower() in {"token", "session_id"}:
            next_pairs.append((key_text, "***"))
            changed = True
        else:
            next_pairs.append((key_text, str(value)))

    if not changed:
        return text

    try:
        return urlunparse(parsed._replace(query=urlencode(next_pairs, doseq=True)))
    except Exception:
        return text


def _clean_url_trailing(value: str) -> str:
    out = str(value or "").strip()
    while True:
        next_value = _URL_TRAIL_STRIP_RE.sub("", out).strip()
        if next_value == out:
            break
        out = next_value
    return out


def _extract_pwd_from_text(text: str) -> str:
    raw = str(text or "")
    for pattern in (
        r"(?:提取码|访问码|密码|口令)\s*[:：]?\s*([A-Za-z0-9]{4,16})",
        r"(?:提取码|访问码|密码|口令)\s+([A-Za-z0-9]{4,16})",
    ):
        m = re.search(pattern, raw, flags=re.IGNORECASE)
        if m:
            return str(m.group(1) or "").strip()
    return ""


def _extract_token_from_url(share_url: str) -> str:
    try:
        parsed = urlparse(str(share_url or "").strip())
        query = parse_qs(parsed.query or "")
        token_list = query.get("token") or query.get("session_id") or []
        return str(token_list[0] if token_list else "").strip()
    except Exception:
        return ""


def _extract_pwd_from_url(share_url: str) -> str:
    try:
        parsed = urlparse(str(share_url or "").strip())
        query = parse_qs(parsed.query or "")
        pwd_list = query.get("pwd") or query.get("p") or query.get("pass") or query.get("passcode") or []
        return str(pwd_list[0] if pwd_list else "").strip()
    except Exception:
        return ""


def _extract_fileid_from_url(share_url: str) -> str:
    url = str(share_url or "").strip()
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    path = str(parsed.path or "").strip()

    def extract_from_path(path_text: str) -> str:
        path_value = str(path_text or "").strip()
        if not path_value:
            return ""
        # /f/<id> 或 /file/<id>
        for prefix in ("/f/", "/file/"):
            if prefix in path_value:
                tail = path_value.split(prefix, 1)[1]
                tail = tail.split("/", 1)[0].strip()
                return tail
        # 兜底：取最后一段
        return path_value.strip("/").split("/")[-1].strip()

    fileid = extract_from_path(path)
    if fileid:
        return fileid

    # 兼容 hash 路由：https://www.ctfile.com/#/f/<id>
    frag = str(parsed.fragment or "").strip()
    if not frag:
        return ""
    frag_path = frag.split("?", 1)[0]
    if not frag_path.startswith("/"):
        frag_path = "/" + frag_path
    return extract_from_path(frag_path)


def _sanitize_share_url(raw: str) -> str:
    """从用户粘贴文本中提取可用的 CTFile 分享链接或 fileid。"""
    text = str(raw or "").strip()
    if not text:
        return ""

    if text.startswith("xtc") and "-" in text:
        # 小通口令：xtc<fileid>-<pass>
        try:
            file_part = text[3 : text.rfind("-")].strip()
        except Exception:
            file_part = ""
        if file_part:
            return file_part

    # 优先提取 http(s):// 链接
    for match in re.finditer(r"https?://[^\s]+", text, flags=re.IGNORECASE):
        candidate = _clean_url_trailing(match.group(0))
        try:
            host = str(urlparse(candidate).hostname or "").lower()
        except Exception:
            host = ""
        if host.endswith("ctfile.com") or host == "ctfile.com":
            return candidate

    # 其次提取无 scheme 的域名
    m = re.search(r"(?i)(?:www\.)?ctfile\.com/[^\s]+", text)
    if m:
        return _clean_url_trailing("https://" + m.group(0))

    # 允许直接粘贴 fileid（例如 8067059-687855402-65ca36）
    if _CTFILE_FILEID_RE.match(text):
        return text

    return ""


def _ctfile_path_type(fileid: str) -> str:
    parts = [p for p in str(fileid or "").split("-") if p]
    return "file" if len(parts) == 2 else "f"


_SIZE_RE = re.compile(r"(?i)^\s*(\d+(?:\.\d+)?)\s*([KMGT]?B)\s*$")


def _parse_size_to_bytes(size_text: str) -> int:
    text = str(size_text or "").strip()
    if not text:
        return 0
    m = _SIZE_RE.match(text.replace(" ", ""))
    if not m:
        # 兼容 "844.13 MB"（中间有空格）
        m = _SIZE_RE.match(text)
    if not m:
        return 0
    try:
        value = float(m.group(1))
    except Exception:
        return 0
    unit = str(m.group(2) or "").upper()
    mul = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
    }.get(unit, 1)
    return max(0, int(value * mul))


def _build_tls_context() -> ssl.SSLContext:
    """构建统一 TLS 上下文，兼容 Decky Python 的证书链差异。"""
    candidates: List[str] = []
    env_cert_file = str(os.environ.get("SSL_CERT_FILE", "") or "").strip()
    if env_cert_file:
        candidates.append(env_cert_file)
    candidates.extend(list(_CA_CANDIDATE_FILES))

    try:
        import certifi  # type: ignore

        certifi_path = str(certifi.where() or "").strip()
        if certifi_path:
            candidates.append(certifi_path)
    except Exception:
        pass

    dedup: List[str] = []
    seen = set()
    for raw in candidates:
        path = os.path.realpath(os.path.expanduser(str(raw).strip()))
        if not path or path in seen:
            continue
        seen.add(path)
        dedup.append(path)

    for path in dedup:
        if not os.path.isfile(path):
            continue
        try:
            return ssl.create_default_context(cafile=path)
        except Exception:
            continue

    context = ssl.create_default_context()

    # 仅用于紧急排障，默认保持证书校验开启。
    insecure_flag = str(os.environ.get("FREEDECK_CTFILE_INSECURE_TLS", "") or "").strip().lower()
    if insecure_flag in {"1", "true", "yes"}:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _ctfile_first_path_segment(url: str) -> str:
    try:
        parsed = urlparse(str(url or "").strip())
    except Exception:
        return ""
    path = str(parsed.path or "").strip()
    if not path:
        return ""
    first = path.lstrip("/").split("/", 1)[0].strip()
    return str(first or "").lower()


def _ctfile_share_kind(url: str) -> str:
    """判断分享链接类型：file/dir。"""
    seg = _ctfile_first_path_segment(url)
    # 站点脚本里：/f/ => file，/d/ => dir（允许 d1/d2… 等变体）
    if seg and seg.startswith("d"):
        return "dir"

    # 兼容 hash 路由：https://www.ctfile.com/#/d/<dirid>
    try:
        parsed = urlparse(str(url or "").strip())
        frag = str(parsed.fragment or "").strip()
        frag_path = frag.split("?", 1)[0]
        frag_seg = frag_path.lstrip("/").split("/", 1)[0].strip().lower()
        if frag_seg.startswith("d"):
            return "dir"
    except Exception:
        pass

    return "file"


def _ctfile_canonical_base(url: str) -> str:
    """尽量保留原始分享域名（例如 url01.ctfile.com），避免部分接口对 url 参数敏感。"""
    text = str(url or "").strip()
    if "://" not in text:
        return _DEFAULT_REF
    try:
        parsed = urlparse(text)
    except Exception:
        return _DEFAULT_REF
    scheme = str(parsed.scheme or "").strip()
    netloc = str(parsed.netloc or "").strip()
    if scheme and netloc:
        return f"{scheme}://{netloc}"
    return _DEFAULT_REF


def _extract_ctfile_dirid_from_url(share_url: str) -> str:
    """从目录分享 URL 中提取 dirid。

    兼容：
    - https://www.ctfile.com/d/<dirid>
    - https://www.ctfile.com/d1/<dirid>/...（后续路径忽略）
    - https://www.ctfile.com/#/d/<dirid>?...
    """
    url = str(share_url or "").strip()
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""

    def extract_from_path(path_text: str) -> str:
        path = str(path_text or "").strip()
        if not path:
            return ""
        segs = [s for s in path.strip("/").split("/") if s]
        if len(segs) < 2:
            return ""
        if str(segs[0] or "").lower().startswith("d"):
            return str(segs[1] or "").strip()
        return ""

    dirid = extract_from_path(parsed.path)
    if dirid:
        return dirid

    frag = str(parsed.fragment or "").strip()
    if not frag:
        return ""
    frag_path = frag.split("?", 1)[0]
    return extract_from_path(frag_path)


def _extract_ctfile_dir_start_params(share_url: str, *, dirid: str) -> Tuple[str, str]:
    """从目录分享 URL 中提取“起始子目录参数”（folder_id/fk）。"""
    url = str(share_url or "").strip()
    if not url:
        return "", ""
    try:
        parsed = urlparse(url)
    except Exception:
        return "", ""

    def normalize_query(qs: Dict[str, List[str]]) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for key, values in (qs or {}).items():
            k = str(key or "").strip().lower()
            if not k:
                continue
            out[k] = [str(v or "").strip() for v in (values or []) if str(v or "").strip()]
        return out

    query = normalize_query(parse_qs(parsed.query or ""))

    frag = str(parsed.fragment or "").strip()
    if frag and "?" in frag:
        frag_query = frag.split("?", 1)[1]
        frag_q = normalize_query(parse_qs(frag_query))
        # fragment query 优先级更高（用户通常复制的是当前目录状态）
        query = {**query, **frag_q}

    def pick_first(keys: Tuple[str, ...]) -> str:
        for key in keys:
            values = query.get(key) or []
            if values:
                return str(values[0] or "").strip()
        return ""

    start_fk = pick_first(("fk", "folder_key", "folderkey", "key", "k"))
    start_folder_id = pick_first(("folder_id", "folderid", "folder", "dir", "subdir", "sub_dir"))

    # 兼容部分页面把“当前目录 id”塞在 d=...（注意避免与 dirid 冲突）
    if not start_folder_id:
        d_val = pick_first(("d",))
        if d_val and d_val.isdigit() and start_fk and d_val != str(dirid or "").strip():
            start_folder_id = d_val

    # 没有 fk 时通常无法进入子目录，避免误用导致“空文件列表”
    if start_folder_id and not start_fk:
        return "", ""

    if not start_folder_id:
        return "", ""

    return start_folder_id, start_fk


def _ctfile_getfile_path_hint(url: str, file_code: str) -> str:
    """推断 getfile.php 的 path 参数（f/file）。"""
    seg = _ctfile_first_path_segment(url)
    if seg in {"f", "file"}:
        return seg
    # 目录列表里的文件通常以 #/f/tempdir-... 形式出现，避免被误判为 path=file
    lower = str(file_code or "").strip().lower()
    if lower.startswith("tempdir-") or lower.startswith("tempfile-") or lower.startswith("temp-"):
        return "f"
    return _ctfile_path_type(file_code)


async def _ctfile_get_json(
    session: aiohttp.ClientSession,
    *,
    url: str,
    params: Dict[str, Any],
    stage: str,
    diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        async with session.get(url, params=params, allow_redirects=True) as resp:
            status = int(resp.status)
            text = await resp.text()
    except aiohttp.ClientConnectorCertificateError as exc:
        raise CtfileApiError(
            f"CTFile TLS 证书校验失败：{exc}",
            diagnostics={**diagnostics, "stage": stage},
        ) from exc
    except aiohttp.ClientError as exc:
        raise CtfileApiError(
            f"访问 CTFile 失败：{exc}",
            diagnostics={**diagnostics, "stage": stage},
        ) from exc
    except Exception as exc:
        raise CtfileApiError(
            f"访问 CTFile 失败：{exc}",
            diagnostics={**diagnostics, "stage": stage},
        ) from exc

    if status >= 400:
        raise CtfileApiError(
            f"CTFile 请求失败：HTTP {status}",
            diagnostics={**diagnostics, "stage": stage, "status": status, "body_head": (text or "")[:200]},
        )
    if not text:
        raise CtfileApiError(
            f"CTFile 返回空响应（{stage}）",
            diagnostics={**diagnostics, "stage": stage, "status": status},
        )
    try:
        payload = json.loads(text)
    except Exception as exc:
        raise CtfileApiError(
            f"CTFile 响应解析失败（{stage}）",
            diagnostics={**diagnostics, "stage": stage, "status": status, "body_head": (text or "")[:200]},
        ) from exc
    if not isinstance(payload, dict):
        raise CtfileApiError(
            f"CTFile 响应结构异常（{stage}）",
            diagnostics={**diagnostics, "stage": stage, "status": status},
        )
    return payload


def _extract_text_first(pattern: re.Pattern[str], text: str, *, group: str) -> str:
    m = pattern.search(text or "")
    if not m:
        return ""
    try:
        return str(m.group(group) or "").strip()
    except Exception:
        return ""


def _parse_ctfile_dir_row(row: Any) -> Dict[str, Any]:
    """解析 file_list 的 DataTables 行。返回 dict（type=file|dir|unknown）。"""
    if not isinstance(row, list) or len(row) < 2:
        return {"type": "unknown"}
    checkbox_html = str(row[0] or "")
    name_html_raw = str(row[1] or "")
    # 部分返回会把引号转义为实体（&#039;），需要先 unescape 才能匹配 load_subdir(...)
    name_html = html.unescape(name_html_raw)
    size_text = str(row[2] or "").strip() if len(row) > 2 else ""
    date_text = str(row[3] or "").strip() if len(row) > 3 else ""

    item_id = _extract_text_first(_CTFILE_DIR_ROW_ID_RE, checkbox_html, group="id")
    is_dir = bool("folder.svg" in name_html.lower() or item_id.lower().startswith("d"))

    name = _extract_text_first(_CTFILE_DIR_ROW_NAME_RE, name_html, group="name")
    if name:
        name = _HTML_TAG_RE.sub("", name)
    name = html.unescape(name)
    name = re.sub(r"\s+", " ", str(name or "")).strip()

    href = _extract_text_first(_CTFILE_DIR_ROW_HREF_RE, name_html, group="href")
    href = html.unescape(href)

    if is_dir:
        subdir_id = _extract_text_first(_CTFILE_DIR_ROW_SUBDIR_RE, name_html, group="id")
        fk = _extract_text_first(_CTFILE_DIR_ROW_SUBDIR_RE, name_html, group="fk")
        if not subdir_id or not fk:
            # 部分页面会把 load_subdir(...) 放在 href 里，或被拆分到其它字段
            combined = " ".join(
                part
                for part in (
                    name_html,
                    href,
                    checkbox_html,
                )
                if str(part or "").strip()
            )
            if combined:
                subdir_id = subdir_id or _extract_text_first(_CTFILE_DIR_ROW_SUBDIR_RE, combined, group="id")
                fk = fk or _extract_text_first(_CTFILE_DIR_ROW_SUBDIR_RE, combined, group="fk")
        return {
            "type": "dir",
            "id": item_id,
            "name": name,
            "size": size_text,
            "date": date_text,
            "subdir_id": subdir_id,
            "fk": fk,
        }

    # file: 优先从 hash 路由中取出子文件 share code（例如 tempdir-...）
    share_code = ""
    if href:
        if href.startswith("#/"):
            share_code = href[2:]
        elif href.startswith("#"):
            share_code = href[1:]
        else:
            share_code = href
    if share_code.startswith("/"):
        share_code = share_code[1:]
    if share_code.lower().startswith(("f/", "file/")):
        share_code = share_code.split("/", 1)[1]
    if "?" in share_code:
        share_code = share_code.split("?", 1)[0]
    share_code = str(share_code or "").strip()

    return {
        "type": "file",
        "id": item_id,
        "name": name,
        "size": size_text,
        "date": date_text,
        "share_code": share_code,
    }


async def _fetch_ctfile_dir_file_list(
    session: aiohttp.ClientSession,
    *,
    list_path: str,
    original_redacted: str,
    page_size: int,
    max_rows: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """通过 getdir.php 返回的 file.url 拉取 DataTables 列表并解析为行。"""
    url = str(list_path or "").strip()
    if not url:
        raise CtfileApiError(
            "CTFile 目录列表缺少 url",
            diagnostics={"stage": "ctfile.file_list", "share_url": original_redacted},
        )
    base_url = _API_BASE.rstrip("/") + "/" + url.lstrip("/")
    start = 0
    rows: List[Dict[str, Any]] = []
    diagnostics: Dict[str, Any] = {"stage": "ctfile.file_list", "share_url": original_redacted, "pages": 0, "truncated": False}
    total_hint: Optional[int] = None

    while True:
        query = {"sEcho": 1, "iDisplayStart": start, "iDisplayLength": page_size}
        joiner = "&" if "?" in base_url else "?"
        page_url = base_url + joiner + urlencode(query)
        try:
            async with session.get(page_url, allow_redirects=True) as resp:
                status = int(resp.status)
                text = await resp.text()
        except Exception as exc:
            raise CtfileApiError(
                f"CTFile 拉取目录列表失败：{exc}",
                diagnostics={"stage": "ctfile.file_list", "share_url": original_redacted},
            ) from exc

        if status >= 400:
            raise CtfileApiError(
                f"CTFile 目录列表请求失败：HTTP {status}",
                diagnostics={"stage": "ctfile.file_list", "status": status, "share_url": original_redacted, "body_head": (text or "")[:200]},
            )
        if not text:
            break

        try:
            payload = json.loads(text)
        except Exception:
            raise CtfileApiError(
                "CTFile 目录列表响应解析失败",
                diagnostics={"stage": "ctfile.file_list", "status": status, "share_url": original_redacted, "body_head": (text or "")[:200]},
            )

        if not isinstance(payload, dict):
            break

        if total_hint is None:
            try:
                total_hint = max(0, int(payload.get("iTotalRecords") or 0))
            except Exception:
                total_hint = 0

        data = payload.get("aaData")
        if not isinstance(data, list) or not data:
            break

        for raw_row in data:
            parsed = _parse_ctfile_dir_row(raw_row)
            if parsed.get("type") == "unknown":
                continue
            rows.append(parsed)
            if len(rows) >= max_rows:
                diagnostics["truncated"] = True
                break
        diagnostics["pages"] = int(diagnostics.get("pages", 0) or 0) + 1
        if diagnostics["truncated"]:
            break

        if total_hint is not None and total_hint > 0 and len(rows) >= total_hint:
            break

        if len(data) < page_size:
            break
        start += page_size
        if start > (total_hint or 0) + page_size:
            break

    diagnostics["total_hint"] = total_hint or 0
    diagnostics["rows"] = len(rows)
    return rows, diagnostics


async def _resolve_ctfile_dir_share(
    *,
    session: aiohttp.ClientSession,
    dirid: str,
    pwd: str,
    token: str,
    path_hint: str,
    canonical_url: str,
    original_redacted: str,
    start_folder_id: str,
    start_fk: str,
    max_files: int,
    max_dirs: int,
) -> Tuple[List[ResolvedFile], Dict[str, Any]]:
    """解析 CTFile 目录分享，返回递归展开后的文件列表与诊断信息。"""
    ref = _DEFAULT_REF
    diagnostics: Dict[str, Any] = {
        "stage": "ctfile.getdir",
        "share_url": original_redacted,
        "canonical_url": canonical_url,
        "dirid": dirid,
        "max_files": max_files,
        "max_dirs": max_dirs,
        "truncated": False,
        "visited_dirs": 0,
        "dir_rows_total": 0,
        "dir_rows_skipped": 0,
        "file_rows_seen": 0,
    }

    async def call_getdir(folder_id: str, fk: str) -> Dict[str, Any]:
        params = {
            "path": path_hint,
            "d": dirid,
            "folder_id": str(folder_id or ""),
            "fk": str(fk or ""),
            "passcode": pwd,
            "token": str(token or "").strip(),
            "r": str(random.random()),
            "ref": ref,
            "url": canonical_url,
        }
        payload = await _ctfile_get_json(
            session,
            url=_API_BASE + "/getdir.php",
            params=params,
            stage="ctfile.getdir",
            diagnostics={"share_url": original_redacted, "canonical_url": canonical_url},
        )
        code = int(payload.get("code") or 0)
        if code == 423:
            raise CtfileApiError(
                "CTFile 目录需要访问码/密码",
                diagnostics={"stage": "ctfile.getdir", "share_url": original_redacted, "code": code},
            )
        if code != 200:
            message = ""
            try:
                message = str((payload.get("file") or {}).get("message") or payload.get("message") or "").strip()
            except Exception:
                message = ""
            raise CtfileApiError(
                message or f"CTFile 目录解析失败：code={code}",
                diagnostics={"stage": "ctfile.getdir", "share_url": original_redacted, "code": code},
            )
        return payload

    # 1) 先拉一次目录入口（可能返回 folder_id=0 的“根文件夹指针”列表）
    payload = await call_getdir(start_folder_id, start_fk)
    file_obj = payload.get("file") if isinstance(payload, dict) else {}
    if not isinstance(file_obj, dict):
        file_obj = {}

    root_folder_id = str(file_obj.get("folder_id", "") or "").strip()
    root_folder_name = str(file_obj.get("folder_name", "") or "").strip()
    list_url = str(file_obj.get("url", "") or "").strip()

    if not list_url:
        raise CtfileApiError(
            "CTFile 目录解析缺少列表 URL",
            diagnostics={"stage": "ctfile.getdir", "share_url": original_redacted},
        )

    rows, list_diag = await _fetch_ctfile_dir_file_list(
        session,
        list_path=list_url,
        original_redacted=original_redacted,
        page_size=200,
        max_rows=max(50, max_files * 2),
    )
    diagnostics["file_list"] = list_diag

    # 2) 处理“根文件夹指针”：只有 1 个目录项且指向自身时，自动进入该目录
    if not start_folder_id and root_folder_id:
        root_only = [r for r in rows if r.get("type") == "dir"]
        files_only = [r for r in rows if r.get("type") == "file"]
        if len(root_only) == 1 and not files_only:
            subdir_id = str(root_only[0].get("subdir_id") or "").strip()
            fk = str(root_only[0].get("fk") or "").strip()
            if subdir_id and subdir_id == root_folder_id and fk:
                payload = await call_getdir(subdir_id, fk)
                file_obj = payload.get("file") if isinstance(payload, dict) else {}
                if not isinstance(file_obj, dict):
                    file_obj = {}
                root_folder_name = str(file_obj.get("folder_name", "") or root_folder_name).strip()
                list_url = str(file_obj.get("url", "") or "").strip()
                rows, list_diag = await _fetch_ctfile_dir_file_list(
                    session,
                    list_path=list_url,
                    original_redacted=original_redacted,
                    page_size=200,
                    max_rows=max(50, max_files * 2),
                )
                diagnostics["file_list"] = list_diag
                start_folder_id = subdir_id
                start_fk = fk

    # 3) 递归遍历子目录，扁平化文件列表（用路径前缀区分）
    resolved_files: List[ResolvedFile] = []
    visited_dirs: set[Tuple[str, str]] = set()
    queue: List[Tuple[str, str, str]] = []  # (folder_id, fk, prefix)

    root_prefix = root_folder_name.strip()
    if start_folder_id:
        queue.append((start_folder_id, start_fk, root_prefix))
    else:
        queue.append(("", "", root_prefix))

    while queue:
        folder_id, fk, prefix = queue.pop(0)
        if len(visited_dirs) >= max_dirs:
            diagnostics["truncated"] = True
            break
        key = (str(folder_id or ""), str(fk or ""))
        if key in visited_dirs:
            continue
        visited_dirs.add(key)
        diagnostics["visited_dirs"] = len(visited_dirs)

        payload = await call_getdir(folder_id, fk)
        file_obj = payload.get("file") if isinstance(payload, dict) else {}
        if not isinstance(file_obj, dict):
            file_obj = {}
        list_url = str(file_obj.get("url", "") or "").strip()
        if not list_url:
            continue
        rows, _ = await _fetch_ctfile_dir_file_list(
            session,
            list_path=list_url,
            original_redacted=original_redacted,
            page_size=200,
            max_rows=max(50, max_files * 2),
        )

        for row in rows:
            row_type = str(row.get("type") or "")
            if row_type == "dir":
                diagnostics["dir_rows_total"] = int(diagnostics.get("dir_rows_total", 0) or 0) + 1
                sub_id = str(row.get("subdir_id") or "").strip()
                sub_fk = str(row.get("fk") or "").strip()
                sub_name = str(row.get("name") or "").strip()
                if not sub_id or not sub_fk or not sub_name:
                    diagnostics["dir_rows_skipped"] = int(diagnostics.get("dir_rows_skipped", 0) or 0) + 1
                    continue
                next_prefix = "/".join([p for p in [prefix.strip("/"), sub_name.strip("/")] if p])
                queue.append((sub_id, sub_fk, next_prefix))
                continue
            if row_type != "file":
                continue

            share_code = str(row.get("share_code") or "").strip()
            display_name = str(row.get("name") or "").strip()
            if not share_code or not display_name:
                continue
            diagnostics["file_rows_seen"] = int(diagnostics.get("file_rows_seen", 0) or 0) + 1
            full_name = "/".join([p for p in [prefix.strip("/"), display_name.strip("/")] if p])
            size_bytes = _parse_size_to_bytes(str(row.get("size") or ""))
            resolved_files.append(
                ResolvedFile(
                    file_id=share_code,
                    name=full_name or display_name,
                    size=size_bytes,
                    is_folder=False,
                )
            )
            if len(resolved_files) >= max_files:
                diagnostics["truncated"] = True
                break

        if diagnostics.get("truncated"):
            break

    diagnostics["file_count"] = len(resolved_files)
    return resolved_files, diagnostics


async def resolve_ctfile_share(share_url: str, token: str = "") -> Tuple[ResolvedShare, Dict[str, Any]]:
    """解析 CTFile 分享并返回单文件列表与上下文。"""
    original = str(share_url or "").strip()
    original_redacted = _redact_url_token(original)
    sanitized = _sanitize_share_url(original)
    if not sanitized:
        raise CtfileApiError("无效的 CTFile 分享链接/ID", diagnostics={"share_url": original_redacted})

    pwd = _extract_pwd_from_url(sanitized) or _extract_pwd_from_text(original)
    token_effective = str(token or "").strip() or _extract_token_from_url(sanitized)
    timeout = aiohttp.ClientTimeout(total=35)
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": _DEFAULT_REF,
        "Referer": _DEFAULT_REF,
    }
    connector = aiohttp.TCPConnector(ssl=_build_tls_context())

    # 目录分享（/d/…）
    if "://" in sanitized and _ctfile_share_kind(sanitized) == "dir":
        dirid = str(_extract_ctfile_dirid_from_url(sanitized) or "").strip()
        if not dirid:
            raise CtfileApiError("未解析到目录分享 ID", diagnostics={"share_url": original_redacted})
        start_folder_id, start_fk = _extract_ctfile_dir_start_params(sanitized, dirid=dirid)
        path_hint = _ctfile_first_path_segment(sanitized) or "d"
        canonical_url = f"{_ctfile_canonical_base(sanitized).rstrip('/')}/d/{dirid}"

        async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
            files, dir_diag = await _resolve_ctfile_dir_share(
                session=session,
                dirid=dirid,
                pwd=pwd,
                token=token_effective,
                path_hint=path_hint,
                canonical_url=canonical_url,
                original_redacted=original_redacted,
                start_folder_id=start_folder_id,
                start_fk=start_fk,
                max_files=2000,
                max_dirs=200,
            )

        if not files:
            raise CtfileApiError(
                "CTFile 目录中未发现可下载文件（可能是子目录参数不兼容或目录仅包含子文件夹）",
                diagnostics={"share_url": original_redacted, "canonical_url": canonical_url, "dir_diag": dir_diag},
            )

        resolved = ResolvedShare(
            share_code=dirid,
            share_id=dirid,
            pwd=pwd,
            files=files,
        )
        ctx = {
            "provider": "ctfile",
            "client_rev": CTFILE_CLIENT_REV,
            "share_url": canonical_url,
            "canonical_url": canonical_url,
            "input_url": original_redacted,
            # 注意：保持 key 名为 fileid 以复用现有 compact ctx 逻辑（其含义在目录场景为 dirid）。
            "fileid": dirid,
            "pwd": pwd,
            "path": "d",
            "dir_diag": dir_diag,
        }
        return resolved, ctx

    # 单文件分享（/f/… 或 /file/… 或直接 fileid）
    fileid = _extract_fileid_from_url(sanitized) if "://" in sanitized else sanitized
    fileid = str(fileid or "").strip()
    if not fileid:
        raise CtfileApiError("未解析到 fileid", diagnostics={"share_url": original_redacted})

    base = _ctfile_canonical_base(sanitized) if "://" in sanitized else _DEFAULT_REF
    canonical_url = f"{base.rstrip('/')}/{_ctfile_path_type(fileid)}/{fileid}"
    ref = _DEFAULT_REF

    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
        path_hint = _ctfile_getfile_path_hint(sanitized, fileid) if "://" in sanitized else _ctfile_path_type(fileid)
        params = {
            "path": path_hint,
            "f": fileid,
            "passcode": pwd,
            "token": token_effective,
            "r": str(random.random()),
            "ref": ref,
            "url": canonical_url,
        }
        payload = await _ctfile_get_json(
            session,
            url=_API_BASE + "/getfile.php",
            params=params,
            stage="ctfile.getfile",
            diagnostics={"share_url": original_redacted},
        )

        code = int(payload.get("code") or 0)
        if code != 200:
            message = ""
            try:
                message = str((payload.get("file") or {}).get("message") or payload.get("message") or "").strip()
            except Exception:
                message = ""
            raise CtfileApiError(
                message or f"CTFile 解析失败：code={code}",
                diagnostics={"stage": "ctfile.getfile", "code": code, "share_url": original_redacted},
            )

        file_obj = payload.get("file") if isinstance(payload, dict) else {}
        if not isinstance(file_obj, dict):
            file_obj = {}

        name = str(file_obj.get("file_name", "") or "").strip() or f"ctfile-{fileid}"
        size_bytes = _parse_size_to_bytes(str(file_obj.get("file_size", "") or ""))

        resolved = ResolvedShare(
            share_code=fileid,
            share_id=fileid,
            pwd=pwd,
            files=[
                ResolvedFile(
                    file_id=fileid,
                    name=name,
                    size=size_bytes,
                    is_folder=False,
                )
            ],
        )
        ctx = {
            "provider": "ctfile",
            "client_rev": CTFILE_CLIENT_REV,
            "share_url": canonical_url,
            "canonical_url": canonical_url,
            "input_url": original_redacted,
            "fileid": fileid,
            "pwd": pwd,
        }
        return resolved, ctx


async def resolve_ctfile_file_infos(
    file_ids: List[str],
    *,
    pwd: str,
    token: str = "",
    ref_url: str = "",
    max_files: int = 2000,
) -> Tuple[List[ResolvedFile], Dict[str, Any]]:
    """批量获取 CTFile 文件信息（name/size），用于目录分享“临时 file_id”漂移时的兜底。

    注意：该函数只调用 getfile.php，不会获取直链。
    """
    pwd_effective = str(pwd or "").strip()
    if not pwd_effective:
        raise CtfileApiError("CTFile 缺少访问码/密码", diagnostics={"stage": "ctfile.batch_getfile"})

    requested_raw = [str(x or "").strip() for x in (file_ids or []) if str(x or "").strip()]
    if not requested_raw:
        return [], {"stage": "ctfile.batch_getfile", "requested": 0, "resolved": 0, "failed": 0}

    requested: List[str] = []
    seen: set[str] = set()
    for item in requested_raw:
        if item in seen:
            continue
        seen.add(item)
        requested.append(item)
        if len(requested) >= max_files:
            break

    timeout = aiohttp.ClientTimeout(total=35)
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": _DEFAULT_REF,
        "Referer": _DEFAULT_REF,
    }
    connector = aiohttp.TCPConnector(ssl=_build_tls_context())
    token_effective = str(token or "").strip()
    url_param = str(ref_url or "").strip() or _DEFAULT_REF

    sem = asyncio.Semaphore(6)
    results: List[Optional[ResolvedFile]] = [None] * len(requested)
    errors: Dict[str, str] = {}

    async def fetch_one(index: int, file_code: str) -> None:
        async with sem:
            try:
                params = {
                    "path": _ctfile_getfile_path_hint(url_param, file_code),
                    "f": file_code,
                    "passcode": pwd_effective,
                    "token": token_effective,
                    "r": str(random.random()),
                    "ref": _DEFAULT_REF,
                    "url": url_param,
                }
                payload = await _ctfile_get_json(
                    session,
                    url=_API_BASE + "/getfile.php",
                    params=params,
                    stage="ctfile.getfile",
                    diagnostics={"share_url": _redact_url_token(url_param)},
                )
                code = int(payload.get("code") or 0)
                if code != 200:
                    message = ""
                    try:
                        message = str((payload.get("file") or {}).get("message") or payload.get("message") or "").strip()
                    except Exception:
                        message = ""
                    errors[file_code] = message or f"code={code}"
                    return

                file_obj = payload.get("file") if isinstance(payload, dict) else {}
                if not isinstance(file_obj, dict):
                    file_obj = {}
                name = str(file_obj.get("file_name", "") or "").strip() or f"ctfile-{file_code}"
                size_bytes = _parse_size_to_bytes(str(file_obj.get("file_size", "") or ""))
                results[index] = ResolvedFile(file_id=file_code, name=name, size=size_bytes, is_folder=False)
            except CtfileApiError as exc:
                errors[file_code] = str(exc)
            except Exception as exc:
                errors[file_code] = str(exc)

    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
        await asyncio.gather(*(fetch_one(i, code) for i, code in enumerate(requested)))

    files: List[ResolvedFile] = [item for item in results if item is not None]
    failed = max(0, len(requested) - len(files))

    diagnostics: Dict[str, Any] = {
        "stage": "ctfile.batch_getfile",
        "requested": len(requested_raw),
        "requested_unique": len(requested),
        "resolved": len(files),
        "failed": failed,
        "truncated": len(requested) < len(requested_raw),
    }

    if errors:
        preview: Dict[str, str] = {}
        for key in list(errors.keys())[:12]:
            preview[key] = errors[key]
        diagnostics["error_preview"] = preview
        diagnostics["error_count"] = len(errors)

    if failed > 0:
        raise CtfileApiError(
            f"CTFile 获取文件信息失败：{failed}/{len(requested)}",
            diagnostics=diagnostics,
        )

    return files, diagnostics


async def fetch_ctfile_download_url(token: str, ctx: Dict[str, Any], file_id: str) -> str:
    """根据解析上下文获取单文件直链。"""
    token_effective = str(token or "").strip()
    pwd = str((ctx or {}).get("pwd", "") or "").strip()
    file_code = str(file_id or "").strip()
    if not file_code:
        # 兼容旧版本：单文件场景下 ctx.fileid 可能是分享 code
        file_code = str((ctx or {}).get("fileid", "") or "").strip()
    if not file_code:
        raise CtfileApiError("CTFile 下载上下文不完整（缺少 file_id）", diagnostics={"stage": "ctfile.ctx", "ctx": ctx})

    ref = _DEFAULT_REF
    timeout = aiohttp.ClientTimeout(total=25)
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": ref,
        "Referer": ref,
    }

    connector = aiohttp.TCPConnector(ssl=_build_tls_context())
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
        path_hint = _ctfile_getfile_path_hint(str((ctx or {}).get("share_url", "") or ""), file_code)
        # 先 getfile，拿 userid/file_id/file_chk/start_time/verifycode 等（避免缓存过期）。
        params = {
            "path": path_hint,
            "f": file_code,
            "passcode": pwd,
            "token": token_effective,
            "r": str(random.random()),
            "ref": ref,
            "url": str((ctx or {}).get("canonical_url") or (ctx or {}).get("share_url") or "").strip() or ref,
        }
        payload = await _ctfile_get_json(
            session,
            url=_API_BASE + "/getfile.php",
            params=params,
            stage="ctfile.getfile",
            diagnostics={"ctx": {k: v for k, v in (ctx or {}).items() if k != "token"}},
        )

        code = int(payload.get("code") or 0)
        if code != 200:
            message = ""
            try:
                message = str((payload.get("file") or {}).get("message") or payload.get("message") or "").strip()
            except Exception:
                message = ""
            raise CtfileApiError(message or f"CTFile 解析失败：code={code}", diagnostics={"stage": "ctfile.getfile", "code": code})

        file_obj = payload.get("file") if isinstance(payload, dict) else {}
        if not isinstance(file_obj, dict):
            file_obj = {}

        uid = str(file_obj.get("userid", "") or "").strip()
        fid = str(file_obj.get("file_id", "") or "").strip()
        chk = str(file_obj.get("file_chk", "") or "").strip()
        is_vip = int(file_obj.get("is_vip") or 0) if str(file_obj.get("is_vip") or "").strip() else 0
        start_time = str(file_obj.get("start_time", "") or "").strip()
        wait_seconds = str(file_obj.get("wait_seconds", "") or "").strip()
        verifycode = str(file_obj.get("verifycode", "") or "").strip()

        if not uid or not fid or not chk:
            raise CtfileApiError(
                "CTFile 返回字段缺失（userid/file_id/file_chk）",
                diagnostics={"stage": "ctfile.getfile", "uid": uid, "fid": fid, "chk": bool(chk)},
            )

        # VIP 用户可能直接给出可用链接（不同线路字段名不同）。
        if is_vip == 1:
            for key in ("vip_dx_url", "vip_yd_url", "vip_lt_url", "us_downurl_a"):
                url = str(file_obj.get(key, "") or "").strip()
                if url:
                    return url

        # 普通用户：再请求一次性直链（现代接口需要 start_time/verifycode 等参数）
        params2 = {
            "uid": uid,
            "fid": fid,
            "folder_id": str((ctx or {}).get("folder_id", "") or 0),
            "share_id": str((ctx or {}).get("share_id", "") or ""),
            "file_chk": chk,
            "start_time": start_time or "0",
            "wait_seconds": wait_seconds or "0",
            "mb": "0",
            "app": "0",
            "acheck": "2",
            "verifycode": verifycode,
            "rd": str(random.random()),
        }
        payload2 = await _ctfile_get_json(
            session,
            url=_API_BASE + "/get_file_url.php",
            params=params2,
            stage="ctfile.get_file_url",
            diagnostics={"ctx": {k: v for k, v in (ctx or {}).items() if k != "token"}},
        )

        code2 = int(payload2.get("code") or 0)
        if code2 == 200:
            url = str(payload2.get("downurl", "") or "").strip()
            if not url:
                raise CtfileApiError(
                    "CTFile 返回直链为空",
                    diagnostics={"stage": "ctfile.get_file_url", "code": code2},
                )
            return url

        if code2 == 302:
            raise CtfileApiError(
                "需要登录 CTFile（请在设置页配置 token/session_id）",
                diagnostics={"stage": "ctfile.get_file_url", "code": code2},
            )

        message2 = str(payload2.get("message", "") or "").strip()
        raise CtfileApiError(
            message2 or f"CTFile 获取直链失败：code={code2}",
            diagnostics={"stage": "ctfile.get_file_url", "code": code2},
        )
