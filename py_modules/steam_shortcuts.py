"""Steam shortcut integration for Freedeck Tianyi installs."""

from __future__ import annotations

import asyncio
import binascii
import os
import re
import struct
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

try:
    import vdf
except Exception:
    vdf = None  # type: ignore[assignment]

import config


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _strip_outer_quotes(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1].strip()
    return text


def _find_steam_root() -> str:
    homes = []
    home_candidates = [
        str(os.environ.get("DECKY_USER_HOME", "") or "").strip(),
        str(os.environ.get("HOME", "") or "").strip(),
        str(Path.home()),
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

    candidates = []
    for home in homes:
        candidates.append(os.path.join(home, ".steam", "steam"))
        candidates.append(os.path.join(home, ".local", "share", "Steam"))

    for item in candidates:
        steamapps = os.path.join(item, "steamapps")
        if os.path.isdir(steamapps):
            return os.path.realpath(item)
    return ""


def _steam64_to_account_id(steam64_id: str) -> str:
    try:
        value = int(str(steam64_id).strip())
    except Exception:
        return ""
    return str(value & 0xFFFFFFFF)


def _detect_active_user(steam_root: str) -> str:
    userdata_root = os.path.join(steam_root, "userdata")
    if not os.path.isdir(userdata_root):
        return ""

    # Primary path: loginusers.vdf with MostRecent=1.
    loginusers_path = os.path.join(steam_root, "config", "loginusers.vdf")
    try:
        if vdf is None:
            raise RuntimeError("vdf module unavailable")
        if os.path.isfile(loginusers_path):
            with open(loginusers_path, "r", encoding="utf-8", errors="ignore") as fp:
                payload = vdf.load(fp)
            users = payload.get("users") if isinstance(payload, dict) else None
            if isinstance(users, dict):
                for steam64, info in users.items():
                    if not isinstance(info, dict):
                        continue
                    if str(info.get("MostRecent", "")).strip() != "1":
                        continue
                    account_id = _steam64_to_account_id(str(steam64))
                    if not account_id or account_id == "0":
                        continue
                    candidate = os.path.join(userdata_root, account_id)
                    if os.path.isdir(candidate):
                        return account_id
    except Exception as exc:
        config.logger.warning("Steam loginusers parse failed: %s", exc)

    # Fallback: latest mtime numeric user dir, excluding user 0.
    candidates = []
    try:
        for name in os.listdir(userdata_root):
            if not name.isdigit() or name == "0":
                continue
            full = os.path.join(userdata_root, name)
            if not os.path.isdir(full):
                continue
            candidates.append((name, os.path.getmtime(full)))
    except Exception:
        return ""

    if not candidates:
        return ""
    candidates.sort(key=lambda item: item[1], reverse=True)
    return str(candidates[0][0])


def _shortcuts_path(steam_root: str, user_id: str) -> str:
    return os.path.join(steam_root, "userdata", user_id, "config", "shortcuts.vdf")


def _config_vdf_path(steam_root: str) -> str:
    return os.path.join(steam_root, "config", "config.vdf")


def _grid_dir(steam_root: str, user_id: str) -> str:
    return os.path.join(steam_root, "userdata", user_id, "config", "grid")


def _load_shortcuts_vdf(path: str) -> Dict[str, Any]:
    if vdf is None:
        return {"shortcuts": {}}
    if not os.path.isfile(path):
        return {"shortcuts": {}}

    try:
        with open(path, "rb") as fp:
            raw = fp.read()
        if not raw:
            return {"shortcuts": {}}
        payload = vdf.binary_loads(raw)
    except Exception as exc:
        config.logger.warning("Load shortcuts.vdf failed: %s", exc)
        return {"shortcuts": {}}

    if not isinstance(payload, dict):
        return {"shortcuts": {}}
    shortcuts = payload.get("shortcuts")
    if not isinstance(shortcuts, dict):
        payload["shortcuts"] = {}
    return payload


def _atomic_write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=".freedeck_", suffix=".tmp", dir=os.path.dirname(path), delete=False) as fp:
        fp.write(data)
        fp.flush()
        os.fsync(fp.fileno())
        temp_path = fp.name
    os.replace(temp_path, path)


def _atomic_write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=".freedeck_",
        suffix=".tmp",
        dir=os.path.dirname(path),
        delete=False,
    ) as fp:
        fp.write(content)
        fp.flush()
        os.fsync(fp.fileno())
        temp_path = fp.name
    os.replace(temp_path, path)


def _save_shortcuts_vdf(path: str, payload: Dict[str, Any]) -> None:
    if vdf is None:
        raise RuntimeError("vdf module unavailable")
    encoded = vdf.binary_dumps(payload)
    _atomic_write_bytes(path, encoded)


def _generate_non_steam_app_id(app_name: str, exe_path: str) -> int:
    key = f"{exe_path}{app_name}"
    crc = binascii.crc32(key.encode("utf-8")) & 0xFFFFFFFF
    unsigned = crc | 0x80000000
    return struct.unpack("i", struct.pack("I", unsigned))[0]


def _upsert_shortcut_sync(
    *,
    game_id: str,
    display_name: str,
    exe_path: str,
    launch_options: str,
) -> Dict[str, Any]:
    if vdf is None:
        return {"ok": False, "message": "vdf 模块不可用，无法写入 Steam shortcuts.vdf"}

    exe_real = os.path.realpath(os.path.expanduser(str(exe_path or "").strip()))
    if not os.path.isfile(exe_real):
        return {"ok": False, "message": "可执行文件不存在", "exe_path": exe_real}

    steam_root = _find_steam_root()
    if not steam_root:
        return {"ok": False, "message": "未找到 Steam 安装目录"}

    user_id = _detect_active_user(steam_root)
    if not user_id:
        return {"ok": False, "message": "未找到已登录 Steam 用户"}

    shortcuts_file = _shortcuts_path(steam_root, user_id)
    payload = _load_shortcuts_vdf(shortcuts_file)
    shortcuts = payload.setdefault("shortcuts", {})
    if not isinstance(shortcuts, dict):
        shortcuts = {}
        payload["shortcuts"] = shortcuts

    launch_key = str(launch_options or "").strip()
    expected_token = _extract_tianyi_launch_token(launch_key)
    if not expected_token:
        expected_token = _derive_tianyi_launch_token(game_id)
    target_idx: Optional[str] = None
    target_entry: Optional[Dict[str, Any]] = None
    for idx, item in shortcuts.items():
        if not isinstance(item, dict):
            continue
        current_token = _extract_tianyi_token_from_shortcut(item)
        if expected_token and current_token == expected_token:
            target_idx = str(idx)
            target_entry = item
            break

    action = "created"
    if target_entry is None:
        numeric = []
        for key in shortcuts.keys():
            try:
                numeric.append(int(str(key)))
            except Exception:
                continue
        target_idx = str((max(numeric) + 1) if numeric else 0)
        target_entry = {}
        shortcuts[target_idx] = target_entry
    else:
        action = "updated"

    app_id = _safe_int(target_entry.get("appid"), 0)
    if app_id == 0:
        app_id = _generate_non_steam_app_id(display_name, exe_real)
    app_id_unsigned = int(app_id) & 0xFFFFFFFF

    quoted_exe = f'"{exe_real}"'
    quoted_start = f'"{os.path.dirname(exe_real)}"'
    expected_devkit = _tianyi_devkit_game_id(expected_token)

    existing_launch_options = str(target_entry.get("LaunchOptions", "") or "").strip()
    existing_launch_options = _strip_tianyi_launch_token(existing_launch_options)
    incoming_launch_options = _strip_tianyi_launch_token(launch_key)
    effective_launch_options = incoming_launch_options if incoming_launch_options else existing_launch_options

    target_entry.update(
        {
            "appid": int(app_id),
            "AppName": str(display_name or Path(exe_real).stem or "Freedeck Game"),
            "exe": quoted_exe,
            "StartDir": quoted_start,
            "icon": str(target_entry.get("icon", "") or ""),
            "ShortcutPath": str(target_entry.get("ShortcutPath", "") or ""),
            "LaunchOptions": effective_launch_options,
            "IsHidden": 0,
            "AllowDesktopConfig": 1,
            "OpenVR": 0,
            "Devkit": 0,
            "DevkitGameID": expected_devkit,
            "DevkitOverrideAppID": "0",
            "LastPlayTime": 0,
            "FlatpakAppID": "",
            "tags": {"0": "Freedeck", "1": "Tianyi"},
        }
    )

    try:
        _save_shortcuts_vdf(shortcuts_file, payload)
    except Exception as exc:
        return {"ok": False, "message": f"写入 shortcuts.vdf 失败: {exc}"}

    return {
        "ok": True,
        "action": action,
        "steam_root": steam_root,
        "user_id": user_id,
        "shortcut_path": shortcuts_file,
        "exe_path": exe_real,
        "appid": int(app_id),
        "appid_unsigned": int(app_id_unsigned),
        "launch_options": launch_key,
    }


def _set_proton_mapping_sync(*, steam_root: str, app_id: int, compat_tool: str) -> Dict[str, Any]:
    config_path = _config_vdf_path(steam_root)
    if not os.path.isfile(config_path):
        return {"ok": False, "message": f"config.vdf 不存在: {config_path}"}

    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    app_id_str = str(app_id_unsigned)
    tool_name = str(compat_tool or "").strip()
    if not tool_name:
        return {"ok": False, "message": "compat_tool 为空"}

    try:
        with open(config_path, "r", encoding="utf-8", errors="ignore") as fp:
            content = fp.read()
    except Exception as exc:
        return {"ok": False, "message": f"读取 config.vdf 失败: {exc}"}

    marker = '"CompatToolMapping"'
    marker_pos = content.find(marker)
    if marker_pos < 0:
        return {"ok": False, "message": "config.vdf 中缺少 CompatToolMapping"}

    brace_pos = content.find("{", marker_pos)
    if brace_pos < 0:
        return {"ok": False, "message": "CompatToolMapping 结构异常"}

    compat_entry = (
        f'\n\t\t\t\t\t"{app_id_str}"\n'
        f"\t\t\t\t\t{{\n"
        f'\t\t\t\t\t\t"name"\t\t"{tool_name}"\n'
        f'\t\t\t\t\t\t"config"\t\t""\n'
        f'\t\t\t\t\t\t"priority"\t\t"250"\n'
        f"\t\t\t\t\t}}"
    )

    entry_pattern = re.compile(rf'\s*"{app_id_str}"\s*\{{[^}}]*\}}', re.DOTALL)
    if entry_pattern.search(content):
        updated = entry_pattern.sub(compat_entry, content, count=1)
    else:
        updated = content[: brace_pos + 1] + compat_entry + content[brace_pos + 1 :]

    if updated == content:
        return {"ok": True, "message": "Proton 映射保持不变"}

    try:
        _atomic_write_text(config_path, updated)
    except Exception as exc:
        return {"ok": False, "message": f"写入 config.vdf 失败: {exc}"}

    return {"ok": True, "message": f"已设置 Proton: {tool_name}"}


def _get_proton_mapping_sync(*, steam_root: str, app_id: int) -> Dict[str, Any]:
    config_path = _config_vdf_path(steam_root)
    if not os.path.isfile(config_path):
        return {"ok": False, "message": f"config.vdf 不存在: {config_path}", "compat_tool": ""}

    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    app_id_str = str(app_id_unsigned)

    try:
        with open(config_path, "r", encoding="utf-8", errors="ignore") as fp:
            content = fp.read()
    except Exception as exc:
        return {"ok": False, "message": f"读取 config.vdf 失败: {exc}", "compat_tool": ""}

    pattern = re.compile(
        rf'"{re.escape(app_id_str)}"\s*\{{.*?"name"\s*"\s*([^"\r\n]+)"',
        re.DOTALL,
    )
    matched = pattern.search(content)
    compat_tool = str(matched.group(1) or "").strip() if matched else ""
    return {
        "ok": True,
        "message": "已读取 CompatToolMapping" if compat_tool else "CompatToolMapping 不存在",
        "compat_tool": compat_tool,
    }


def _remove_proton_mapping_sync(*, steam_root: str, app_id: int) -> Dict[str, Any]:
    """Remove compat tool mapping for shortcut app id."""
    config_path = _config_vdf_path(steam_root)
    if not os.path.isfile(config_path):
        return {"ok": False, "removed": False, "message": f"config.vdf 不存在: {config_path}"}

    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    app_id_str = str(app_id_unsigned)

    try:
        with open(config_path, "r", encoding="utf-8", errors="ignore") as fp:
            content = fp.read()
    except Exception as exc:
        return {"ok": False, "removed": False, "message": f"读取 config.vdf 失败: {exc}"}

    marker = '"CompatToolMapping"'
    marker_pos = content.find(marker)
    if marker_pos < 0:
        return {"ok": True, "removed": False, "message": "config.vdf 中缺少 CompatToolMapping"}

    entry_pattern = re.compile(rf'\s*"{re.escape(app_id_str)}"\s*\{{[^}}]*\}}', re.DOTALL)
    updated, replaced = entry_pattern.subn("", content, count=1)
    if replaced <= 0:
        return {"ok": True, "removed": False, "message": "Proton 映射不存在"}

    try:
        _atomic_write_text(config_path, updated)
    except Exception as exc:
        return {"ok": False, "removed": False, "message": f"写入 config.vdf 失败: {exc}"}

    return {"ok": True, "removed": True, "message": "已删除 Proton 映射"}


def _remove_grid_assets_sync(*, steam_root: str, user_id: str, app_id: int) -> Dict[str, Any]:
    """Remove Steam grid assets for shortcut app id."""
    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    grid_dir = _grid_dir(steam_root, user_id)
    bases = [
        (os.path.join(grid_dir, f"{app_id_unsigned}"), [".jpg", ".png"]),
        (os.path.join(grid_dir, f"{app_id_unsigned}p"), [".jpg", ".png"]),
        (os.path.join(grid_dir, f"{app_id_unsigned}_hero"), [".jpg", ".png"]),
        (os.path.join(grid_dir, f"{app_id_unsigned}_logo"), [".png", ".jpg"]),
        (os.path.join(grid_dir, f"{app_id_unsigned}_icon"), [".jpg", ".png"]),
    ]
    targets: List[str] = []
    for base, exts in bases:
        for ext in exts:
            targets.append(f"{base}{ext}")
    removed: List[str] = []
    failed: List[str] = []
    for path in targets:
        if not os.path.isfile(path):
            continue
        try:
            os.remove(path)
            removed.append(path)
        except Exception:
            failed.append(path)
    return {
        "ok": len(failed) == 0,
        "removed_count": len(removed),
        "removed": removed,
        "failed": failed,
    }


def _remove_compatdata_prefix_sync(*, steam_root: str, app_id: int) -> Dict[str, Any]:
    """Remove Steam compatdata prefix for shortcut app id."""
    compat_root = os.path.realpath(os.path.join(str(steam_root or "").strip(), "steamapps", "compatdata"))
    if not compat_root or not os.path.isdir(compat_root):
        return {"ok": False, "removed": False, "message": f"compatdata 目录不存在: {compat_root}"}

    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    if app_id_unsigned <= 0:
        return {"ok": False, "removed": False, "message": "appid 无效"}

    target = os.path.realpath(os.path.join(compat_root, str(app_id_unsigned)))
    if target != compat_root and not target.startswith(compat_root + os.sep):
        return {"ok": False, "removed": False, "message": "compatdata 路径不安全"}
    if not os.path.lexists(target):
        return {"ok": True, "removed": False, "path": target, "message": "Proton 前缀不存在"}

    try:
        if os.path.islink(target) or os.path.isfile(target):
            os.remove(target)
        else:
            shutil.rmtree(target, ignore_errors=False)
    except Exception as exc:
        return {"ok": False, "removed": False, "path": target, "message": f"删除 Proton 前缀失败: {exc}"}

    return {"ok": True, "removed": True, "path": target, "message": "已删除 Proton 前缀"}


def _reindex_shortcuts(shortcuts: Dict[str, Any]) -> Dict[str, Any]:
    """Rebuild numeric shortcut indexes after deletions."""
    rows: List[Tuple[int, Dict[str, Any]]] = []
    for key, value in shortcuts.items():
        if not isinstance(value, dict):
            continue
        try:
            idx = int(str(key))
        except Exception:
            idx = 10**9
        rows.append((idx, value))
    rows.sort(key=lambda item: item[0])
    rebuilt: Dict[str, Any] = {}
    for idx, (_, item) in enumerate(rows):
        rebuilt[str(idx)] = item
    return rebuilt


def _remove_tianyi_shortcut_sync(*, game_id: str) -> Dict[str, Any]:
    """Remove Freedeck shortcut entry from shortcuts.vdf by game_id."""
    if vdf is None:
        return {"ok": False, "message": "vdf 模块不可用，无法写入 Steam shortcuts.vdf", "removed": False}

    expected_token = _derive_tianyi_launch_token(game_id)
    expected_launch_options = _tianyi_devkit_game_id(expected_token) or f"freedeck:tianyi:{expected_token}"

    steam_root = _find_steam_root()
    if not steam_root:
        return {"ok": False, "message": "未找到 Steam 安装目录", "removed": False}

    user_id = _detect_active_user(steam_root)
    if not user_id:
        return {"ok": False, "message": "未找到已登录 Steam 用户", "removed": False}

    shortcuts_file = _shortcuts_path(steam_root, user_id)
    payload = _load_shortcuts_vdf(shortcuts_file)
    shortcuts = payload.setdefault("shortcuts", {})
    if not isinstance(shortcuts, dict):
        shortcuts = {}
        payload["shortcuts"] = shortcuts

    removed_items: List[Dict[str, Any]] = []
    remove_keys: List[str] = []
    for idx, item in shortcuts.items():
        if not isinstance(item, dict):
            continue
        token = _extract_tianyi_token_from_shortcut(item)
        if token == expected_token:
            remove_keys.append(str(idx))
            removed_items.append(dict(item))

    if not remove_keys:
        return {
            "ok": True,
            "removed": False,
            "message": "未找到对应 Freedeck 快捷方式",
            "steam_root": steam_root,
            "user_id": user_id,
            "shortcut_path": shortcuts_file,
            "launch_options": expected_launch_options,
        }

    for key in remove_keys:
        shortcuts.pop(key, None)
    payload["shortcuts"] = _reindex_shortcuts(shortcuts)

    try:
        _save_shortcuts_vdf(shortcuts_file, payload)
    except Exception as exc:
        return {"ok": False, "removed": False, "message": f"写入 shortcuts.vdf 失败: {exc}"}

    app_id = 0
    app_id_unsigned = 0
    if removed_items:
        app_id = _safe_int(removed_items[0].get("appid"), 0)
        app_id_unsigned = int(app_id) & 0xFFFFFFFF if app_id else 0

    return {
        "ok": True,
        "removed": True,
        "message": f"已删除 {len(remove_keys)} 个快捷方式",
        "steam_root": steam_root,
        "user_id": user_id,
        "shortcut_path": shortcuts_file,
        "launch_options": expected_launch_options,
        "appid": int(app_id),
        "appid_unsigned": int(app_id_unsigned),
        "removed_count": len(remove_keys),
    }


def _infer_image_ext(url: str, content_type: str) -> str:
    ctype = str(content_type or "").split(";", 1)[0].strip().lower()
    if ctype == "image/png":
        return ".png"
    if ctype in {"image/jpeg", "image/jpg"}:
        return ".jpg"

    lowered = str(url or "").strip().lower()
    if lowered.endswith(".png"):
        return ".png"
    if lowered.endswith(".jpg") or lowered.endswith(".jpeg"):
        return ".jpg"
    return ""


async def _fetch_image_bytes(
    session: aiohttp.ClientSession,
    url: str,
) -> tuple[int, bytes, str]:
    source = str(url or "").strip()
    if not source:
        return 0, b"", ""
    try:
        async with session.get(source, ssl=False) as resp:
            status = int(resp.status)
            content_type = str(resp.headers.get("Content-Type", "") or "")
            if status < 200 or status >= 300:
                return status, b"", content_type
            data = await resp.read()
            return status, data or b"", content_type
    except Exception:
        return 0, b"", ""


def _remove_variants_sync(base_path: str, exts: List[str]) -> None:
    base = str(base_path or "").strip()
    if not base:
        return
    for ext in exts:
        path = f"{base}{ext}"
        try:
            if os.path.isfile(path):
                os.remove(path)
        except Exception:
            continue


async def _download_image_to_base(
    *,
    session: aiohttp.ClientSession,
    base_path: str,
    urls: List[str],
    allowed_exts: List[str],
) -> str:
    """Download first available URL and write to base_path + inferred ext.

    Returns the written file path on success, empty string otherwise.
    """
    target_base = str(base_path or "").strip()
    if not target_base:
        return ""

    for item in urls:
        url = str(item or "").strip()
        if not url:
            continue
        status, data, content_type = await _fetch_image_bytes(session, url)
        if status and (status < 200 or status >= 300):
            continue
        if not data:
            continue
        ext = _infer_image_ext(url, content_type)
        if not ext or ext not in allowed_exts:
            continue
        target = f"{target_base}{ext}"
        try:
            await asyncio.to_thread(_atomic_write_bytes, target, data)
            cleanup = [value for value in allowed_exts if value != ext]
            await asyncio.to_thread(_remove_variants_sync, target_base, cleanup)
            return target
        except Exception:
            continue
    return ""


def _copy_file_sync(source_path: str, target_path: str) -> bool:
    try:
        source = os.path.realpath(os.path.expanduser(str(source_path or "").strip()))
        target = os.path.realpath(os.path.expanduser(str(target_path or "").strip()))
        if not source or not target or not os.path.isfile(source):
            return False
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(source, "rb") as src:
            data = src.read()
        _atomic_write_bytes(target, data)
        return True
    except Exception:
        return False


async def _apply_grid_assets(
    *,
    steam_root: str,
    user_id: str,
    app_id: int,
    landscape_urls: List[str],
    portrait_urls: List[str],
    hero_urls: List[str],
    logo_urls: List[str],
    icon_urls: List[str],
) -> Dict[str, Any]:
    grid_dir = _grid_dir(steam_root, user_id)
    await asyncio.to_thread(os.makedirs, grid_dir, mode=0o777, exist_ok=True)

    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    bases = {
        "landscape": os.path.join(grid_dir, f"{app_id_unsigned}"),
        "portrait": os.path.join(grid_dir, f"{app_id_unsigned}p"),
        "hero": os.path.join(grid_dir, f"{app_id_unsigned}_hero"),
        "logo": os.path.join(grid_dir, f"{app_id_unsigned}_logo"),
        "icon": os.path.join(grid_dir, f"{app_id_unsigned}_icon"),
    }

    allowed_exts = {
        "landscape": [".jpg", ".png"],
        "portrait": [".jpg", ".png"],
        "hero": [".jpg", ".png"],
        "logo": [".png", ".jpg"],
        "icon": [".jpg", ".png"],
    }

    status: Dict[str, bool] = {"landscape": False, "portrait": False, "hero": False, "logo": False, "icon": False}
    written: Dict[str, str] = {"landscape": "", "portrait": "", "hero": "", "logo": "", "icon": ""}
    candidates = {
        "landscape": list(landscape_urls or []),
        "portrait": list(portrait_urls or []),
        "hero": list(hero_urls or []),
        "logo": list(logo_urls or []),
        "icon": list(icon_urls or []),
    }

    if not any(bool(urls) for urls in candidates.values()):
        return {"ok": True, "status": status, "message": "无可用封面 URL"}

    timeout = aiohttp.ClientTimeout(total=14)
    headers = {"User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for key, urls in candidates.items():
            if not urls:
                continue
            saved = await _download_image_to_base(
                session=session,
                base_path=bases[key],
                urls=urls,
                allowed_exts=allowed_exts[key],
            )
            if saved:
                status[key] = True
                written[key] = saved

    # Fallback: no landscape -> reuse portrait.
    if not status.get("landscape") and status.get("portrait"):
        source = written.get("portrait")
        if source:
            ext = os.path.splitext(source)[1] or ".jpg"
            if ext not in allowed_exts["landscape"]:
                ext = ".jpg"
            target = f"{bases['landscape']}{ext}"
            reused = await asyncio.to_thread(_copy_file_sync, source, target)
            if reused:
                await asyncio.to_thread(
                    _remove_variants_sync,
                    bases["landscape"],
                    [value for value in allowed_exts["landscape"] if value != ext],
                )
                status["landscape"] = True
                written["landscape"] = target

    # Fallback: no dedicated icon -> reuse portrait, then landscape.
    if not status.get("icon"):
        source = written.get("portrait") or written.get("landscape")
        if source:
            ext = os.path.splitext(source)[1] or ".jpg"
            if ext not in allowed_exts["icon"]:
                ext = ".jpg"
            target = f"{bases['icon']}{ext}"
            reused = await asyncio.to_thread(_copy_file_sync, source, target)
            if reused:
                await asyncio.to_thread(
                    _remove_variants_sync,
                    bases["icon"],
                    [value for value in allowed_exts["icon"] if value != ext],
                )
                status["icon"] = True
                written["icon"] = target

    # Fallback: no hero -> reuse landscape.
    if not status.get("hero") and status.get("landscape"):
        source = written.get("landscape")
        if source:
            ext = os.path.splitext(source)[1] or ".jpg"
            if ext not in allowed_exts["hero"]:
                ext = ".jpg"
            target = f"{bases['hero']}{ext}"
            reused = await asyncio.to_thread(_copy_file_sync, source, target)
            if reused:
                await asyncio.to_thread(
                    _remove_variants_sync,
                    bases["hero"],
                    [value for value in allowed_exts["hero"] if value != ext],
                )
                status["hero"] = True
                written["hero"] = target

    # If this refresh resolved any artwork, remove stale leftovers for types that
    # still have no valid replacement so Steam does not keep showing old mismatched assets.
    if any(status.values()):
        for key, ok in status.items():
            if ok:
                continue
            await asyncio.to_thread(_remove_variants_sync, bases[key], allowed_exts[key])

    return {"ok": True, "status": status, "written": written}


def _default_landscape_cover(app_id: int) -> str:
    app = int(app_id or 0)
    if app <= 0:
        return ""
    return f"https://shared.steamstatic.com/store_item_assets/steam/apps/{app}/header.jpg"


def _default_landscape_cover_fallback(app_id: int) -> str:
    app = int(app_id or 0)
    if app <= 0:
        return ""
    return f"https://shared.steamstatic.com/store_item_assets/steam/apps/{app}/capsule_616x353.jpg"


def _default_portrait_cover(app_id: int) -> str:
    app = int(app_id or 0)
    if app <= 0:
        return ""
    return f"https://shared.steamstatic.com/store_item_assets/steam/apps/{app}/library_600x900_2x.jpg"


def _default_hero_cover(app_id: int) -> str:
    app = int(app_id or 0)
    if app <= 0:
        return ""
    return f"https://shared.steamstatic.com/store_item_assets/steam/apps/{app}/library_hero.jpg"


def _default_logo_cover(app_id: int) -> str:
    app = int(app_id or 0)
    if app <= 0:
        return ""
    return f"https://shared.steamstatic.com/store_item_assets/steam/apps/{app}/logo.png"



def _derive_tianyi_launch_token(game_id: str) -> str:
    """Derive Freedeck launch token for matching shortcuts."""
    token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_id or "")).strip("_")
    return token or "game"

_TIANYI_LAUNCH_TOKEN_RE = re.compile(r"freedeck:tianyi:(?P<token>[A-Za-z0-9._-]+)")


def _extract_tianyi_launch_token(launch_options: str) -> str:
    """Extract Freedeck launch token from Steam LaunchOptions (allow prefix/wrappers)."""
    text = str(launch_options or "").strip()
    if not text:
        return ""
    match = _TIANYI_LAUNCH_TOKEN_RE.search(text)
    if not match:
        return ""
    return str(match.group("token") or "").strip()


def _tianyi_devkit_game_id(token: str) -> str:
    """Encode token into a Steam shortcut field that does NOT affect process args."""
    value = str(token or "").strip()
    if not value:
        return ""
    return f"freedeck:tianyi:{value}"


def _strip_tianyi_launch_token(text: str) -> str:
    """Remove internal Freedeck token from launch options (keep other user args)."""
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = _TIANYI_LAUNCH_TOKEN_RE.sub("", raw)
    return " ".join(cleaned.split()).strip()


def _extract_tianyi_token_from_shortcut(item: Any) -> str:
    """Extract token from shortcut entry.

    Prefer DevkitGameID (new) and fall back to LaunchOptions (legacy).
    """
    if not isinstance(item, dict):
        return ""
    devkit = _extract_tianyi_launch_token(str(item.get("DevkitGameID", "") or ""))
    if devkit:
        return devkit
    launch = _extract_tianyi_launch_token(str(item.get("LaunchOptions", "") or ""))
    return launch


def list_tianyi_shortcuts_sync() -> Dict[str, Any]:
    """List Freedeck Tianyi shortcuts in one pass for fast appid mapping."""
    steam_root = _find_steam_root()
    if not steam_root:
        return {"ok": False, "message": "未找到 Steam 安装目录", "rows": [], "by_token": {}, "by_appid": {}}

    user_id = _detect_active_user(steam_root)
    if not user_id:
        return {"ok": False, "message": "未找到已登录 Steam 用户", "rows": [], "by_token": {}, "by_appid": {}}

    shortcuts_file = _shortcuts_path(steam_root, user_id)
    payload = _load_shortcuts_vdf(shortcuts_file)
    shortcuts = payload.get("shortcuts") if isinstance(payload, dict) else {}
    if not isinstance(shortcuts, dict):
        shortcuts = {}

    rows: List[Dict[str, Any]] = []
    by_token: Dict[str, Dict[str, Any]] = {}
    by_appid: Dict[str, Dict[str, Any]] = {}

    for _, item in shortcuts.items():
        if not isinstance(item, dict):
            continue

        token = _extract_tianyi_token_from_shortcut(item)
        if not token:
            continue

        launch_options = str(item.get("LaunchOptions", "") or "").strip()
        devkit_game_id = str(item.get("DevkitGameID", "") or "").strip()
        app_id = _safe_int(item.get("appid"), 0)
        app_id_unsigned = int(app_id) & 0xFFFFFFFF if app_id else 0
        row = {
            "token": token,
            "launch_options": launch_options,
            "devkit_game_id": devkit_game_id,
            "appid": int(app_id),
            "appid_unsigned": int(app_id_unsigned),
            "app_name": str(item.get("AppName", "") or "").strip(),
        }
        rows.append(row)
        by_token[token] = row
        if app_id_unsigned > 0:
            by_appid[str(app_id_unsigned)] = row

    return {
        "ok": True,
        "message": "",
        "steam_root": steam_root,
        "user_id": user_id,
        "shortcut_path": shortcuts_file,
        "rows": rows,
        "by_token": by_token,
        "by_appid": by_appid,
    }


def migrate_tianyi_shortcut_tokens_sync() -> Dict[str, Any]:
    """Migrate legacy Freedeck launch tokens from LaunchOptions to DevkitGameID.

    Older versions stored `freedeck:tianyi:<token>` in LaunchOptions which could be passed to the game as an argument,
    causing some titles to fail to launch. The token is now stored in DevkitGameID so Steam launch arguments stay clean.
    """
    steam_root = _find_steam_root()
    if not steam_root:
        return {"ok": False, "changed": False, "message": "未找到 Steam 安装目录", "migrated": 0, "cleaned": 0}

    user_id = _detect_active_user(steam_root)
    if not user_id:
        return {"ok": False, "changed": False, "message": "未找到已登录 Steam 用户", "migrated": 0, "cleaned": 0}

    shortcuts_file = _shortcuts_path(steam_root, user_id)
    payload = _load_shortcuts_vdf(shortcuts_file)
    shortcuts = payload.get("shortcuts") if isinstance(payload, dict) else {}
    if not isinstance(shortcuts, dict) or not shortcuts:
        return {
            "ok": True,
            "changed": False,
            "message": "无需迁移（shortcuts 为空）",
            "steam_root": steam_root,
            "user_id": user_id,
            "shortcut_path": shortcuts_file,
            "migrated": 0,
            "cleaned": 0,
        }

    changed = False
    migrated = 0
    cleaned = 0
    for item in shortcuts.values():
        if not isinstance(item, dict):
            continue
        launch_options = str(item.get("LaunchOptions", "") or "").strip()
        devkit_game_id = str(item.get("DevkitGameID", "") or "").strip()
        token_in_launch = _extract_tianyi_launch_token(launch_options)
        token_in_devkit = _extract_tianyi_launch_token(devkit_game_id)
        token = token_in_devkit or token_in_launch
        if not token:
            continue

        if token_in_launch:
            next_launch = _strip_tianyi_launch_token(launch_options)
            if next_launch != launch_options:
                item["LaunchOptions"] = next_launch
                cleaned += 1
                changed = True
            if not token_in_devkit:
                item["DevkitGameID"] = _tianyi_devkit_game_id(token)
                migrated += 1
                changed = True

    if not changed:
        return {
            "ok": True,
            "changed": False,
            "message": "无需迁移（已是新格式）",
            "steam_root": steam_root,
            "user_id": user_id,
            "shortcut_path": shortcuts_file,
            "migrated": 0,
            "cleaned": 0,
        }

    try:
        _save_shortcuts_vdf(shortcuts_file, payload)
    except Exception as exc:
        return {
            "ok": False,
            "changed": False,
            "message": f"写入 shortcuts.vdf 失败: {exc}",
            "steam_root": steam_root,
            "user_id": user_id,
            "shortcut_path": shortcuts_file,
            "migrated": migrated,
            "cleaned": cleaned,
        }

    return {
        "ok": True,
        "changed": True,
        "message": "已迁移 LaunchOptions token",
        "steam_root": steam_root,
        "user_id": user_id,
        "shortcut_path": shortcuts_file,
        "migrated": migrated,
        "cleaned": cleaned,
    }


def resolve_tianyi_shortcut_sync(*, game_id: str) -> Dict[str, Any]:
    """Resolve Freedeck shortcut metadata by game_id."""
    expected_token = _derive_tianyi_launch_token(game_id)
    expected_launch_options = _tianyi_devkit_game_id(expected_token) or f"freedeck:tianyi:{expected_token}"

    steam_root = _find_steam_root()
    if not steam_root:
        return {"ok": False, "message": "未找到 Steam 安装目录"}

    user_id = _detect_active_user(steam_root)
    if not user_id:
        return {"ok": False, "message": "未找到已登录 Steam 用户", "steam_root": steam_root}

    shortcuts_file = _shortcuts_path(steam_root, user_id)
    payload = _load_shortcuts_vdf(shortcuts_file)
    shortcuts = payload.get("shortcuts") if isinstance(payload, dict) else {}
    if not isinstance(shortcuts, dict):
        shortcuts = {}

    target: Optional[Dict[str, Any]] = None
    for _, item in shortcuts.items():
        if not isinstance(item, dict):
            continue
        token = _extract_tianyi_token_from_shortcut(item)
        if token == expected_token:
            target = item
            break

    if not isinstance(target, dict):
        return {
            "ok": False,
            "message": "未找到对应 Freedeck 快捷方式",
            "launch_options": expected_launch_options,
            "steam_root": steam_root,
            "user_id": user_id,
        }

    exe_path = _strip_outer_quotes(str(target.get("exe", "") or target.get("Exe", "") or ""))
    start_dir = _strip_outer_quotes(str(target.get("StartDir", "") or ""))
    app_name = str(target.get("AppName", "") or "").strip()

    app_id = _safe_int(target.get("appid"), 0)
    app_id_unsigned = int(app_id) & 0xFFFFFFFF if app_id else 0
    compat_root = os.path.join(steam_root, "steamapps", "compatdata", str(app_id_unsigned), "pfx", "drive_c", "users")
    compat_candidates = [
        os.path.join(compat_root, "steamuser"),
        os.path.join(compat_root, "deck"),
    ]

    compat_user_dir = ""
    for candidate in compat_candidates:
        if os.path.isdir(candidate):
            compat_user_dir = os.path.realpath(candidate)
            break
    if not compat_user_dir and os.path.isdir(compat_root):
        extra_candidates: List[str] = []
        try:
            with os.scandir(compat_root) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        extra_candidates.append(entry.path)
        except Exception:
            extra_candidates = []

        if extra_candidates:
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
            compat_user_dir = os.path.realpath(extra_candidates[0])

    return {
        "ok": True,
        "message": "",
        "steam_root": steam_root,
        "user_id": user_id,
        "shortcut_path": shortcuts_file,
        "launch_options": str(target.get("LaunchOptions", "") or "").strip(),
        "app_name": app_name,
        "exe_path": exe_path,
        "start_dir": start_dir,
        "appid": int(app_id),
        "appid_unsigned": int(app_id_unsigned),
        "compat_user_dir": compat_user_dir,
        "compat_candidates": [os.path.realpath(path) for path in compat_candidates],
    }

async def add_or_update_tianyi_shortcut(
    *,
    game_id: str,
    display_name: str,
    exe_path: str,
    launch_options: str,
    proton_tool: str = "",
    cover_landscape_url: str = "",
    cover_portrait_url: str = "",
    cover_hero_url: str = "",
    cover_logo_url: str = "",
    cover_icon_url: str = "",
    steam_app_id: int = 0,
) -> Dict[str, Any]:
    """Add/update shortcut, set Proton mapping, and write Steam grid assets."""
    shortcut = await asyncio.to_thread(
        _upsert_shortcut_sync,
        game_id=str(game_id or "").strip(),
        display_name=str(display_name or "").strip(),
        exe_path=str(exe_path or "").strip(),
        launch_options=str(launch_options or "").strip(),
    )
    if not shortcut.get("ok"):
        return shortcut

    steam_root = str(shortcut.get("steam_root", "") or "")
    user_id = str(shortcut.get("user_id", "") or "")
    app_id = _safe_int(shortcut.get("appid"), 0)

    proton_tool_name = str(proton_tool or "").strip()
    if proton_tool_name:
        proton = await asyncio.to_thread(
            _set_proton_mapping_sync,
            steam_root=steam_root,
            app_id=app_id,
            compat_tool=proton_tool_name,
        )
    else:
        current_mapping = await asyncio.to_thread(
            _get_proton_mapping_sync,
            steam_root=steam_root,
            app_id=app_id,
        )
        current_tool = str(current_mapping.get("compat_tool", "") or "").strip().lower()
        if current_tool in {"proton_experimental", "proton-experimental"}:
            proton = await asyncio.to_thread(
                _remove_proton_mapping_sync,
                steam_root=steam_root,
                app_id=app_id,
            )
        else:
            proton = {"ok": True, "message": "未设置兼容层映射"}

    landscape = str(cover_landscape_url or "").strip()
    portrait = str(cover_portrait_url or "").strip()
    hero = str(cover_hero_url or "").strip()
    logo = str(cover_logo_url or "").strip()
    icon = str(cover_icon_url or "").strip()
    steam_app = _safe_int(steam_app_id, 0)
    landscape_urls: List[str] = [landscape, _default_landscape_cover(steam_app), _default_landscape_cover_fallback(steam_app)]
    portrait_urls: List[str] = [portrait, _default_portrait_cover(steam_app)]
    hero_urls: List[str] = [hero, _default_hero_cover(steam_app)]
    logo_urls: List[str] = [logo, _default_logo_cover(steam_app)]
    icon_urls: List[str] = [icon]

    artwork = await _apply_grid_assets(
        steam_root=steam_root,
        user_id=user_id,
        app_id=app_id,
        landscape_urls=landscape_urls,
        portrait_urls=portrait_urls,
        hero_urls=hero_urls,
        logo_urls=logo_urls,
        icon_urls=icon_urls,
    )

    result = dict(shortcut)
    result["proton"] = proton
    result["artwork"] = artwork
    result["ok"] = bool(shortcut.get("ok"))
    return result


async def remove_tianyi_shortcut(*, game_id: str, delete_compatdata: bool = False, fallback_app_id: int = 0) -> Dict[str, Any]:
    """Remove Freedeck shortcut and best-effort cleanup Proton mapping and artwork."""
    target_game_id = str(game_id or "").strip()
    if not target_game_id:
        return {"ok": False, "removed": False, "message": "game_id 不能为空"}

    shortcut = await asyncio.to_thread(_remove_tianyi_shortcut_sync, game_id=target_game_id)
    result: Dict[str, Any] = dict(shortcut)
    if not bool(shortcut.get("ok")):
        return result

    if not bool(shortcut.get("removed")):
        result["proton"] = {"ok": True, "removed": False, "message": "未执行 Proton 清理（快捷方式不存在）"}
        result["artwork"] = {
            "ok": True,
            "removed_count": 0,
            "removed": [],
            "failed": [],
            "message": "未执行封面清理（快捷方式不存在）",
        }
        if delete_compatdata:
            steam_root = str(shortcut.get("steam_root", "") or "").strip()
            compat_app_id = _safe_int(fallback_app_id, 0)
            if steam_root and compat_app_id > 0:
                result["compatdata"] = await asyncio.to_thread(
                    _remove_compatdata_prefix_sync,
                    steam_root=steam_root,
                    app_id=compat_app_id,
                )
            else:
                result["compatdata"] = {"ok": True, "removed": False, "message": "未执行 Proton 前缀清理（快捷方式不存在）"}
        else:
            result["compatdata"] = {"ok": True, "removed": False, "message": "跳过 Proton 前缀清理"}
        result["cleanup_ok"] = bool(result["compatdata"].get("ok"))
        return result

    steam_root = str(shortcut.get("steam_root", "") or "").strip()
    user_id = str(shortcut.get("user_id", "") or "").strip()
    app_id = _safe_int(shortcut.get("appid"), 0)

    proton: Dict[str, Any] = {"ok": True, "removed": False, "message": "跳过 Proton 清理（appid 无效）"}
    artwork: Dict[str, Any] = {
        "ok": True,
        "removed_count": 0,
        "removed": [],
        "failed": [],
        "message": "跳过封面清理（appid 或 user_id 无效）",
    }
    compatdata: Dict[str, Any] = {"ok": True, "removed": False, "message": "跳过 Proton 前缀清理"}

    if steam_root and app_id > 0:
        proton = await asyncio.to_thread(_remove_proton_mapping_sync, steam_root=steam_root, app_id=app_id)
        if user_id:
            artwork = await asyncio.to_thread(
                _remove_grid_assets_sync,
                steam_root=steam_root,
                user_id=user_id,
                app_id=app_id,
            )
        if delete_compatdata:
            compatdata = await asyncio.to_thread(_remove_compatdata_prefix_sync, steam_root=steam_root, app_id=app_id)
    elif delete_compatdata and steam_root and _safe_int(fallback_app_id, 0) > 0:
        compatdata = await asyncio.to_thread(
            _remove_compatdata_prefix_sync,
            steam_root=steam_root,
            app_id=_safe_int(fallback_app_id, 0),
        )
    elif delete_compatdata:
        compatdata = {"ok": False, "removed": False, "message": "缺少 appid，无法删除 Proton 前缀"}

    result["proton"] = proton
    result["artwork"] = artwork
    result["compatdata"] = compatdata
    result["cleanup_ok"] = bool(proton.get("ok")) and bool(artwork.get("ok")) and bool(compatdata.get("ok"))
    return result

