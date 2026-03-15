# gba_catalog.py - GBA 模拟器目录（静态 CSV）
#
# 该模块负责读取 GBA 资源 CSV 并提供搜索/分页能力。

from __future__ import annotations

import csv
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import decky

import config


@dataclass
class GbaCatalogEntry:
    """单条 GBA 目录记录。"""

    game_id: str
    title: str
    category_parent: str = "gba"
    categories: str = "GBA"
    down_url: str = ""
    pwd: str = ""
    openpath: str = ""
    size_bytes: int = 0
    size_text: str = ""
    app_id: int = 0
    rom_mbit: float = 0.0

    def to_dict(self) -> Dict[str, object]:
        """转为前端可用字典。"""
        return asdict(self)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _normalize_title(raw: str) -> str:
    return " ".join((raw or "").replace("\u3000", " ").strip().split())


def _format_size_bytes(size_bytes: int) -> str:
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


def _extract_rom_mbit(text: str) -> float:
    raw = str(text or "")
    match = re.search(r"(\d+(?:\.\d+)?)\s*Mb", raw, re.IGNORECASE)
    if match and match.group(1):
        return _safe_float(match.group(1), 0.0)
    # 兼容少数条目使用“ 64.00”这种尾部数字标注大小的情况。
    tail_match = re.search(r"(?i)(?:^|[^a-z])(\d+(?:\.\d+)?)\s*$", raw.strip())
    if tail_match and tail_match.group(1):
        value = _safe_float(tail_match.group(1), 0.0)
        if 0 < value <= 2048:
            return value
    return 0.0


def _mbit_to_bytes(mbit: float) -> int:
    """把 ROM 标注的 Mb（通常为 Mbit）换算为字节数。

    说明：列表里的“32Mb/64Mb/128Mb/256Mb”通常是 Mbit。
    这里按 1 Mbit = 1024*1024 bit 计算。
    """
    try:
        value = float(mbit or 0.0)
    except Exception:
        return 0
    if value <= 0:
        return 0
    return int(round(value * 1024 * 1024 / 8))


def resolve_default_gba_catalog_path() -> str:
    """解析默认 GBA 目录 CSV 路径。"""
    candidates: List[Path] = []

    env_path = (os.getenv("FRIENDECK_GBA_CATALOG_CSV") or "").strip()
    if env_path:
        candidates.append(Path(env_path))

    plugin_dir = getattr(decky, "DECKY_PLUGIN_DIR", None)
    if plugin_dir:
        root = Path(plugin_dir)
        candidates.append(root / "defaults" / "tianyi_catalog" / "gba_catalog.csv")
        candidates.append(root / "defaults" / "gba_catalog.csv")

    cwd = Path.cwd()
    candidates.append(cwd / "exports" / "gba_catalog.csv")
    candidates.append(cwd / "gba_catalog.csv")

    for path in candidates:
        try:
            resolved = path.expanduser().resolve()
        except Exception:
            continue
        if resolved.is_file():
            return str(resolved)

    return ""


class GbaCatalog:
    """GBA 目录仓库（从 CSV 读取）。"""

    def __init__(self, csv_path: str):
        self.csv_path = str(csv_path or "")
        self.entries: List[GbaCatalogEntry] = []
        self.invalid_rows = 0
        self.load_error = ""

    def load(self) -> None:
        self.entries = []
        self.invalid_rows = 0
        self.load_error = ""

        if not self.csv_path:
            self.load_error = "未找到 GBA 目录 CSV 路径"
            config.logger.warning(self.load_error)
            return
        if not os.path.isfile(self.csv_path):
            self.load_error = f"GBA 目录 CSV 不存在: {self.csv_path}"
            config.logger.warning(self.load_error)
            return

        with open(self.csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    self.invalid_rows += 1
                    continue

                title = _normalize_title(row.get("title", ""))
                if not title:
                    self.invalid_rows += 1
                    continue

                game_id = str(row.get("game_id", "")).strip() or title
                category_parent = str(row.get("category_parent", "")).strip() or "gba"
                categories = str(row.get("categories", "")).strip() or "GBA"
                down_url = str(row.get("down_url", "")).strip()
                pwd = str(row.get("pwd", "")).strip()
                openpath = str(row.get("openpath", "")).strip()

                rom_mbit = _safe_float(row.get("rom_mbit", row.get("rom_size_mbit", "")), 0.0)
                if rom_mbit <= 0:
                    rom_mbit = _extract_rom_mbit(title)

                size_bytes = _safe_int(row.get("size_bytes", row.get("filesize_z", "0")), 0)
                if size_bytes <= 0 and rom_mbit > 0:
                    size_bytes = _mbit_to_bytes(rom_mbit)

                size_text = str(row.get("size_text", row.get("list_filesize", "")) or "").strip()
                if not size_text:
                    if rom_mbit > 0:
                        size_text = f"{int(rom_mbit)}Mb" if float(rom_mbit).is_integer() else f"{rom_mbit}Mb"
                    elif size_bytes > 0:
                        size_text = _format_size_bytes(size_bytes)

                app_id = _safe_int(row.get("steam_appid", row.get("app_id", "0")), 0)

                self.entries.append(
                    GbaCatalogEntry(
                        game_id=game_id,
                        title=title,
                        category_parent=category_parent,
                        categories=categories,
                        down_url=down_url,
                        pwd=pwd,
                        openpath=openpath,
                        size_bytes=size_bytes,
                        size_text=size_text,
                        app_id=app_id,
                        rom_mbit=float(rom_mbit or 0.0),
                    )
                )

        config.logger.info(
            "已加载 GBA 目录: total=%s invalid=%s file=%s",
            len(self.entries),
            self.invalid_rows,
            self.csv_path,
        )

    def list(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, object]:
        q = (query or "").strip().lower()
        normalized_page = max(1, int(page))
        normalized_size = max(1, min(200, int(page_size)))

        if not q:
            matched = self.entries
        else:
            matched = [
                item
                for item in self.entries
                if q in item.title.lower()
                or q in item.categories.lower()
                or q in item.game_id.lower()
            ]

        start = (normalized_page - 1) * normalized_size
        end = start + normalized_size
        items = matched[start:end]

        return {
            "total": len(matched),
            "page": normalized_page,
            "page_size": normalized_size,
            "items": [e.to_dict() for e in items],
        }

    def ready(self) -> bool:
        return bool(self.entries) and not bool(self.load_error)
