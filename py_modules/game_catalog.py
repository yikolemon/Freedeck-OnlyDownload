# game_catalog.py - 游戏目录加载与检索
#
# 该模块负责读取 CSV、合并封面索引，并提供分组后的分页检索能力。

from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import decky

import config


@dataclass
class CatalogCoverRecord:
    """预构建封面索引记录。"""

    game_id: str
    title: str
    title_key: str
    cover_url: str
    square_cover_url: str
    app_id: int


@dataclass
class GameCatalogEntry:
    """单条游戏目录记录。"""

    game_id: str
    title: str
    category_parent: str
    categories: str
    down_url: str
    pwd: str
    openpath: str
    size_bytes: int
    size_text: str
    app_id: int
    cover_url: str
    square_cover_url: str
    catalog_index: int

    def to_dict(self) -> Dict[str, object]:
        """转为前端可用字典。"""
        return {
            "game_id": self.game_id,
            "title": self.title,
            "category_parent": self.category_parent,
            "categories": self.categories,
            "down_url": self.down_url,
            "pwd": self.pwd,
            "openpath": self.openpath,
            "size_bytes": self.size_bytes,
            "size_text": self.size_text,
            "app_id": self.app_id,
            "cover_url": self.cover_url,
            "square_cover_url": self.square_cover_url,
        }


def _safe_int(value: object) -> int:
    """安全解析整数字段。"""
    try:
        return int(str(value).strip())
    except Exception:
        return 0


def _normalize_title(raw: str) -> str:
    """标准化标题，去除多余空白。"""
    return " ".join((raw or "").replace("\u3000", " ").strip().split())


def _normalize_catalog_title_key(raw_title: str) -> str:
    """构造稳定分组/匹配标题键。"""
    raw = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not raw:
        return ""

    parts = [part.strip() for part in re.split(r"[\/|｜]", raw) if part.strip()]
    english_parts = [part for part in parts if re.search(r"[A-Za-z]", part)]
    english = english_parts[-1] if english_parts else ""
    if english:
        key = re.sub(r"[\u2010-\u2015]", "-", english)
        key = re.sub(r"[^A-Za-z0-9]+", " ", key).strip().lower()
        key = re.sub(
            r"\b(digital\s+deluxe|deluxe|gold|ultimate|complete|definitive|collector'?s|premium)\s+edition\b",
            "",
            key,
        )
        key = re.sub(r"\s+edition\b", "", key)
        key = re.sub(r"\s+", " ", key).strip()
        return key

    cn = parts[0] if parts else raw
    key = re.sub(r"\s+", " ", cn)
    key = re.sub(r"(?:\s*[（(【\[].*?[）)】\]])+$", "", key).strip()
    key = re.sub(r"(数字豪华版|豪华版|黄金版|终极版|完整版|完全版|决定版|年度版|传奇版|典藏版|加强版)\s*$", "", key).strip()
    return key.lower() if key else ""


def _catalog_group_key(entry: GameCatalogEntry) -> str:
    app_id = _safe_int(getattr(entry, "app_id", 0))
    if app_id > 0:
        return f"appid:{app_id}"
    normalized = _normalize_catalog_title_key(str(getattr(entry, "title", "") or ""))
    if normalized:
        return f"title:{normalized}"
    return f"misc:{str(getattr(entry, 'game_id', '') or getattr(entry, 'title', '') or '').strip()}"


def _variant_sort_weight(title_raw: str) -> int:
    title = str(title_raw or "")
    lower = title.lower()
    weight = 0
    if re.search(r"(豪华版|数字豪华版|黄金版|终极版|完整版|完全版|决定版|年度版|传奇版|典藏版|加强版)", title):
        weight += 10
    if re.search(r"(deluxe|digital deluxe|gold|ultimate|complete|definitive|collector|premium)\s+edition", lower):
        weight += 10
    if re.search(r"\b(beta|demo|test|playtest)\b", lower):
        weight += 30
    return weight


def _extract_variant_version_token(entry: GameCatalogEntry) -> str:
    candidates = [
        str(getattr(entry, "openpath", "") or "").strip(),
        str(getattr(entry, "title", "") or "").strip(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        match = re.search(r"(?:^|[^A-Za-z0-9])v?(\d+(?:[._-]\d+){1,5})", candidate, flags=re.IGNORECASE)
        if match and match.group(1):
            return re.sub(r"[._-]+", ".", match.group(1))
    return ""


def _compare_version_tokens_desc(left_raw: str, right_raw: str) -> int:
    left = str(left_raw or "").strip()
    right = str(right_raw or "").strip()
    if not left and not right:
        return 0
    if not left:
        return 1
    if not right:
        return -1

    def to_parts(value: str) -> List[int]:
        out: List[int] = []
        for part in value.split("."):
            try:
                out.append(int(part))
            except Exception:
                continue
        return out

    left_parts = to_parts(left)
    right_parts = to_parts(right)
    length = max(len(left_parts), len(right_parts))
    for index in range(length):
        l = left_parts[index] if index < len(left_parts) else 0
        r = right_parts[index] if index < len(right_parts) else 0
        if l != r:
            return -1 if l > r else 1
    return 0


def _sort_catalog_variants(items: List[GameCatalogEntry]) -> List[GameCatalogEntry]:
    def sort_key(entry: GameCatalogEntry):
        version = _extract_variant_version_token(entry)
        version_parts = tuple(-part for part in [int(p) for p in version.split(".") if p.isdigit()]) if version else ()
        return (
            0 if version else 1,
            version_parts,
            _variant_sort_weight(entry.title),
            len(str(entry.title or "")),
            -_safe_int(entry.size_bytes),
            _safe_int(entry.catalog_index),
        )

    return sorted(list(items or []), key=sort_key)


def _title_sort_key(raw_title: str) -> str:
    normalized = _normalize_catalog_title_key(raw_title)
    return normalized or str(raw_title or "").casefold()


def _build_store_square_cover_url(app_id: int) -> str:
    app = _safe_int(app_id)
    if app <= 0:
        return ""
    return f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app}/library_600x900_2x.jpg"


def _is_valid_tianyi_url(url: str) -> bool:
    """判断是否是支持的天翼分享链接。"""
    value = (url or "").strip()
    return value.startswith("https://cloud.189.cn/t/") or value.startswith("http://cloud.189.cn/t/")


def resolve_default_catalog_path() -> str:
    """解析默认目录文件路径。"""
    candidates: List[Path] = []

    env_path = (os.getenv("FRIENDECK_GAME_CATALOG_CSV") or "").strip()
    if env_path:
        candidates.append(Path(env_path))

    plugin_dir = getattr(decky, "DECKY_PLUGIN_DIR", None)
    if plugin_dir:
        root = Path(plugin_dir)
        candidates.append(root / "defaults" / "tianyi_catalog" / "freedeck_catalog.csv")
        candidates.append(root / "defaults" / "tianyi_catalog" / "gamebox_all_links_20260221_234730.csv")
        candidates.append(root / "defaults" / "tianyi_catalog.csv")

    cwd = Path.cwd()
    candidates.append(cwd / "exports" / "freedeck_catalog.csv")
    candidates.append(cwd / "exports" / "gamebox_all_links_20260221_234730.csv")
    candidates.append(cwd / "freedeck_catalog.csv")
    candidates.append(cwd / "gamebox_all_links_20260221_234730.csv")

    for path in candidates:
        try:
            resolved = path.expanduser().resolve()
        except Exception:
            continue
        if resolved.is_file():
            return str(resolved)

    return ""


def resolve_default_catalog_cover_index_path() -> str:
    """解析默认封面索引 CSV 路径。"""
    candidates: List[Path] = []

    env_path = (os.getenv("FRIENDECK_GAME_COVER_INDEX_CSV") or "").strip()
    if env_path:
        candidates.append(Path(env_path))

    plugin_dir = getattr(decky, "DECKY_PLUGIN_DIR", None)
    if plugin_dir:
        root = Path(plugin_dir)
        candidates.append(root / "defaults" / "tianyi_catalog" / "freedeck_cover_index.csv")
        candidates.append(root / "defaults" / "tianyi_catalog" / "gamebox_cover_index.csv")

    cwd = Path.cwd()
    candidates.append(cwd / "defaults" / "tianyi_catalog" / "freedeck_cover_index.csv")
    candidates.append(cwd / "defaults" / "tianyi_catalog" / "gamebox_cover_index.csv")
    candidates.append(cwd / "freedeck_cover_index.csv")
    candidates.append(cwd / "gamebox_cover_index.csv")

    for path in candidates:
        try:
            resolved = path.expanduser().resolve()
        except Exception:
            continue
        if resolved.is_file():
            return str(resolved)

    return ""


class GameCatalog:
    """游戏目录仓库。"""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.cover_index_path = resolve_default_catalog_cover_index_path()
        self.entries: List[GameCatalogEntry] = []
        self.entries_by_game_id: Dict[str, GameCatalogEntry] = {}
        self.cover_records_by_game_id: Dict[str, CatalogCoverRecord] = {}
        self.cover_records_by_title_key: Dict[str, CatalogCoverRecord] = {}
        self.cover_records_by_app_id: Dict[int, CatalogCoverRecord] = {}
        self.invalid_rows = 0

    def _is_switch_emulator_entry(self, entry: GameCatalogEntry) -> bool:
        """判断该条目是否属于 Switch 模拟器资源。"""
        try:
            return str(getattr(entry, "category_parent", "") or "").strip() == "527"
        except Exception:
            return False

    def _load_cover_index(self) -> None:
        self.cover_records_by_game_id = {}
        self.cover_records_by_title_key = {}
        self.cover_records_by_app_id = {}
        cover_index_path = str(self.cover_index_path or "").strip()
        if not cover_index_path or not os.path.isfile(cover_index_path):
            return

        count = 0
        with open(cover_index_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                title = _normalize_title(row.get("title", row.get("post_title", "")))
                cover_url = str(row.get("cover_url", row.get("cover_image", "")) or "").strip()
                app_id = _safe_int(row.get("steam_appid", row.get("app_id", "0")))
                game_id = str(row.get("game_id", row.get("id", "")) or "").strip()
                if not title and not game_id:
                    continue
                record = CatalogCoverRecord(
                    game_id=game_id,
                    title=title,
                    title_key=_normalize_catalog_title_key(title),
                    cover_url=cover_url,
                    square_cover_url=_build_store_square_cover_url(app_id),
                    app_id=app_id,
                )
                if record.game_id and record.game_id not in self.cover_records_by_game_id:
                    self.cover_records_by_game_id[record.game_id] = record
                if record.title_key and record.title_key not in self.cover_records_by_title_key:
                    self.cover_records_by_title_key[record.title_key] = record
                if record.app_id > 0 and record.app_id not in self.cover_records_by_app_id:
                    self.cover_records_by_app_id[record.app_id] = record
                count += 1

        config.logger.info("已加载目录封面索引: total=%s file=%s", count, cover_index_path)

    def _resolve_cover_record(self, *, game_id: str, title: str, app_id: int) -> Optional[CatalogCoverRecord]:
        if game_id:
            hit = self.cover_records_by_game_id.get(game_id)
            if isinstance(hit, CatalogCoverRecord):
                return hit

        title_key = _normalize_catalog_title_key(title)
        if title_key:
            hit = self.cover_records_by_title_key.get(title_key)
            if isinstance(hit, CatalogCoverRecord):
                return hit

        app = _safe_int(app_id)
        if app > 0:
            hit = self.cover_records_by_app_id.get(app)
            if isinstance(hit, CatalogCoverRecord):
                return hit
        return None

    def load(self) -> None:
        """加载 CSV 到内存。"""
        self.entries = []
        self.entries_by_game_id = {}
        self.invalid_rows = 0
        self._load_cover_index()
        if not self.csv_path:
            config.logger.warning("未找到游戏目录 CSV 路径")
            return
        if not os.path.isfile(self.csv_path):
            config.logger.warning("游戏目录 CSV 不存在: %s", self.csv_path)
            return

        with open(self.csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    self.invalid_rows += 1
                    continue

                title = _normalize_title(row.get("title", ""))
                down_url = (row.get("down_url") or "").strip()
                if not title or not _is_valid_tianyi_url(down_url):
                    self.invalid_rows += 1
                    continue

                game_id = str(row.get("game_id", "")).strip()
                size_bytes = _safe_int(row.get("filesize_z", "0"))
                size_text = (row.get("list_filesize") or "").strip()
                if not size_text and size_bytes > 0:
                    size_text = _format_size(size_bytes)

                app_id = _safe_int(row.get("steam_appid", row.get("app_id", "0")))
                cover_record = self._resolve_cover_record(game_id=game_id, title=title, app_id=app_id)
                if cover_record and cover_record.app_id > 0:
                    app_id = cover_record.app_id

                entry = GameCatalogEntry(
                    game_id=game_id or title,
                    title=title,
                    category_parent=str(row.get("category_parent", "")).strip(),
                    categories=str(row.get("categories", "")).strip(),
                    down_url=down_url,
                    pwd=str(row.get("pwd", "")).strip(),
                    openpath=str(row.get("openpath", "")).strip(),
                    size_bytes=size_bytes,
                    size_text=size_text,
                    app_id=app_id,
                    cover_url=str(getattr(cover_record, "cover_url", "") or "").strip(),
                    square_cover_url=str(getattr(cover_record, "square_cover_url", "") or "").strip()
                    or _build_store_square_cover_url(app_id),
                    catalog_index=len(self.entries),
                )
                self.entries.append(entry)
                if entry.game_id:
                    self.entries_by_game_id[entry.game_id] = entry

        config.logger.info(
            "已加载游戏目录: total=%s invalid=%s file=%s",
            len(self.entries),
            self.invalid_rows,
            self.csv_path,
        )

    def summary(self) -> Dict[str, object]:
        """返回目录摘要。"""
        return {
            "path": self.csv_path,
            "cover_index_path": self.cover_index_path,
            "total": len(self.entries),
            "invalid": self.invalid_rows,
            "preview": [e.to_dict() for e in self.entries[:8]],
        }

    def _entry_matches_query(self, entry: GameCatalogEntry, query: str) -> bool:
        q = str(query or "").strip().lower()
        if not q:
            return True
        return (
            q in str(entry.title or "").lower()
            or q in str(entry.categories or "").lower()
            or q in str(entry.game_id or "").lower()
        )

    def _build_grouped_items(self, entries: List[GameCatalogEntry], sort_mode: str) -> List[Dict[str, object]]:
        grouped: Dict[str, List[GameCatalogEntry]] = {}
        group_order: Dict[str, int] = {}
        for entry in list(entries or []):
            group_key = _catalog_group_key(entry)
            if group_key not in grouped:
                grouped[group_key] = []
                group_order[group_key] = _safe_int(getattr(entry, "catalog_index", 0))
            grouped[group_key].append(entry)

        groups: List[Dict[str, object]] = []
        for group_key, variants_raw in grouped.items():
            variants = _sort_catalog_variants(variants_raw)
            representative = variants[0] if variants else None
            if representative is None:
                continue
            item = representative.to_dict()
            item["group_key"] = group_key
            item["variants"] = [variant.to_dict() for variant in variants]
            item["variant_count"] = len(variants)
            item["_sort_index"] = group_order.get(group_key, _safe_int(getattr(representative, "catalog_index", 0)))
            groups.append(item)

        if sort_mode == "size_desc":
            groups.sort(key=lambda item: (-_safe_int(item.get("size_bytes", 0)), _safe_int(item.get("_sort_index", 0))))
        elif sort_mode == "size_asc":
            groups.sort(key=lambda item: (_safe_int(item.get("size_bytes", 0)), _safe_int(item.get("_sort_index", 0))))
        elif sort_mode == "title":
            groups.sort(
                key=lambda item: (
                    _title_sort_key(str(item.get("title", "") or "")),
                    _safe_int(item.get("_sort_index", 0)),
                )
            )

        for item in groups:
            item.pop("_sort_index", None)
        return groups

    def _list_entries(
        self,
        *,
        include_switch: bool,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
        sort_mode: str = "default",
    ) -> Dict[str, object]:
        normalized_page = max(1, int(page))
        normalized_size = max(1, min(200, int(page_size)))

        if include_switch:
            base = [item for item in self.entries if self._is_switch_emulator_entry(item)]
        else:
            base = [item for item in self.entries if not self._is_switch_emulator_entry(item)]

        matched = [item for item in base if self._entry_matches_query(item, query)]
        groups = self._build_grouped_items(matched, sort_mode=sort_mode)

        start = (normalized_page - 1) * normalized_size
        end = start + normalized_size
        items = groups[start:end]

        return {
            "total": len(groups),
            "page": normalized_page,
            "page_size": normalized_size,
            "items": items,
        }

    def list(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
        sort_mode: str = "default",
    ) -> Dict[str, object]:
        """按关键词分页检索普通游戏目录。"""
        return self._list_entries(
            include_switch=False,
            query=query,
            page=page,
            page_size=page_size,
            sort_mode=sort_mode,
        )

    def list_switch(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
        sort_mode: str = "default",
    ) -> Dict[str, object]:
        """列出 Switch 模拟器资源（category_parent=527）。"""
        return self._list_entries(
            include_switch=True,
            query=query,
            page=page,
            page_size=page_size,
            sort_mode=sort_mode,
        )

    def get_by_game_id(self, game_id: str) -> Optional[GameCatalogEntry]:
        """按 game_id 查找条目。"""
        target = (game_id or "").strip()
        if not target:
            return None
        hit = self.entries_by_game_id.get(target)
        return hit if isinstance(hit, GameCatalogEntry) else None


def _format_size(size_bytes: int) -> str:
    """格式化字节大小显示。"""
    value = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"
