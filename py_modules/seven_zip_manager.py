# seven_zip_manager.py - 7z 解压运行时管理
#
# 该模块负责定位 7z 可执行文件并执行解压，支持进度回调。

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Callable, List, Optional, Sequence


class SevenZipError(RuntimeError):
    """7z 相关异常。"""


class SevenZipCancelledError(SevenZipError):
    """7z 解压被取消。"""


class SevenZipManager:
    """7z 解压管理器。"""

    def __init__(self, plugin_dir: str):
        self._plugin_dir = str(plugin_dir or "")

    @staticmethod
    def _diagnose_failure(output_tail: Sequence[str]) -> str:
        """根据 7z 输出内容推断常见失败原因（用于更友好提示）。"""
        lines = [str(line or "").strip() for line in list(output_tail or []) if str(line or "").strip()]
        if not lines:
            return ""

        joined = "\n".join(lines)
        lower = joined.lower()

        # 密码/加密
        if "wrong password" in lower or ("password" in lower and "wrong" in lower) or "encrypted" in lower:
            return "可能原因：压缩包需要密码或密码错误"

        # 空间不足/写入失败
        if "no space left on device" in lower or "not enough space" in lower or "write error" in lower:
            return "可能原因：磁盘空间不足或写入失败"

        # 权限问题
        if "permission denied" in lower:
            return "可能原因：权限不足，无法写入安装目录"

        # 分卷缺失/找不到文件
        if "no such file" in lower or "cannot open file" in lower or "can not open file" in lower or "cannot find" in lower:
            match = re.search(r"(?i)(?:can(?:not| not)|cannot)\s+open\s+file\s+'([^']+)'", joined)
            if match:
                missing = os.path.basename(match.group(1) or "").strip()
                if missing:
                    return f"可能原因：缺少分卷文件 {missing}（请确保同一组分卷全部下载完成且文件名未改动）"
            return "可能原因：分卷缺失或文件路径无效"

        # 压缩包损坏/不完整
        if (
            "unexpected end of archive" in lower
            or "data error" in lower
            or "crc failed" in lower
            or "headers error" in lower
            or "checksum error" in lower
        ):
            return "可能原因：压缩包损坏或下载不完整"

        # 不是压缩包/格式不支持
        if "is not archive" in lower or "cannot open the file as" in lower:
            return "可能原因：文件不是有效压缩包（可能下载到错误内容或下载不完整）"

        return ""

    def _resolve_binary_path(self) -> str:
        """优先解析插件内置 7z，可回退系统命令。"""
        env_path = (os.getenv("FREEDECK_7Z_BIN") or "").strip()
        if env_path and os.path.isfile(env_path):
            return env_path

        root = Path(self._plugin_dir)
        candidates: List[Path] = [
            root / "defaults" / "7z" / "linux-x86_64" / "7zz",
            root / "defaults" / "7z" / "linux-x64" / "7zz",
            root / "defaults" / "7z" / "7zz",
            root / "defaults" / "7z" / "7z",
            root / "defaults" / "7zz",
            root / "defaults" / "7z",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)

        for command in ("7zz", "7zr", "7z"):
            system_path = shutil.which(command)
            if system_path:
                return system_path

        raise SevenZipError("解压组件不可用，未找到内置 7z 或系统 7z 命令")

    def extract_archive(
        self,
        archive_path: str,
        output_dir: str,
        progress_cb: Optional[Callable[[float], None]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> None:
        """执行解压流程。"""
        archive = os.path.realpath(os.path.expanduser((archive_path or "").strip()))
        target = os.path.realpath(os.path.expanduser((output_dir or "").strip()))
        if not archive or not os.path.isfile(archive):
            raise SevenZipError(f"待解压文件不存在: {archive_path}")
        if not target:
            raise SevenZipError("安装目录无效")
        if cancel_event is not None and cancel_event.is_set():
            raise SevenZipCancelledError("解压已取消")
        os.makedirs(target, exist_ok=True)

        binary = self._resolve_binary_path()
        try:
            mode = os.stat(binary).st_mode
            if mode & 0o111 == 0:
                os.chmod(binary, mode | 0o755)
        except Exception:
            # 权限修复失败时继续尝试执行，保留真实错误给调用方。
            pass

        args = [
            binary,
            "x",
            "-y",
            f"-o{target}",
            archive,
            "-bsp1",
        ]

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
            raise SevenZipError(f"启动 7z 失败: {exc}") from exc

        if cancel_event is not None:
            def _watch_cancel() -> None:
                while True:
                    try:
                        if process.poll() is not None:
                            return
                    except Exception:
                        return
                    try:
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

            threading.Thread(target=_watch_cancel, name="freedeck_7z_cancel", daemon=True).start()

        percent_re = re.compile(r"(\d{1,3})%")
        output_tail: List[str] = []
        max_tail_lines = 40
        if process.stdout is not None:
            for line in process.stdout:
                if cancel_event is not None and cancel_event.is_set():
                    break
                text = line.strip()
                if text:
                    output_tail.append(text)
                    if len(output_tail) > max_tail_lines:
                        output_tail.pop(0)
                match = percent_re.search(text)
                if match and progress_cb:
                    try:
                        progress_cb(float(match.group(1)))
                    except Exception:
                        pass

        return_code = process.wait()
        if cancel_event is not None and cancel_event.is_set():
            raise SevenZipCancelledError("解压已取消")
        if return_code != 0:
            hint = " | ".join(output_tail[-10:]) if output_tail else "no output"
            diagnosis = self._diagnose_failure(output_tail)
            extra = f"，{diagnosis}" if diagnosis else ""
            raise SevenZipError(f"7z 解压失败，exit={return_code}{extra}，诊断={hint}")

        if progress_cb:
            try:
                progress_cb(100.0)
            except Exception:
                pass

    def create_archive(
        self,
        archive_path: str,
        source_paths: Sequence[str],
        working_dir: str = "",
        progress_cb: Optional[Callable[[float], None]] = None,
    ) -> None:
        """执行压缩流程。"""
        archive = os.path.realpath(os.path.expanduser((archive_path or "").strip()))
        if not archive:
            raise SevenZipError("压缩包路径无效")

        sources: List[str] = []
        for item in list(source_paths or []):
            path = os.path.realpath(os.path.expanduser(str(item or "").strip()))
            if not path:
                continue
            if not os.path.exists(path):
                raise SevenZipError(f"待压缩路径不存在: {item}")
            sources.append(path)
        if not sources:
            raise SevenZipError("缺少待压缩路径")

        binary = self._resolve_binary_path()
        try:
            mode = os.stat(binary).st_mode
            if mode & 0o111 == 0:
                os.chmod(binary, mode | 0o755)
        except Exception:
            pass

        if working_dir:
            cwd = os.path.realpath(os.path.expanduser(str(working_dir).strip()))
        else:
            first = sources[0]
            cwd = first if os.path.isdir(first) else os.path.dirname(first)

        if not cwd or not os.path.isdir(cwd):
            raise SevenZipError("压缩工作目录无效")

        rel_sources: List[str] = []
        for source in sources:
            try:
                rel = os.path.relpath(source, cwd)
            except Exception:
                rel = source
            rel_sources.append(rel)

        archive_dir = os.path.dirname(archive)
        if archive_dir:
            os.makedirs(archive_dir, exist_ok=True)

        args = [
            binary,
            "a",
            "-t7z",
            "-y",
            archive,
            *rel_sources,
            "-bsp1",
        ]

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
            raise SevenZipError(f"启动 7z 失败: {exc}") from exc

        percent_re = re.compile(r"(\d{1,3})%")
        output_tail: List[str] = []
        if process.stdout is not None:
            for line in process.stdout:
                text = line.strip()
                if text:
                    output_tail.append(text)
                    if len(output_tail) > 12:
                        output_tail.pop(0)
                match = percent_re.search(text)
                if match and progress_cb:
                    try:
                        progress_cb(float(match.group(1)))
                    except Exception:
                        pass

        return_code = process.wait()
        if return_code != 0:
            hint = " | ".join(output_tail[-6:]) if output_tail else "no output"
            raise SevenZipError(f"7z 压缩失败，exit={return_code}，诊断={hint}")

        if progress_cb:
            try:
                progress_cb(100.0)
            except Exception:
                pass
