# main.py - Freedeck 插件入口
#
# 只保留天翼下载与本地网页服务所需能力。

import asyncio
import os
import pwd
import shutil
import subprocess

import decky

import config
import server_manager
from tianyi_service import LocalWebNotReadyError, TianyiService


class Plugin:
    """Freedeck 主插件类。"""

    # 本地 HTTP 服务状态
    server_running = False
    server_host = config.DEFAULT_SERVER_HOST
    server_port = config.DEFAULT_SERVER_PORT
    app = None
    runner = None
    site = None

    # 目录状态
    downloads_dir = config.DOWNLOADS_DIR
    decky_send_dir = config.DECKY_SEND_DIR

    # 业务服务
    tianyi_service = None

    # 设置键
    SETTINGS_KEY = config.SETTINGS_KEY
    SETTING_RUNNING = config.SETTING_RUNNING
    SETTING_PORT = config.SETTING_PORT
    SETTING_DOWNLOAD_DIR = config.SETTING_DOWNLOAD_DIR

    async def _main(self):
        """插件主循环。"""
        decky.logger.info("Freedeck plugin initialized")
        config.logger.info("Freedeck plugin initialized")

        os.makedirs(self.decky_send_dir, exist_ok=True)
        try:
            os.makedirs(self.downloads_dir, exist_ok=True)
        except Exception as exc:
            decky.logger.warning("Ensure default download directory failed: %s", exc)

        await server_manager.load_settings(self)
        try:
            os.makedirs(self.downloads_dir, exist_ok=True)
        except Exception as exc:
            decky.logger.warning("Ensure configured download directory failed: %s", exc)
            try:
                fallback_base = "/home/deck" if os.path.isdir("/home/deck") else os.path.expanduser("~")
                fallback_dir = os.path.realpath(os.path.join(fallback_base, "Game"))
                await self.set_download_dir(fallback_dir)
            except Exception as inner_exc:
                decky.logger.error("Fallback download directory setup failed: %s", inner_exc)

        self.tianyi_service = TianyiService(self)
        await self.tianyi_service.initialize()

        # 按上次保存的状态恢复本地服务。
        if bool(self.server_running):
            start_result = await server_manager.start_server(self, self.server_port)
            if start_result.get("status") != "success":
                decky.logger.error(
                    "Failed to restore local server: %s",
                    start_result.get("message", "unknown error"),
                )
                self.server_running = False
                await server_manager.save_settings(self)

        while True:
            await asyncio.sleep(60)

    async def _unload(self):
        """插件卸载时清理服务。"""
        decky.logger.info("Unloading Freedeck plugin")

        try:
            await server_manager.stop_server(self)
        except Exception as exc:
            decky.logger.error(f"Stop server failed: {exc}")

        if self.tianyi_service is not None:
            try:
                await self.tianyi_service.shutdown()
            except Exception as exc:
                decky.logger.error(f"Stop Tianyi service failed: {exc}")
            self.tianyi_service = None

    async def _uninstall(self):
        """插件卸载钩子。"""
        decky.logger.info("Uninstalling Freedeck plugin")

    async def frontend_debug_log(self, payload=None, message: str = "", details=None) -> dict:
        """前端调试日志（用于定位 UI 注入、菜单注入等问题）。"""
        try:
            if isinstance(payload, dict):
                message = str(payload.get("message", message) or "")
                details = payload.get("details", details)

            safe_details = details
            try:
                if safe_details is not None and not isinstance(
                    safe_details,
                    (str, int, float, bool, list, dict),
                ):
                    safe_details = str(safe_details)
            except Exception:
                safe_details = str(details)

            decky.logger.info("[frontend] %s | %s", message, safe_details)
            config.logger.info("[frontend] %s | %s", message, safe_details)
            return {"status": "success", "message": "ok"}
        except Exception as exc:
            try:
                decky.logger.error("frontend_debug_log failed: %s", exc)
            except Exception:
                pass
            return {"status": "error", "message": str(exc)}

    def _ensure_keyboard_bridge_state(self) -> None:
        """确保网页键盘桥接状态已初始化。"""
        if getattr(self, "_keyboard_bridge_lock", None) is None:
            self._keyboard_bridge_lock = asyncio.Lock()
        if not isinstance(getattr(self, "_keyboard_bridge_requests", None), dict):
            self._keyboard_bridge_requests = {}
        if not isinstance(getattr(self, "_keyboard_bridge_waiters", None), dict):
            self._keyboard_bridge_waiters = {}
        if not isinstance(getattr(self, "_keyboard_bridge_order", None), list):
            self._keyboard_bridge_order = []
        if not isinstance(getattr(self, "_keyboard_bridge_seq", None), int):
            self._keyboard_bridge_seq = 0

    async def _create_keyboard_bridge_request(self, payload=None) -> tuple[dict, asyncio.Future]:
        """创建一个待处理的键盘输入请求。"""
        self._ensure_keyboard_bridge_state()
        body = payload if isinstance(payload, dict) else {}
        loop = asyncio.get_running_loop()

        title = str(body.get("title", "") or "").strip() or "输入"
        placeholder = str(body.get("placeholder", "") or "")
        value = str(body.get("value", "") or "")
        password = bool(body.get("password", False))
        field = str(body.get("field", "") or "").strip()
        source = str(body.get("source", "") or "").strip()

        async with self._keyboard_bridge_lock:
            self._keyboard_bridge_seq += 1
            request_id = f"kbd_http_{self._keyboard_bridge_seq}_{int(loop.time() * 1000)}"
            request = {
                "request_id": request_id,
                "title": title,
                "placeholder": placeholder,
                "value": value,
                "password": password,
                "field": field,
                "source": source,
                "created_at_ms": int(loop.time() * 1000),
                "claimed": False,
            }
            future = loop.create_future()
            self._keyboard_bridge_requests[request_id] = request
            self._keyboard_bridge_waiters[request_id] = future
            self._keyboard_bridge_order.append(request_id)

        try:
            config.logger.info("Keyboard bridge request created request_id=%s field=%s source=%s", request_id, field, source)
        except Exception:
            pass
        return dict(request), future

    async def _wait_keyboard_bridge_result(self, request_id: str, timeout_seconds: float = 20.0) -> dict:
        """等待键盘桥接请求返回。"""
        self._ensure_keyboard_bridge_state()
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return {"ok": False, "value": "", "reason": "missing_request_id"}

        async with self._keyboard_bridge_lock:
            future = self._keyboard_bridge_waiters.get(normalized_request_id)
            request = dict(self._keyboard_bridge_requests.get(normalized_request_id) or {})

        original_value = str(request.get("value", "") or "")
        if future is None:
            return {"ok": False, "value": original_value, "reason": "missing_future"}

        try:
            result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout_seconds)
            if isinstance(result, dict):
                return result
            return {"ok": False, "value": original_value, "reason": "invalid_result"}
        except asyncio.TimeoutError:
            try:
                config.logger.warning("Keyboard bridge request timeout request_id=%s", normalized_request_id)
            except Exception:
                pass
            async with self._keyboard_bridge_lock:
                future = self._keyboard_bridge_waiters.pop(normalized_request_id, None)
                self._keyboard_bridge_requests.pop(normalized_request_id, None)
                try:
                    self._keyboard_bridge_order.remove(normalized_request_id)
                except ValueError:
                    pass
                if future is not None and not future.done():
                    future.cancel()
            return {"ok": False, "value": original_value, "reason": "timeout"}

    async def request_tianyi_keyboard_input(self, payload=None, timeout_seconds: float = 20.0) -> dict:
        """供网页端调用的键盘输入桥接。"""
        request, _future = await self._create_keyboard_bridge_request(payload)
        result = await self._wait_keyboard_bridge_result(request.get("request_id", ""), timeout_seconds=timeout_seconds)
        result["request_id"] = str(request.get("request_id", "") or "")
        return result

    async def poll_tianyi_keyboard_bridge_request(self) -> dict:
        """供 Decky 前端轮询待处理的网页键盘请求。"""
        try:
            self._ensure_keyboard_bridge_state()
            async with self._keyboard_bridge_lock:
                for request_id in list(self._keyboard_bridge_order):
                    request = self._keyboard_bridge_requests.get(request_id)
                    if not isinstance(request, dict):
                        continue
                    if bool(request.get("claimed")):
                        continue
                    request["claimed"] = True
                    request["claimed_at_ms"] = int(asyncio.get_running_loop().time() * 1000)
                    try:
                        config.logger.info(
                            "Keyboard bridge request claimed request_id=%s field=%s source=%s",
                            request_id,
                            str(request.get("field", "") or ""),
                            str(request.get("source", "") or ""),
                        )
                    except Exception:
                        pass
                    return {"status": "success", "data": {"request": dict(request)}}
            return {"status": "success", "data": {"request": None}}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {"request": None}}

    async def resolve_tianyi_keyboard_bridge_request(
        self,
        payload=None,
        request_id: str = "",
        ok: bool = False,
        value: str = "",
        reason: str = "",
    ) -> dict:
        """由 Decky 前端回写网页键盘请求结果。"""
        try:
            self._ensure_keyboard_bridge_state()
            if isinstance(payload, dict):
                request_id = str(payload.get("request_id", request_id) or "")
                if "ok" in payload:
                    ok = bool(payload.get("ok"))
                value = str(payload.get("value", value) or "")
                reason = str(payload.get("reason", reason) or "")

            normalized_request_id = request_id.strip()
            if not normalized_request_id:
                return {"status": "error", "message": "missing request_id", "data": {}}

            async with self._keyboard_bridge_lock:
                request = dict(self._keyboard_bridge_requests.get(normalized_request_id) or {})
                future = self._keyboard_bridge_waiters.pop(normalized_request_id, None)
                self._keyboard_bridge_requests.pop(normalized_request_id, None)
                try:
                    self._keyboard_bridge_order.remove(normalized_request_id)
                except ValueError:
                    pass

            original_value = str(request.get("value", "") or "")
            result = {
                "request_id": normalized_request_id,
                "ok": bool(ok),
                "value": value if bool(ok) else original_value,
                "reason": reason.strip() or ("ok" if bool(ok) else "cancel"),
            }

            if future is not None and not future.done():
                future.set_result(result)

            try:
                config.logger.info(
                    "Keyboard bridge request resolved request_id=%s ok=%s reason=%s",
                    normalized_request_id,
                    bool(ok),
                    result["reason"],
                )
            except Exception:
                pass
            return {"status": "success", "data": result}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    # ------------------------- 本地服务接口 -------------------------

    async def start_server(self, port: int = config.DEFAULT_SERVER_PORT) -> dict:
        """启动本地网页服务。"""
        return await server_manager.start_server(self, port)

    async def stop_server(self) -> dict:
        """停止本地网页服务。"""
        return await server_manager.stop_server(self)

    async def get_server_status(self) -> dict:
        """获取本地网页服务状态。"""
        return await server_manager.get_server_status(self)

    async def set_download_dir(self, path: str) -> dict:
        """更新下载目录并持久化。"""
        try:
            if not isinstance(path, str) or not path.strip():
                return {"status": "error", "message": "无效的目录路径"}

            resolved = os.path.realpath(os.path.expanduser(path.strip()))
            if not resolved:
                return {"status": "error", "message": "无效的目录路径"}
            if os.path.exists(resolved) and not os.path.isdir(resolved):
                return {"status": "error", "message": "目标路径不是文件夹"}

            os.makedirs(resolved, exist_ok=True)
            self.downloads_dir = resolved
            await server_manager.save_settings(self)
            return {"status": "success", "path": self.downloads_dir}
        except Exception as exc:
            return {"status": "error", "message": str(exc)}

    async def list_media_mounts(self, payload=None) -> dict:
        """列出可用的外接存储挂载点（用于一键切换下载/安装目录到 SD 卡等）。"""
        try:
            config.logger.info("Decky callable: list_media_mounts")

            roots = []
            for root in ("/run/media", "/run/meida"):
                if os.path.isdir(root):
                    roots.append(os.path.realpath(root))

            def unescape_mount_path(path: str) -> str:
                # /proc/mounts uses octal escapes for spaces etc.
                return (
                    str(path or "")
                    .replace("\\040", " ")
                    .replace("\\011", "\t")
                    .replace("\\012", "\n")
                    .replace("\\134", "\\")
                )

            def read_mount_points() -> set:
                points = set()
                try:
                    with open("/proc/mounts", "r", encoding="utf-8", errors="ignore") as handle:
                        for line in handle:
                            parts = line.split()
                            if len(parts) < 2:
                                continue
                            points.add(os.path.realpath(unescape_mount_path(parts[1])))
                except Exception:
                    return points
                return points

            def scan_candidates(base: str):
                try:
                    with os.scandir(base) as iterator:
                        for entry in iterator:
                            if not entry.is_dir(follow_symlinks=False):
                                continue
                            yield entry.path
                            try:
                                with os.scandir(entry.path) as sub_iter:
                                    for sub in sub_iter:
                                        if sub.is_dir(follow_symlinks=False):
                                            yield sub.path
                            except OSError:
                                continue
                except OSError:
                    return

            mount_points = read_mount_points()
            candidates = set()
            for point in mount_points:
                for root in roots:
                    prefix = root.rstrip("/") + "/"
                    if not point or point == root:
                        continue
                    if point.startswith(prefix):
                        candidates.add(point)
                        break

            if not candidates:
                # 没有解析到挂载点时回退到扫描目录（兼容部分系统挂载信息不可读的情况）。
                for root in roots:
                    for candidate in scan_candidates(root):
                        resolved = os.path.realpath(candidate)
                        if resolved:
                            candidates.add(resolved)

            mounts = []
            seen = set()
            for candidate in sorted(candidates):
                resolved = os.path.realpath(candidate)
                if not resolved or resolved in seen:
                    continue
                seen.add(resolved)

                try:
                    if not os.path.isdir(resolved):
                        continue
                except OSError:
                    continue

                label = os.path.basename(resolved) or resolved
                free_bytes = 0
                total_bytes = 0
                try:
                    usage = shutil.disk_usage(resolved)
                    free_bytes = int(getattr(usage, "free", 0) or 0)
                    total_bytes = int(getattr(usage, "total", 0) or 0)
                except Exception:
                    pass

                mounts.append(
                    {
                        "path": resolved,
                        "label": label,
                        "free_bytes": free_bytes,
                        "total_bytes": total_bytes,
                    }
                )

            mounts.sort(key=lambda item: str(item.get("label") or item.get("path") or ""))
            config.logger.info("list_media_mounts result count=%s", len(mounts))
            return {"status": "success", "data": {"mounts": mounts}}
        except Exception as exc:
            try:
                config.logger.exception("list_media_mounts failed: %s", exc)
            except Exception:
                pass
            return {"status": "error", "message": str(exc), "data": {"mounts": []}}

    # ------------------------- 天翼业务接口 -------------------------

    def _get_tianyi_service(self) -> TianyiService:
        """获取已初始化的天翼服务实例。"""
        if self.tianyi_service is None:
            raise RuntimeError("天翼服务未初始化")
        return self.tianyi_service

    async def get_tianyi_panel_state(
        self,
        payload=None,
        poll_mode: str = "",
        visible: bool = True,
        has_focus: bool = True,
    ) -> dict:
        """Decky 主面板状态。"""
        try:
            request_context = {}
            if isinstance(payload, dict):
                request_context.update(payload)
            if poll_mode and "poll_mode" not in request_context:
                request_context["poll_mode"] = poll_mode
            if "visible" not in request_context:
                request_context["visible"] = bool(visible)
            if "has_focus" not in request_context:
                request_context["has_focus"] = bool(has_focus)
            data = await self._get_tianyi_service().get_panel_state(request_context=request_context)
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_tianyi_library_url(self) -> dict:
        """本地游戏库页面地址。"""
        try:
            url = await self._get_tianyi_service().get_library_url()
            return {"status": "success", "url": url}
        except LocalWebNotReadyError as exc:
            return {
                "status": "error",
                "message": str(exc),
                "url": "",
                "reason": exc.reason,
                "diagnostics": exc.diagnostics,
            }
        except Exception as exc:
            return {"status": "error", "message": str(exc), "url": ""}

    async def get_tianyi_login_url(self) -> dict:
        """天翼登录页地址。"""
        try:
            url = await self._get_tianyi_service().get_login_url()
            return {"status": "success", "url": url}
        except LocalWebNotReadyError as exc:
            return {
                "status": "error",
                "message": str(exc),
                "url": "",
                "reason": exc.reason,
                "diagnostics": exc.diagnostics,
            }
        except Exception as exc:
            return {"status": "error", "message": str(exc), "url": ""}

    async def clear_tianyi_login(self) -> dict:
        """清理天翼登录态。"""
        try:
            data = await self._get_tianyi_service().clear_login()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_baidu_login_url(self) -> dict:
        """百度网盘登录页地址。"""
        try:
            url = self._get_tianyi_service().get_baidu_cloud_login_url()
            return {"status": "success", "url": url}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "url": ""}

    async def start_baidu_login_capture(self, payload=None, timeout_seconds: int = 240) -> dict:
        """启动百度网盘登录态自动采集。"""
        try:
            if isinstance(payload, dict) and "timeout_seconds" in payload:
                timeout_seconds = int(payload.get("timeout_seconds", timeout_seconds))
            data = await self._get_tianyi_service().start_baidu_login_capture(timeout_seconds=timeout_seconds)
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def stop_baidu_login_capture(self) -> dict:
        """停止百度网盘登录态自动采集。"""
        try:
            data = await self._get_tianyi_service().stop_baidu_login_capture()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def clear_baidu_login(self) -> dict:
        """清理百度网盘登录态。"""
        try:
            data = await self._get_tianyi_service().clear_baidu_login()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_ctfile_login_guide_url(self) -> dict:
        """CTFile 登录引导页地址（用于获取 session_id token）。"""
        try:
            url = self._get_tianyi_service().get_ctfile_login_guide_url()
            return {"status": "success", "url": url}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "url": ""}

    async def set_ctfile_token(self, payload=None, token: str = "") -> dict:
        """保存 CTFile token（session_id）。"""
        try:
            if isinstance(payload, dict) and "token" in payload:
                token = str(payload.get("token", token) or "")
            data = await self._get_tianyi_service().set_ctfile_token(token)
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def clear_ctfile_token(self) -> dict:
        """清理 CTFile token。"""
        try:
            data = await self._get_tianyi_service().clear_ctfile_token()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def set_tianyi_settings(
        self,
        payload=None,
        download_dir: str = "",
        install_dir: str = "",
        emulator_dir: str = "",
        split_count: int = 16,
        aria2_fast_mode=None,
        force_ipv4=None,
        auto_switch_line=None,
        page_size: int = 50,
        auto_delete_package: bool = False,
        auto_install: bool = True,
        lsfg_enabled=None,
        show_playtime_widget=None,
        cloud_save_auto_upload=None,
        steamgriddb_enabled=None,
        steamgriddb_api_key=None,
    ) -> dict:
        """保存天翼下载设置。"""
        try:
            emulator_dir_patch = None
            if isinstance(payload, dict):
                download_dir = str(payload.get("download_dir", download_dir))
                install_dir = str(payload.get("install_dir", install_dir))
                if "emulator_dir" in payload:
                    emulator_dir_patch = str(payload.get("emulator_dir", emulator_dir))
                split_count = int(payload.get("split_count", split_count))
                if "aria2_fast_mode" in payload:
                    aria2_fast_mode = bool(payload.get("aria2_fast_mode"))
                if "force_ipv4" in payload:
                    force_ipv4 = bool(payload.get("force_ipv4"))
                if "auto_switch_line" in payload:
                    auto_switch_line = bool(payload.get("auto_switch_line"))
                page_size = int(payload.get("page_size", page_size))
                auto_delete_package = bool(payload.get("auto_delete_package", auto_delete_package))
                auto_install = bool(payload.get("auto_install", auto_install))
                if "lsfg_enabled" in payload:
                    lsfg_enabled = bool(payload.get("lsfg_enabled"))
                if "show_playtime_widget" in payload:
                    show_playtime_widget = bool(payload.get("show_playtime_widget"))
                if "cloud_save_auto_upload" in payload:
                    cloud_save_auto_upload = bool(payload.get("cloud_save_auto_upload"))
                if "steamgriddb_enabled" in payload:
                    steamgriddb_enabled = bool(payload.get("steamgriddb_enabled"))
                if "steamgriddb_api_key" in payload:
                    steamgriddb_api_key = str(payload.get("steamgriddb_api_key", "") or "")
            elif isinstance(payload, str) and not download_dir:
                download_dir = payload
            if emulator_dir_patch is None and str(emulator_dir or "").strip():
                emulator_dir_patch = str(emulator_dir or "")

            data = await self._get_tianyi_service().update_settings(
                download_dir=download_dir,
                install_dir=install_dir,
                emulator_dir=emulator_dir_patch,
                split_count=split_count,
                aria2_fast_mode=aria2_fast_mode,
                force_ipv4=force_ipv4,
                auto_switch_line=auto_switch_line,
                page_size=page_size,
                auto_delete_package=auto_delete_package,
                auto_install=auto_install,
                lsfg_enabled=lsfg_enabled,
                show_playtime_widget=show_playtime_widget,
                cloud_save_auto_upload=cloud_save_auto_upload,
                steamgriddb_enabled=steamgriddb_enabled,
                steamgriddb_api_key=steamgriddb_api_key,
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def cancel_tianyi_task(self, payload=None, task_id: str = "") -> dict:
        """取消下载任务并从列表移除。"""
        try:
            config.logger.info("Decky callable: cancel_tianyi_task")
            delete_files = True
            if isinstance(payload, dict):
                task_id = str(payload.get("task_id", task_id))
                if "delete_files" in payload:
                    delete_files = bool(payload.get("delete_files"))
            data = await self._get_tianyi_service().cancel_task(task_id, delete_files=delete_files)
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable cancel_tianyi_task cancelled")
            return {"status": "error", "message": "取消操作被中断", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable cancel_tianyi_task failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def dismiss_tianyi_task(self, payload=None, task_id: str = "") -> dict:
        """清除已结束任务。"""
        try:
            config.logger.info("Decky callable: dismiss_tianyi_task")
            delete_files = False
            if isinstance(payload, dict):
                task_id = str(payload.get("task_id", task_id))
                if "delete_files" in payload:
                    delete_files = bool(payload.get("delete_files"))
            data = await self._get_tianyi_service().dismiss_task(task_id, delete_files=delete_files)
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable dismiss_tianyi_task cancelled")
            return {"status": "error", "message": "清除操作被中断", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable dismiss_tianyi_task failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def cancel_tianyi_install(self, payload=None, task_id: str = "") -> dict:
        """取消安装流程（解压/导入），用于面板终止安装进度。"""
        try:
            config.logger.info("Decky callable: cancel_tianyi_install")
            if isinstance(payload, dict):
                task_id = str(payload.get("task_id", task_id))
            data = await self._get_tianyi_service().cancel_install(task_id)
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable cancel_tianyi_install cancelled")
            return {"status": "error", "message": "取消安装操作被中断", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable cancel_tianyi_install failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def uninstall_tianyi_installed_game(
        self,
        payload=None,
        game_id: str = "",
        install_path: str = "",
        delete_files: bool = True,
        delete_proton_files: bool = False,
    ) -> dict:
        """卸载已安装游戏并移除记录。"""
        try:
            if isinstance(payload, dict):
                game_id = str(payload.get("game_id", game_id))
                install_path = str(payload.get("install_path", install_path))
                if "delete_files" in payload:
                    delete_files = bool(payload.get("delete_files"))
                if "delete_proton_files" in payload:
                    delete_proton_files = bool(payload.get("delete_proton_files"))
            data = await self._get_tianyi_service().uninstall_installed_game(
                game_id=game_id,
                install_path=install_path,
                delete_files=delete_files,
                delete_proton_files=delete_proton_files,
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_missing_steam_imports(self) -> dict:
        """列出已安装但未正确加入 Steam 的游戏。"""
        try:
            data = await self._get_tianyi_service().list_missing_steam_imports()
            return {"status": "success", "data": data}
        except Exception as exc:
            config.logger.exception("Decky callable list_tianyi_missing_steam_imports failed")
            return {"status": "error", "message": str(exc), "data": {}}

    async def reimport_tianyi_missing_steam_imports(self, payload=None, game_ids=None) -> dict:
        """将已安装但未正确加入 Steam 的游戏重新导入 Steam。"""
        try:
            if isinstance(payload, dict):
                game_ids = payload.get("game_ids", game_ids)
            data = await self._get_tianyi_service().reimport_missing_steam_imports(
                game_ids=[str(item) for item in list(game_ids or []) if str(item).strip()],
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            config.logger.exception("Decky callable reimport_tianyi_missing_steam_imports failed")
            return {"status": "error", "message": str(exc), "data": {}}

    async def start_tianyi_cloud_save_upload(self) -> dict:
        """启动云存档上传任务。"""
        try:
            data = await self._get_tianyi_service().start_cloud_save_upload()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_tianyi_cloud_save_upload_status(self) -> dict:
        """获取云存档上传任务状态。"""
        try:
            data = await self._get_tianyi_service().get_cloud_save_upload_status()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_cloud_save_restore_options(self) -> dict:
        """列出可恢复云存档版本。"""
        try:
            data = await self._get_tianyi_service().list_cloud_save_restore_options()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_cloud_save_restore_entries(
        self,
        payload=None,
        game_id: str = "",
        game_key: str = "",
        game_title: str = "",
        version_name: str = "",
    ) -> dict:
        """读取指定版本可选存档项。"""
        try:
            if isinstance(payload, dict):
                game_id = str(payload.get("game_id", game_id))
                game_key = str(payload.get("game_key", game_key))
                game_title = str(payload.get("game_title", game_title))
                version_name = str(payload.get("version_name", version_name))
            data = await self._get_tianyi_service().list_cloud_save_restore_entries(
                game_id=game_id,
                game_key=game_key,
                game_title=game_title,
                version_name=version_name,
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def plan_tianyi_cloud_save_restore(
        self,
        payload=None,
        game_id: str = "",
        game_key: str = "",
        game_title: str = "",
        version_name: str = "",
        selected_entry_ids=None,
        target_dir: str = "",
    ) -> dict:
        """生成云存档恢复计划（冲突探测）。"""
        try:
            if isinstance(payload, dict):
                game_id = str(payload.get("game_id", game_id))
                game_key = str(payload.get("game_key", game_key))
                game_title = str(payload.get("game_title", game_title))
                version_name = str(payload.get("version_name", version_name))
                target_dir = str(payload.get("target_dir", target_dir))
                selected_entry_ids = payload.get("selected_entry_ids", selected_entry_ids)
            rows = selected_entry_ids if isinstance(selected_entry_ids, list) else []
            data = await self._get_tianyi_service().plan_cloud_save_restore(
                game_id=game_id,
                game_key=game_key,
                game_title=game_title,
                version_name=version_name,
                selected_entry_ids=[str(item) for item in rows if str(item).strip()],
                target_dir=target_dir,
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def apply_tianyi_cloud_save_restore(
        self,
        payload=None,
        plan_id: str = "",
        confirm_overwrite: bool = False,
    ) -> dict:
        """执行云存档恢复计划。"""
        try:
            if isinstance(payload, dict):
                plan_id = str(payload.get("plan_id", plan_id))
                confirm_overwrite = bool(payload.get("confirm_overwrite", confirm_overwrite))
            data = await self._get_tianyi_service().apply_cloud_save_restore(
                plan_id=plan_id,
                confirm_overwrite=confirm_overwrite,
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_tianyi_cloud_save_restore_status(self) -> dict:
        """获取云存档恢复任务状态。"""
        try:
            data = await self._get_tianyi_service().get_cloud_save_restore_status()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_runtime_repair_candidates(self) -> dict:
        """列出可用于运行库修复的已安装 PC 游戏。"""
        try:
            data = await self._get_tianyi_service().list_runtime_repair_candidates()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_runtime_repair_packages(self) -> dict:
        """列出支持的运行库包。"""
        try:
            data = await self._get_tianyi_service().list_runtime_repair_packages()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def start_tianyi_runtime_repair(self, payload=None, game_ids=None, package_ids=None) -> dict:
        """启动运行库修复任务。"""
        try:
            if isinstance(payload, dict):
                game_ids = payload.get("game_ids", game_ids)
                package_ids = payload.get("package_ids", package_ids)
            data = await self._get_tianyi_service().start_runtime_repair(
                game_ids=[str(item) for item in list(game_ids or []) if str(item).strip()],
                package_ids=[str(item) for item in list(package_ids or []) if str(item).strip()],
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_tianyi_runtime_repair_status(self) -> dict:
        """获取运行库修复任务状态。"""
        try:
            data = await self._get_tianyi_service().get_runtime_repair_status()
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def record_tianyi_game_action(
        self,
        payload=None,
        phase: str = "",
        app_id: str = "",
        action_name: str = "",
    ) -> dict:
        """记录 Steam 游戏启动/退出事件（用于游玩时长统计）。"""
        try:
            if isinstance(payload, dict):
                phase = str(payload.get("phase", phase))
                app_id = str(payload.get("app_id", app_id))
                action_name = str(payload.get("action_name", action_name))
            data = await self._get_tianyi_service().record_game_action(
                phase=phase,
                app_id=app_id,
                action_name=action_name,
            )
            return {"status": "success", "data": data}
        except Exception as exc:
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_tianyi_library_game_time_stats(
        self,
        payload=None,
        app_id: str = "",
        title: str = "",
    ) -> dict:
        """获取 Steam 库页面游戏时长（我的游玩/最后运行时间）。"""
        try:
            if isinstance(payload, dict):
                app_id = str(payload.get("app_id", app_id))
                title = str(payload.get("title", title))
            config.logger.info(
                "Decky callable: get_tianyi_library_game_time_stats app_id=%s title=%s",
                str(app_id or "").strip(),
                (str(title or "").strip()[:80]),
            )
            data = await self._get_tianyi_service().get_library_game_time_stats(
                app_id=app_id,
                title=title,
            )
            try:
                config.logger.info(
                    "get_tianyi_library_game_time_stats result managed=%s reason=%s app_id=%s",
                    bool(data.get("managed")),
                    str(data.get("reason", "") or ""),
                    str(data.get("app_id", "") or ""),
                )
            except Exception:
                pass
            return {"status": "success", "data": data}
        except Exception as exc:
            config.logger.exception("get_tianyi_library_game_time_stats failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_tianyi_catalog_version(self) -> dict:
        """获取当前游戏目录 CSV 版本日期。"""
        try:
            config.logger.info("Decky callable: get_tianyi_catalog_version")
            data = await self._get_tianyi_service().get_catalog_version()
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable get_tianyi_catalog_version cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable get_tianyi_catalog_version failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def update_tianyi_catalog(self) -> dict:
        """检查并更新游戏目录 CSV（从 GitHub 获取）。"""
        try:
            config.logger.info("Decky callable: update_tianyi_catalog")
            data = await self._get_tianyi_service().update_catalog_from_github()
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable update_tianyi_catalog cancelled")
            return {"status": "error", "message": "更新操作被中断", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable update_tianyi_catalog failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_catalog(
        self,
        payload=None,
        query: str = "",
        page: int = 1,
        page_size: int = 0,
        sort_mode: str = "default",
    ) -> dict:
        """读取普通游戏目录分页。"""
        try:
            config.logger.info("Decky callable: list_tianyi_catalog")
            if isinstance(payload, dict):
                query = str(payload.get("query", query) or "")
                page = int(payload.get("page", page) or page or 1)
                page_size = int(payload.get("page_size", page_size) or page_size or 0)
                sort_mode = str(payload.get("sort_mode", sort_mode) or sort_mode or "default")
            data = await self._get_tianyi_service().list_catalog(
                query=query,
                page=page,
                page_size=page_size,
                sort_mode=sort_mode,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable list_tianyi_catalog cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable list_tianyi_catalog failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def list_tianyi_switch_catalog(
        self,
        payload=None,
        query: str = "",
        page: int = 1,
        page_size: int = 0,
        sort_mode: str = "default",
    ) -> dict:
        """读取 Switch 资源目录分页。"""
        try:
            config.logger.info("Decky callable: list_tianyi_switch_catalog")
            if isinstance(payload, dict):
                query = str(payload.get("query", query) or "")
                page = int(payload.get("page", page) or page or 1)
                page_size = int(payload.get("page_size", page_size) or page_size or 0)
                sort_mode = str(payload.get("sort_mode", sort_mode) or sort_mode or "default")
            data = await self._get_tianyi_service().list_switch_catalog(
                query=query,
                page=page,
                page_size=page_size,
                sort_mode=sort_mode,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable list_tianyi_switch_catalog cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable list_tianyi_switch_catalog failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def resolve_tianyi_catalog_cover(
        self,
        payload=None,
        game_id: str = "",
        title: str = "",
        categories: str = "",
    ) -> dict:
        """解析目录封面信息。"""
        try:
            config.logger.info("Decky callable: resolve_tianyi_catalog_cover")
            if isinstance(payload, dict):
                game_id = str(payload.get("game_id", game_id) or "")
                title = str(payload.get("title", title) or "")
                categories = str(payload.get("categories", categories) or "")
            data = await self._get_tianyi_service().resolve_catalog_cover(
                game_id=game_id,
                title=title,
                categories=categories,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable resolve_tianyi_catalog_cover cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable resolve_tianyi_catalog_cover failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def prepare_tianyi_install(
        self,
        payload=None,
        game_id: str = "",
        share_url: str = "",
        file_ids=None,
        steam_app_id: int = 0,
        download_dir: str = "",
        install_dir: str = "",
    ) -> dict:
        """为原生库页生成安装计划。"""
        try:
            config.logger.info("Decky callable: prepare_tianyi_install")
            if isinstance(payload, dict):
                game_id = str(payload.get("game_id", game_id) or "")
                share_url = str(payload.get("share_url", share_url) or "")
                steam_app_id = int(payload.get("steam_app_id", steam_app_id) or steam_app_id or 0)
                download_dir = str(payload.get("download_dir", download_dir) or "")
                install_dir = str(payload.get("install_dir", install_dir) or "")
                if "file_ids" in payload and isinstance(payload.get("file_ids"), (list, tuple)):
                    file_ids = [str(item or "").strip() for item in payload.get("file_ids", []) if str(item or "").strip()]
            data = await self._get_tianyi_service().prepare_install(
                game_id=game_id,
                share_url=share_url,
                file_ids=file_ids,
                steam_app_id=steam_app_id,
                download_dir=download_dir or None,
                install_dir=install_dir or None,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable prepare_tianyi_install cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable prepare_tianyi_install failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def start_tianyi_install(
        self,
        payload=None,
        game_id: str = "",
        share_url: str = "",
        file_ids=None,
        steam_app_id: int = 0,
        split_count: int = 0,
        download_dir: str = "",
        install_dir: str = "",
    ) -> dict:
        """为原生库页创建安装任务。"""
        try:
            config.logger.info("Decky callable: start_tianyi_install")
            if isinstance(payload, dict):
                game_id = str(payload.get("game_id", game_id) or "")
                share_url = str(payload.get("share_url", share_url) or "")
                steam_app_id = int(payload.get("steam_app_id", steam_app_id) or steam_app_id or 0)
                split_count = int(payload.get("split_count", split_count) or split_count or 0)
                download_dir = str(payload.get("download_dir", download_dir) or "")
                install_dir = str(payload.get("install_dir", install_dir) or "")
                if "file_ids" in payload and isinstance(payload.get("file_ids"), (list, tuple)):
                    file_ids = [str(item or "").strip() for item in payload.get("file_ids", []) if str(item or "").strip()]
            data = await self._get_tianyi_service().start_install(
                game_id=game_id,
                share_url=share_url,
                file_ids=file_ids,
                steam_app_id=steam_app_id,
                split_count=split_count or None,
                download_dir=download_dir or None,
                install_dir=install_dir or None,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable start_tianyi_install cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable start_tianyi_install failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def download_switch_emulator(self, payload=None) -> dict:
        """一键下载 Switch 模拟器包（天翼云盘分享）。"""
        try:
            config.logger.info("Decky callable: download_switch_emulator")

            download_dir = ""
            install_dir = ""
            if isinstance(payload, dict):
                download_dir = str(payload.get("download_dir", "") or "").strip()
                install_dir = str(payload.get("install_dir", "") or "").strip()

            share_url = "https://cloud.189.cn/web/share?code=JNJnIfRzqy22&pwd=4z1x"
            service = self._get_tianyi_service()

            # 先探测文件清单：默认选择最大且更像压缩包/AppImage 的文件，避免误下说明文件。
            plan = await service.prepare_install(
                game_id="switch_emulator",
                share_url=share_url,
                download_dir=download_dir or None,
                install_dir=install_dir or None,
            )

            file_ids = None
            try:
                files = plan.get("files") if isinstance(plan, dict) else None
                if isinstance(files, list) and files:
                    candidates = []
                    for item in files:
                        if not isinstance(item, dict):
                            continue
                        if bool(item.get("is_folder")):
                            continue
                        file_id = str(item.get("file_id", "") or "").strip()
                        name = str(item.get("name", "") or "").strip()
                        size = int(item.get("size", 0) or 0)
                        if not file_id or not name or size <= 0:
                            continue
                        lower = name.lower()
                        kind = 0
                        if lower.endswith((".7z", ".zip", ".rar", ".tar", ".tgz", ".tar.gz", ".tar.xz", ".tar.zst")):
                            kind = 3
                        elif lower.endswith(".appimage"):
                            kind = 3
                        elif lower.endswith((".exe", ".msi")):
                            kind = 2
                        elif lower.endswith((".txt", ".nfo", ".md", ".url", ".ini")):
                            kind = 0
                        else:
                            kind = 1
                        candidates.append((kind, size, file_id))

                    if candidates:
                        candidates.sort(key=lambda row: (row[0], row[1]))
                        file_ids = [candidates[-1][2]]
            except Exception:
                file_ids = None

            data = await service.start_install(
                game_id="switch_emulator",
                share_url=share_url,
                file_ids=file_ids,
                download_dir=download_dir or None,
                install_dir=install_dir or None,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable download_switch_emulator cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable download_switch_emulator failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def get_switch_emulator_status(self) -> dict:
        """获取 Switch 模拟器安装状态。"""
        try:
            config.logger.info("Decky callable: get_switch_emulator_status")
            data = await self._get_tianyi_service().get_switch_emulator_status()
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable get_switch_emulator_status cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable get_switch_emulator_status failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def import_tianyi_task_to_steam(self, payload=None, task_id: str = "", exe_rel_path: str = "") -> dict:
        """为已安装任务选择启动程序并导入 Steam（用于自定义源等多 exe 场景）。"""
        try:
            if isinstance(payload, dict):
                task_id = str(payload.get("task_id", task_id))
                exe_rel_path = str(payload.get("exe_rel_path", exe_rel_path))
            data = await self._get_tianyi_service().import_task_to_steam(
                task_id=task_id,
                exe_rel_path=exe_rel_path,
            )
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable import_tianyi_task_to_steam cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable import_tianyi_task_to_steam failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    async def restart_steam(self) -> dict:
        """重启 Steam 客户端（best-effort）。"""
        try:
            config.logger.info("Decky callable: restart_steam")
            data = await asyncio.to_thread(self._restart_steam_best_effort)
            return {"status": "success", "data": data}
        except asyncio.CancelledError:
            config.logger.warning("Decky callable restart_steam cancelled")
            return {"status": "error", "message": "操作已取消", "data": {}}
        except Exception as exc:
            config.logger.exception("Decky callable restart_steam failed: %s", exc)
            return {"status": "error", "message": str(exc), "data": {}}

    def _restart_steam_best_effort(self) -> dict:
        """使用 SteamOS 用户会话中的 steam-launcher.service 重启 Steam。"""
        def _short(text: str, limit: int = 240) -> str:
            value = str(text or "").strip().replace("\n", " ")
            if len(value) <= limit:
                return value
            return value[: limit - 3] + "..."

        def _steam_user() -> tuple[str, int, str]:
            try:
                info = pwd.getpwnam("deck")
                return info.pw_name, int(info.pw_uid), str(info.pw_dir or "/home/deck")
            except KeyError:
                info = pwd.getpwuid(os.getuid())
                return info.pw_name, int(info.pw_uid), str(info.pw_dir or os.path.expanduser("~"))

        def _steam_env(user: str, uid: int, home: str) -> dict:
            xdg_runtime_dir = f"/run/user/{uid}"
            env = {
                "HOME": home,
                "USER": user,
                "LOGNAME": user,
                "XDG_RUNTIME_DIR": xdg_runtime_dir,
                "DBUS_SESSION_BUS_ADDRESS": f"unix:path={xdg_runtime_dir}/bus",
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            }
            lang = str(os.environ.get("LANG", "") or "").strip()
            lc_all = str(os.environ.get("LC_ALL", "") or "").strip()
            if lang:
                env["LANG"] = lang
            if lc_all:
                env["LC_ALL"] = lc_all
            return env

        def _resolve_binary(name: str, fallbacks: list[str]) -> str:
            path = shutil.which(name)
            if path:
                return path
            for candidate in fallbacks:
                try:
                    if candidate and os.path.isfile(candidate):
                        return candidate
                except Exception:
                    continue
            return ""

        last_error: str = ""
        attempts: list[dict] = []
        target_user, target_uid, target_home = _steam_user()
        user_env = _steam_env(target_user, target_uid, target_home)

        systemctl = _resolve_binary("systemctl", ["/usr/bin/systemctl", "/bin/systemctl"])
        if not systemctl:
            raise RuntimeError("未找到 systemctl，无法重启 Steam")

        command = [systemctl, "--user", "restart", "steam-launcher.service"]
        run_env = user_env

        if os.geteuid() != target_uid:
            sudo_bin = _resolve_binary("sudo", ["/usr/bin/sudo", "/bin/sudo"])
            env_bin = _resolve_binary("env", ["/usr/bin/env", "/bin/env"])
            if not sudo_bin or not env_bin:
                raise RuntimeError("当前进程不在 deck 用户会话中，且缺少 sudo/env，无法重启 Steam")
            command = [
                sudo_bin,
                "-u",
                target_user,
                env_bin,
                f"HOME={target_home}",
                f"USER={target_user}",
                f"LOGNAME={target_user}",
                f"XDG_RUNTIME_DIR=/run/user/{target_uid}",
                f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{target_uid}/bus",
                f"PATH={user_env['PATH']}",
                systemctl,
                "--user",
                "restart",
                "steam-launcher.service",
            ]
            run_env = {
                "PATH": user_env["PATH"],
                "LANG": user_env.get("LANG", ""),
                "LC_ALL": user_env.get("LC_ALL", ""),
            }

        try:
            completed = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=10.0,
                start_new_session=True,
                text=True,
                env=run_env,
            )
            attempts.append(
                {
                    "method": "systemctl",
                    "unit": "steam-launcher.service",
                    "command": command,
                    "returncode": int(completed.returncode or 0),
                    "stdout": _short(completed.stdout),
                    "stderr": _short(completed.stderr),
                }
            )
            if int(completed.returncode or 0) == 0:
                return {
                    "ok": True,
                    "method": "systemctl",
                    "unit": "steam-launcher.service",
                    "attempts": attempts,
                }
            last_error = str(completed.stderr or completed.stdout or "").strip() or "systemctl --user restart steam-launcher.service failed"
        except Exception as exc:
            last_error = str(exc)
            attempts.append(
                {
                    "method": "systemctl",
                    "unit": "steam-launcher.service",
                    "command": command,
                    "error": last_error,
                }
            )

        raise RuntimeError(last_error or f"重启 Steam 失败（attempts={attempts}）")
