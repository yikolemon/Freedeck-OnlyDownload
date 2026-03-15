import {
  ButtonItem,
  ConfirmModal,
  DialogButton,
  Focusable,
  GamepadButton,
  Menu,
  MenuItem,
  Navigation,
  PanelSection,
  PanelSectionRow,
  Router,
  DropdownItem,
  SliderField,
  Tabs,
  TextField,
  ToggleField,
  showModal,
  showContextMenu,
  staticClasses,
} from "@decky/ui";
import * as DeckyUiNS from "@decky/ui";
import {
  FileSelectionType,
  callable,
  definePlugin,
  openFilePicker,
  routerHook,
  toaster,
} from "@decky/api";
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FaCloudDownloadAlt } from "react-icons/fa";

type ApiStatus = "success" | "error";
const SETTINGS_ROUTE = "/freedeck/settings";

function resolveSteamClientForKeyboard():
  | {
      Input?: {
        ShowVirtualKeyboard?: () => void;
        ShowKeyboard?: () => void;
        HideVirtualKeyboard?: () => void;
        HideKeyboard?: () => void;
      };
      System?: {
        ShowVirtualKeyboard?: () => void;
        ShowKeyboard?: () => void;
        HideVirtualKeyboard?: () => void;
        HideKeyboard?: () => void;
      };
    }
  | null {
  const resolveFromHost = (host: Window | null | undefined) => {
    if (!host) return null;
    try {
      return ((host as unknown as { SteamClient?: unknown }).SteamClient || null) as
        | {
            Input?: {
              ShowVirtualKeyboard?: () => void;
              ShowKeyboard?: () => void;
              HideVirtualKeyboard?: () => void;
              HideKeyboard?: () => void;
            };
            System?: {
              ShowVirtualKeyboard?: () => void;
              ShowKeyboard?: () => void;
              HideVirtualKeyboard?: () => void;
              HideKeyboard?: () => void;
            };
          }
        | null;
    } catch {
      return null;
    }
  };

  return (
    resolveFromHost(window) ||
    resolveFromHost(window.top || null) ||
    resolveFromHost((window as unknown as { parent?: Window }).parent || null)
  );
}

function showSteamDeckKeyboard(): void {
  const steamClient = resolveSteamClientForKeyboard();
  try {
    steamClient?.Input?.ShowVirtualKeyboard?.();
  } catch {
    // ignore
  }
  try {
    steamClient?.Input?.ShowKeyboard?.();
  } catch {
    // ignore
  }
  try {
    steamClient?.System?.ShowVirtualKeyboard?.();
  } catch {
    // ignore
  }
  try {
    steamClient?.System?.ShowKeyboard?.();
  } catch {
    // ignore
  }
  try {
    const manager = (window as unknown as { SteamUIStore?: any })?.SteamUIStore?.ActiveWindowInstance?.VirtualKeyboardManager;
    manager?.SetVirtualKeyboardVisible?.();
    manager?.SetVirtualKeyboardVisible_?.();
  } catch {
    // ignore
  }
}

function hideSteamDeckKeyboard(): void {
  const steamClient = resolveSteamClientForKeyboard();
  try {
    steamClient?.Input?.HideVirtualKeyboard?.();
  } catch {
    // ignore
  }
  try {
    steamClient?.Input?.HideKeyboard?.();
  } catch {
    // ignore
  }
  try {
    steamClient?.System?.HideVirtualKeyboard?.();
  } catch {
    // ignore
  }
  try {
    steamClient?.System?.HideKeyboard?.();
  } catch {
    // ignore
  }
  try {
    const manager = (window as unknown as { SteamUIStore?: any })?.SteamUIStore?.ActiveWindowInstance?.VirtualKeyboardManager;
    manager?.SetVirtualKeyboardHidden?.();
    manager?.SetVirtualKeyboardHidden_?.();
  } catch {
    // ignore
  }
}

interface ApiResponse<T> {
  status: ApiStatus;
  message?: string;
  data?: T;
  url?: string;
  reason?: string;
  diagnostics?: unknown;
}

interface StorageMount {
  path: string;
  label?: string;
  free_bytes?: number;
  total_bytes?: number;
}

interface LoginState {
  logged_in: boolean;
  user_account: string;
  message: string;
}

interface CtfileLoginState {
  configured: boolean;
  token_hint: string;
  message: string;
  updated_at?: number;
}

interface InstalledGameItem {
  game_id: string;
  title: string;
  install_path: string;
  size_text?: string;
  status?: string;
  steam_app_id?: number;
  playtime_seconds?: number;
  playtime_text?: string;
  playtime_sessions?: number;
  playtime_last_played_at?: number;
  playtime_active?: boolean;
}

interface MissingSteamImportItem {
  game_id: string;
  title: string;
  install_path: string;
  platform?: string;
  steam_app_id?: number;
  reason?: string;
  status?: string;
  message?: string;
  appid_unsigned?: number;
}

interface MissingSteamImportListData {
  total: number;
  items: MissingSteamImportItem[];
}

interface MissingSteamImportRunData {
  requested: number;
  imported: number;
  failed: number;
  skipped: number;
  needs_restart?: boolean;
  items: MissingSteamImportItem[];
  message?: string;
}

interface InstalledState {
  total: number;
  preview: InstalledGameItem[];
}

interface TaskItem {
  task_id: string;
  game_id: string;
  game_title?: string;
  game_name?: string;
  file_name: string;
  status: string;
  progress: number;
  speed: number;
  error_reason?: string;
  notice?: string;
  install_status?: string;
  install_progress?: number;
  install_message?: string;
  installed_path?: string;
  updated_at?: number;
  steam_import_status?: string;
  steam_exe_candidates?: string[];
  steam_exe_selected?: string;
}

interface CancelTaskPayload {
  task_id: string;
  delete_files?: boolean;
}

interface FrontendDebugLogPayload {
  message: string;
  details?: unknown;
}

interface KeyboardBridgeRequest {
  request_id: string;
  title?: string;
  placeholder?: string;
  value?: string;
  password?: boolean;
  field?: string;
  source?: string;
}

interface SettingsState {
  download_dir: string;
  install_dir: string;
  emulator_dir: string;
  split_count: number;
  aria2_fast_mode: boolean;
  force_ipv4: boolean;
  auto_switch_line: boolean;
  page_size: number;
  auto_delete_package: boolean;
  auto_install: boolean;
  lsfg_enabled: boolean;
  show_playtime_widget: boolean;
  cloud_save_auto_upload: boolean;
}

interface PanelState {
  login: LoginState;
  baidu_login: LoginState;
  ctfile_login: CtfileLoginState;
  installed: InstalledState;
  tasks: TaskItem[];
  settings: SettingsState;
  library_url: string;
  login_capture?: Record<string, unknown>;
  baidu_login_capture?: Record<string, unknown>;
  power_diagnostics?: Record<string, unknown>;
}

interface SettingsPayload {
  download_dir: string;
  install_dir: string;
  emulator_dir: string;
  split_count: number;
  aria2_fast_mode: boolean;
  force_ipv4: boolean;
  auto_switch_line: boolean;
  page_size: number;
  auto_delete_package: boolean;
  auto_install: boolean;
  lsfg_enabled: boolean;
  show_playtime_widget: boolean;
  cloud_save_auto_upload: boolean;
}

interface UrlResponse {
  url: string;
}

interface ClearLoginResponse {
  logged_in: boolean;
  user_account: string;
  message: string;
}

interface SwitchEmulatorStatusData {
  installed: boolean;
  exe_path: string;
  message: string;
  diagnostics?: Record<string, unknown>;
}

interface RuntimeRepairCandidate {
  game_id: string;
  title: string;
  install_path: string;
  prefix_ready: boolean;
  prefix_message: string;
  steam_app_id: number;
  proton_tool: string;
}

interface RuntimeRepairPackage {
  package_id: string;
  label: string;
  description: string;
  default_selected: boolean;
  global_available: boolean;
  size_bytes: number;
  source_hint?: string;
}

interface RuntimeRepairResult {
  game_id: string;
  game_title: string;
  install_path: string;
  package_id: string;
  package_label: string;
  status: string;
  reason: string;
  message: string;
  source_type: string;
  source_path: string;
  proton_tool: string;
  app_id: number;
  return_code: number;
  duration_ms: number;
  log_excerpt?: string;
}

interface RuntimeRepairLastResult {
  stage: string;
  reason: string;
  message: string;
  started_at: number;
  finished_at: number;
  duration_seconds: number;
  total_games: number;
  processed_games: number;
  total_steps: number;
  completed_steps: number;
  succeeded_steps: number;
  skipped_steps: number;
  failed_steps: number;
  results: RuntimeRepairResult[];
}

interface RuntimeRepairState {
  stage: string;
  message: string;
  reason: string;
  running: boolean;
  progress: number;
  total_games: number;
  processed_games: number;
  total_steps: number;
  completed_steps: number;
  succeeded_steps: number;
  skipped_steps: number;
  failed_steps: number;
  current_game_id: string;
  current_game_title: string;
  current_package_id: string;
  current_package_label: string;
  results: RuntimeRepairResult[];
  last_result: RuntimeRepairLastResult;
}

interface RuntimeRepairCandidatesData {
  games: RuntimeRepairCandidate[];
  ready_count: number;
  total_count: number;
}

interface RuntimeRepairPackagesData {
  packages: RuntimeRepairPackage[];
  default_package_ids: string[];
}

interface RuntimeRepairStatusData {
  state: RuntimeRepairState;
}

interface RuntimeRepairStartData {
  started: boolean;
  message: string;
  state?: RuntimeRepairState;
}

interface CloudSaveUploadItem {
  game_id: string;
  game_title: string;
  game_key: string;
  status: string;
  reason: string;
  cloud_path: string;
  source_paths?: string[];
  diagnostics?: Record<string, unknown>;
}

interface CloudSaveLastResult {
  stage: string;
  reason: string;
  message: string;
  started_at: number;
  finished_at: number;
  timestamp: string;
  total_games: number;
  processed_games: number;
  uploaded: number;
  skipped: number;
  failed: number;
  results: CloudSaveUploadItem[];
  diagnostics?: Record<string, unknown>;
}

interface CloudSaveUploadState {
  stage: string;
  message: string;
  reason: string;
  running: boolean;
  progress: number;
  current_game: string;
  total_games: number;
  processed_games: number;
  uploaded: number;
  skipped: number;
  failed: number;
  results: CloudSaveUploadItem[];
  last_result: CloudSaveLastResult;
}

interface CloudSaveUploadStartData {
  accepted: boolean;
  message: string;
  state: CloudSaveUploadState;
}

interface CloudSaveUploadStatusData {
  state: CloudSaveUploadState;
}

interface CloudSaveRestoreEntry {
  entry_id: string;
  entry_name: string;
  archive_rel_path?: string;
  file_count?: number;
}

interface CloudSaveRestoreVersion {
  version_name: string;
  timestamp: number;
  display_time: string;
  size_bytes: number;
  file_id?: string;
}

interface CloudSaveRestoreGameOption {
  game_id: string;
  game_title: string;
  game_key: string;
  versions: CloudSaveRestoreVersion[];
  available: boolean;
  reason: string;
}

interface CloudSaveRestoreOptionsData {
  games: CloudSaveRestoreGameOption[];
  updated_at: number;
}

interface CloudSaveRestoreEntriesData {
  game_id: string;
  game_key: string;
  game_title: string;
  version_name: string;
  entries: CloudSaveRestoreEntry[];
}

interface CloudSaveRestorePlanData {
  accepted: boolean;
  plan_id?: string;
  message: string;
  reason: string;
  requires_confirmation: boolean;
  conflict_count: number;
  conflict_samples: string[];
  target_candidates: string[];
  selected_target_dir: string;
  selected_entry_ids: string[];
  available_entries: CloudSaveRestoreEntry[];
  restorable_files: number;
  restorable_entries: number;
}

interface CloudSaveRestoreApplyData {
  status: string;
  reason: string;
  message: string;
  target_dir: string;
  restored_files: number;
  restored_entries: number;
  conflicts_overwritten: number;
}

interface CloudSaveRestoreResultItem {
  entry_id: string;
  entry_name: string;
  status: string;
  reason: string;
  file_count: number;
}

interface CloudSaveRestoreLastResult {
  status: string;
  reason: string;
  message: string;
  target_dir: string;
  restored_files: number;
  restored_entries: number;
  conflicts_overwritten: number;
  results: CloudSaveRestoreResultItem[];
}

interface CloudSaveRestoreState {
  stage: string;
  message: string;
  reason: string;
  running: boolean;
  progress: number;
  target_game_id: string;
  target_game_title: string;
  target_game_key: string;
  target_version: string;
  selected_entry_ids: string[];
  selected_target_dir: string;
  requires_confirmation: boolean;
  conflict_count: number;
  conflict_samples: string[];
  restored_files: number;
  restored_entries: number;
  results: CloudSaveRestoreResultItem[];
  last_result: CloudSaveRestoreLastResult;
}

interface CloudSaveRestoreStatusData {
  state: CloudSaveRestoreState;
}

interface UninstallInstalledPayload {
  game_id: string;
  install_path: string;
  delete_files: boolean;
  delete_proton_files?: boolean;
}

interface ImportTaskToSteamPayload {
  task_id: string;
  exe_rel_path: string;
}

interface LibraryGameTimeStatsData {
  managed: boolean;
  reason?: string;
  message?: string;
  app_id?: number;
  game_id?: string;
  title?: string;
  my_playtime_seconds?: number;
  my_playtime_text?: string;
  my_playtime_active?: boolean;
  last_played_at?: number;
}

interface CatalogVersionData {
  date: string;
  csv_path?: string;
}

interface CatalogUpdateData extends CatalogVersionData {
  updated: boolean;
  latest_date?: string;
  message?: string;
}

interface CatalogItem {
  game_id: string;
  title: string;
  category_parent: string;
  categories: string;
  down_url: string;
  pwd?: string;
  openpath?: string;
  open_path?: string;
  size_bytes: number;
  size_text?: string;
  app_id: number;
  square_cover_url?: string;
  cover_url?: string;
  cover?: string;
  image_url?: string;
  image?: string;
  pic_url?: string;
  pic?: string;
  thumbnail?: string;
  poster?: string;
}

interface CatalogListData {
  total: number;
  page: number;
  page_size: number;
  items: CatalogGroup[];
}

interface CatalogCoverData {
  cover_url: string;
  square_cover_url: string;
  source: string;
  matched_title: string;
  app_id: number;
  protondb_tier: string;
  cached?: boolean;
}

interface InstallPlanFile {
  file_id: string;
  name: string;
  size: number;
  is_folder: boolean;
}

interface InstallPlanData {
  game_id: string;
  game_title: string;
  openpath: string;
  provider: string;
  share_url: string;
  steam_app_id: number;
  share_code?: string;
  share_id?: string;
  pwd?: string;
  download_dir: string;
  install_dir: string;
  required_download_bytes: number;
  required_install_bytes: number;
  required_download_human: string;
  required_install_human: string;
  free_download_bytes: number;
  free_install_bytes: number;
  free_download_human: string;
  free_install_human: string;
  download_dir_ok: boolean;
  install_dir_ok: boolean;
  can_install: boolean;
  file_count: number;
  files: InstallPlanFile[];
  provider_notice?: string;
}

interface StartInstallData {
  plan: InstallPlanData;
  tasks: TaskItem[];
}

interface LibraryCatalogRequest {
  query?: string;
  page?: number;
  page_size?: number;
  sort_mode?: LibrarySortMode;
}

interface LibraryCoverRequest {
  game_id: string;
  title?: string;
  categories?: string;
}

interface PrepareInstallPayload {
  game_id: string;
  share_url?: string;
  file_ids?: string[];
  steam_app_id?: number;
  download_dir?: string;
  install_dir?: string;
}

interface StartInstallPayload extends PrepareInstallPayload {
  split_count?: number;
}

const getTianyiPanelState = callable<[Record<string, unknown>?], ApiResponse<PanelState>>("get_tianyi_panel_state");
const getTianyiLibraryUrl = callable<[], ApiResponse<UrlResponse>>("get_tianyi_library_url");
const getTianyiLoginUrl = callable<[], ApiResponse<UrlResponse>>("get_tianyi_login_url");
const getBaiduLoginUrl = callable<[], ApiResponse<UrlResponse>>("get_baidu_login_url");
const getCtfileLoginGuideUrl = callable<[], ApiResponse<UrlResponse>>("get_ctfile_login_guide_url");
const getTianyiCatalogVersion = callable<[], ApiResponse<CatalogVersionData>>("get_tianyi_catalog_version");
const updateTianyiCatalog = callable<[], ApiResponse<CatalogUpdateData>>("update_tianyi_catalog");
const listTianyiCatalog = callable<[LibraryCatalogRequest?], ApiResponse<CatalogListData>>("list_tianyi_catalog");
const listTianyiSwitchCatalog = callable<[LibraryCatalogRequest?], ApiResponse<CatalogListData>>("list_tianyi_switch_catalog");
const resolveTianyiCatalogCover = callable<[LibraryCoverRequest], ApiResponse<CatalogCoverData>>("resolve_tianyi_catalog_cover");
const prepareTianyiInstall = callable<[PrepareInstallPayload], ApiResponse<InstallPlanData>>("prepare_tianyi_install");
const startTianyiInstall = callable<[StartInstallPayload], ApiResponse<StartInstallData>>("start_tianyi_install");
const setTianyiSettings = callable<[SettingsPayload], ApiResponse<SettingsState>>("set_tianyi_settings");
const listMediaMounts = callable<[], ApiResponse<{ mounts: StorageMount[] }>>("list_media_mounts");
const clearTianyiLogin = callable<[], ApiResponse<ClearLoginResponse>>("clear_tianyi_login");
const startBaiduLoginCapture = callable<[Record<string, unknown>?], ApiResponse<Record<string, unknown>>>("start_baidu_login_capture");
const stopBaiduLoginCapture = callable<[], ApiResponse<Record<string, unknown>>>("stop_baidu_login_capture");
const clearBaiduLogin = callable<[], ApiResponse<ClearLoginResponse>>("clear_baidu_login");
const setCtfileToken = callable<[Record<string, unknown>?], ApiResponse<CtfileLoginState>>("set_ctfile_token");
const clearCtfileToken = callable<[], ApiResponse<CtfileLoginState>>("clear_ctfile_token");
const cancelTianyiTask = callable<[CancelTaskPayload], ApiResponse<Record<string, unknown>>>("cancel_tianyi_task");
const dismissTianyiTask = callable<[CancelTaskPayload], ApiResponse<Record<string, unknown>>>("dismiss_tianyi_task");
const cancelTianyiInstall = callable<[CancelTaskPayload], ApiResponse<Record<string, unknown>>>("cancel_tianyi_install");
const frontendDebugLog = callable<[FrontendDebugLogPayload], ApiResponse<Record<string, unknown>>>("frontend_debug_log");
const pollTianyiKeyboardBridgeRequest = callable<[], ApiResponse<{ request?: KeyboardBridgeRequest | null }>>(
  "poll_tianyi_keyboard_bridge_request",
);
const resolveTianyiKeyboardBridgeRequest = callable<
  [{ request_id: string; ok: boolean; value: string; reason: string }],
  ApiResponse<Record<string, unknown>>
>("resolve_tianyi_keyboard_bridge_request");
const importTianyiTaskToSteam = callable<[ImportTaskToSteamPayload], ApiResponse<Record<string, unknown>>>(
  "import_tianyi_task_to_steam",
);
const restartSteam = callable<[], ApiResponse<Record<string, unknown>>>("restart_steam");
const listTianyiMissingSteamImports = callable<[], ApiResponse<MissingSteamImportListData>>(
  "list_tianyi_missing_steam_imports",
);
const reimportTianyiMissingSteamImports = callable<
  [Record<string, unknown>?],
  ApiResponse<MissingSteamImportRunData>
>("reimport_tianyi_missing_steam_imports");
const startTianyiCloudSaveUpload = callable<[], ApiResponse<CloudSaveUploadStartData>>("start_tianyi_cloud_save_upload");
const getTianyiCloudSaveUploadStatus = callable<[], ApiResponse<CloudSaveUploadStatusData>>(
  "get_tianyi_cloud_save_upload_status",
);
const listTianyiCloudSaveRestoreOptions = callable<[], ApiResponse<CloudSaveRestoreOptionsData>>(
  "list_tianyi_cloud_save_restore_options",
);
const listTianyiCloudSaveRestoreEntries = callable<
  [{ game_id: string; game_key: string; game_title: string; version_name: string }],
  ApiResponse<CloudSaveRestoreEntriesData>
>("list_tianyi_cloud_save_restore_entries");
const planTianyiCloudSaveRestore = callable<
  [{ game_id: string; game_key: string; game_title: string; version_name: string; selected_entry_ids: string[]; target_dir?: string }],
  ApiResponse<CloudSaveRestorePlanData>
>("plan_tianyi_cloud_save_restore");
const applyTianyiCloudSaveRestore = callable<
  [{ plan_id: string; confirm_overwrite: boolean }],
  ApiResponse<CloudSaveRestoreApplyData>
>("apply_tianyi_cloud_save_restore");
const getTianyiCloudSaveRestoreStatus = callable<[], ApiResponse<CloudSaveRestoreStatusData>>(
  "get_tianyi_cloud_save_restore_status",
);
const listTianyiRuntimeRepairCandidates = callable<[], ApiResponse<RuntimeRepairCandidatesData>>(
  "list_tianyi_runtime_repair_candidates",
);
const listTianyiRuntimeRepairPackages = callable<[], ApiResponse<RuntimeRepairPackagesData>>(
  "list_tianyi_runtime_repair_packages",
);
const startTianyiRuntimeRepair = callable<
  [{ game_ids: string[]; package_ids: string[] }],
  ApiResponse<RuntimeRepairStartData>
>("start_tianyi_runtime_repair");
const getTianyiRuntimeRepairStatus = callable<[], ApiResponse<RuntimeRepairStatusData>>(
  "get_tianyi_runtime_repair_status",
);
const downloadSwitchEmulator = callable<[Record<string, unknown>?], ApiResponse<Record<string, unknown>>>(
  "download_switch_emulator",
);
const getSwitchEmulatorStatus = callable<[], ApiResponse<SwitchEmulatorStatusData>>("get_switch_emulator_status");
const recordTianyiGameAction = callable<
  [{ phase: string; app_id: string; action_name?: string }],
  ApiResponse<Record<string, unknown>>
>("record_tianyi_game_action");
const getTianyiLibraryGameTimeStats = callable<
  [{ app_id: string; title?: string }],
  ApiResponse<LibraryGameTimeStatsData>
>("get_tianyi_library_game_time_stats");
const uninstallTianyiInstalledGame = callable<[UninstallInstalledPayload], ApiResponse<Record<string, unknown>>>(
  "uninstall_tianyi_installed_game",
);
const PANEL_REQUEST_TIMEOUT_MS = 6000;
const PANEL_POLL_MODE_ACTIVE = "active";
const PANEL_POLL_MODE_IDLE = "idle";
const PANEL_POLL_MODE_BACKGROUND = "background";
type PanelPollMode = typeof PANEL_POLL_MODE_ACTIVE | typeof PANEL_POLL_MODE_IDLE | typeof PANEL_POLL_MODE_BACKGROUND;
const PANEL_ACTIVE_POLL_MS = 900;
const PANEL_IDLE_POLL_MS = 6000;
const PANEL_BACKGROUND_POLL_MS = 30000;
const FAILED_TASK_AUTO_DISMISS_SECONDS = 6;

let restartSteamPromptOpen = false;

function showRestartSteamPrompt(reason: string): void {
  const message = String(reason || "").trim() || "Steam 库变更可能需要重启 Steam 才能生效。";
  if (restartSteamPromptOpen) return;
  restartSteamPromptOpen = true;

  showModal(
    <ConfirmModal
      strTitle="重启 Steam"
      strDescription={`${message}\n\n是否现在重启 Steam（不会重启系统）？`}
      strOKButtonText="现在重启"
      strCancelButtonText="稍后"
      onOK={() => {
        restartSteamPromptOpen = false;
        toaster.toast({ title: "Freedeck", body: "正在重启 Steam..." });
        void (async () => {
          try {
            const result = await restartSteam();
            if (result.status !== "success" || result.data?.ok === false) {
              throw new Error(String(result.message || result.data?.message || "重启 Steam 失败"));
            }
            toaster.toast({ title: "Freedeck", body: "已请求重启 Steam" });
          } catch (error) {
            toaster.toast({ title: "Freedeck", body: `重启 Steam 失败：${error}` });
          }
        })();
      }}
      onCancel={() => {
        restartSteamPromptOpen = false;
      }}
    />,
  );
}

const EMPTY_SETTINGS: SettingsState = {
  download_dir: "",
  install_dir: "",
  emulator_dir: "",
  split_count: 16,
  aria2_fast_mode: false,
  force_ipv4: true,
  auto_switch_line: true,
  page_size: 50,
  auto_delete_package: false,
  auto_install: true,
  lsfg_enabled: false,
  show_playtime_widget: true,
  cloud_save_auto_upload: false,
};

const EMPTY_SWITCH_EMULATOR_STATUS: SwitchEmulatorStatusData & { loaded: boolean } = {
  installed: false,
  exe_path: "",
  message: "",
  loaded: false,
};

const EMPTY_CLOUD_SAVE_STATE: CloudSaveUploadState = {
  stage: "idle",
  message: "未开始",
  reason: "",
  running: false,
  progress: 0,
  current_game: "",
  total_games: 0,
  processed_games: 0,
  uploaded: 0,
  skipped: 0,
  failed: 0,
  results: [],
  last_result: {
    stage: "",
    reason: "",
    message: "",
    started_at: 0,
    finished_at: 0,
    timestamp: "",
    total_games: 0,
    processed_games: 0,
    uploaded: 0,
    skipped: 0,
    failed: 0,
    results: [],
  },
};

const EMPTY_CLOUD_SAVE_RESTORE_STATE: CloudSaveRestoreState = {
  stage: "idle",
  message: "未开始",
  reason: "",
  running: false,
  progress: 0,
  target_game_id: "",
  target_game_title: "",
  target_game_key: "",
  target_version: "",
  selected_entry_ids: [],
  selected_target_dir: "",
  requires_confirmation: false,
  conflict_count: 0,
  conflict_samples: [],
  restored_files: 0,
  restored_entries: 0,
  results: [],
  last_result: {
    status: "",
    reason: "",
    message: "",
    target_dir: "",
    restored_files: 0,
    restored_entries: 0,
    conflicts_overwritten: 0,
    results: [],
  },
};

const EMPTY_RUNTIME_REPAIR_STATE: RuntimeRepairState = {
  stage: "idle",
  message: "未开始",
  reason: "",
  running: false,
  progress: 0,
  total_games: 0,
  processed_games: 0,
  total_steps: 0,
  completed_steps: 0,
  succeeded_steps: 0,
  skipped_steps: 0,
  failed_steps: 0,
  current_game_id: "",
  current_game_title: "",
  current_package_id: "",
  current_package_label: "",
  results: [],
  last_result: {
    stage: "",
    reason: "",
    message: "",
    started_at: 0,
    finished_at: 0,
    duration_seconds: 0,
    total_games: 0,
    processed_games: 0,
    total_steps: 0,
    completed_steps: 0,
    succeeded_steps: 0,
    skipped_steps: 0,
    failed_steps: 0,
    results: [],
  },
};

const PAGE_SIZE_OPTIONS = [20, 30, 50, 80, 100, 150, 200].map((value) => ({
  data: value,
  label: `${value} / 页`,
}));

const EMPTY_STATE: PanelState = {
  login: { logged_in: false, user_account: "", message: "" },
  baidu_login: { logged_in: false, user_account: "", message: "" },
  ctfile_login: { configured: false, token_hint: "", message: "" },
  installed: { total: 0, preview: [] },
  tasks: [],
  settings: EMPTY_SETTINGS,
  library_url: "",
};

interface GamepadTabClassMap {
  GamepadTabbedPage?: string;
  TabsRowScroll?: string;
  TabRowTabs?: string;
  TabHeaderRowWrapper?: string;
  TabRow?: string;
  Tab?: string;
  TabContents?: string;
  TabContentsScroll?: string;
  Left?: string;
  Right?: string;
  Glyphs?: string;
  Arrows?: string;
  Active?: string;
  Selected?: string;
}

function getGamepadTabClassMap(): GamepadTabClassMap | null {
  const map = (DeckyUiNS as unknown as { gamepadTabbedPageClasses?: GamepadTabClassMap }).gamepadTabbedPageClasses;
  if (!map || typeof map !== "object") return null;
  return map;
}

function formatSpeed(speed: number): string {
  let value = Number(speed || 0);
  if (!Number.isFinite(value) || value <= 0) return "0 B/s";
  const units = ["B/s", "KB/s", "MB/s", "GB/s"];
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

function formatPlaytimeText(seconds: number, fallback?: string): string {
  const fallbackText = String(fallback || "").trim();
  if (fallbackText) return fallbackText;
  const totalSeconds = Math.max(0, Number(seconds || 0));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  if (hours > 0) return `${hours} 小时 ${minutes} 分钟`;
  if (minutes > 0) return `${minutes} 分钟`;
  return "0 分钟";
}

function formatLastPlayedText(lastPlayedAt: number | undefined): string {
  const timestamp = Math.max(0, Number(lastPlayedAt || 0));
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "未记录";
  const date = new Date(timestamp * 1000);
  if (Number.isNaN(date.getTime())) return "未记录";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day} ${hours}:${minutes}`;
}

function clampProgress(progress: number): number {
  const value = Number(progress || 0);
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 100) return 100;
  return value;
}

function downloadStatusText(status: string): string {
  const value = String(status || "").toLowerCase();
  if (value === "active") return "下载中";
  if (value === "waiting") return "等待中";
  if (value === "paused") return "已暂停";
  if (value === "complete") return "下载完成";
  if (value === "error") return "下载失败";
  if (value === "removed") return "已移除";
  return status || "未知";
}

function installStatusText(status: string): string {
  const value = String(status || "").toLowerCase();
  if (value === "pending") return "待处理";
  if (value === "installing") return "安装中";
  if (value === "installed") return "已安装";
  if (value === "bundled") return "分卷";
  if (value === "skipped") return "已跳过";
  if (value === "canceled") return "已取消";
  if (value === "failed") return "安装失败";
  return status || "未开始";
}

function runtimeRepairStageText(stage: string): string {
  const value = String(stage || "").toLowerCase();
  if (value === "running") return "修复中";
  if (value === "completed") return "已完成";
  if (value === "failed") return "失败";
  return "未开始";
}

function runtimeRepairResultStatusText(status: string): string {
  const value = String(status || "").toLowerCase();
  if (value === "success") return "成功";
  if (value === "skipped") return "跳过";
  if (value === "failed") return "失败";
  return status || "未知";
}

function progressColors(task: TaskItem): { track: string; fill: string; label: string } {
  const downloadStatus = String(task.status || "").toLowerCase();
  const installStatus = String(task.install_status || "").toLowerCase();
  if (downloadStatus === "error" || installStatus === "failed") {
    return { track: "rgba(255, 87, 87, 0.2)", fill: "#ff5757", label: "#ffb2b2" };
  }
  if (downloadStatus === "complete" && installStatus === "installed") {
    return { track: "rgba(67, 181, 129, 0.2)", fill: "#43b581", label: "#b8f3d7" };
  }
  if (downloadStatus === "paused") {
    return { track: "rgba(255, 184, 64, 0.22)", fill: "#ffb840", label: "#ffe2a8" };
  }
  return { track: "rgba(208, 188, 255, 0.24)", fill: "#d0bcff", label: "#ece2ff" };
}

function TaskProgressRow(
  task: TaskItem,
  options?: {
    canceling?: boolean;
    busy?: boolean;
    selectingExe?: boolean;
    onCancel?: (task: TaskItem) => void;
    onSelectExe?: (task: TaskItem) => void;
  },
) {
  const downloadStatus = String(task.status || "").trim().toLowerCase();
  const rawInstallStatus = String(task.install_status || "").trim();
  const installStatus = rawInstallStatus.toLowerCase();
  const isDownloadComplete = downloadStatus === "complete";
  const isInstallStage = isDownloadComplete;
  const progress = clampProgress(isInstallStage ? task.install_progress || 0 : task.progress);
  const colors = progressColors(task);
  const title = task.game_title || task.game_name || task.file_name || "未命名任务";
  const installMessage = String(task.install_message || "").trim();
  const errorReason = String(task.error_reason || "").trim();
  const notice = String(task.notice || "").trim();
  const steamImportStatus = String(task.steam_import_status || "").trim().toLowerCase();
  const exeCandidates = Array.isArray(task.steam_exe_candidates) ? task.steam_exe_candidates : [];
  const needsExeSelection = Boolean(
    isInstallStage &&
      installStatus === "installed" &&
      steamImportStatus === "needs_exe" &&
      exeCandidates.length > 1 &&
      options?.onSelectExe,
  );

  const stageText = (() => {
    if (downloadStatus === "error") return downloadStatusText(downloadStatus);
    if (!isDownloadComplete) return downloadStatusText(downloadStatus);
    if (installStatus === "failed") return "安装失败";
    if (installStatus === "installed") return "已安装";
    if (installStatus === "bundled") return "分卷";
    if (installStatus === "skipped" && installMessage.startsWith("分卷文件")) return "分卷";
    if (installStatus === "skipped") return "已跳过";
    if (installStatus === "canceled") return "已取消";
    return "安装中";
  })();

  const canCancel = Boolean(
    options?.onCancel &&
      ((!isDownloadComplete && ["active", "waiting", "paused", "error"].includes(downloadStatus)) ||
        (isDownloadComplete && ["pending", "installing"].includes(installStatus)) ||
        (isDownloadComplete && ["failed", "canceled", "skipped", "bundled"].includes(installStatus))),
  );
  const cancelLabel =
    downloadStatus === "error"
      ? "清除"
      : !isDownloadComplete
        ? "取消"
        : ["failed", "canceled", "skipped", "bundled"].includes(installStatus)
          ? "清除"
          : "取消安装";

  return (
    <div
      style={{
        width: "100%",
        padding: "8px 0",
        display: "flex",
        flexDirection: "column",
        gap: "6px",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
        <div
          style={{
            fontSize: "14px",
            fontWeight: 600,
            lineHeight: 1.35,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            maxWidth: "70%",
          }}
          title={title}
        >
          {title}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px", flex: "0 0 auto" }}>
          <div style={{ fontSize: "11px", color: colors.label }}>{stageText}</div>
          {canCancel ? (
            <Focusable style={{ flex: "0 0 auto" }}>
              <DialogButton
                onClick={() => options?.onCancel?.(task)}
                onOKButton={() => options?.onCancel?.(task)}
                disabled={Boolean(options?.busy) || Boolean(options?.canceling)}
                style={{
                  minWidth: "72px",
                  height: "26px",
                  borderRadius: "10px",
                  border: "1px solid rgba(255, 255, 255, 0.26)",
                  background: "rgba(255, 106, 106, 0.2)",
                  color: "#ffe8e8",
                  padding: "0 10px",
                }}
              >
                {options?.canceling ? "处理中..." : cancelLabel}
              </DialogButton>
            </Focusable>
          ) : null}
        </div>
      </div>

      <div
        style={{
          width: "100%",
          height: "8px",
          borderRadius: "999px",
          background: colors.track,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            width: `${progress.toFixed(1)}%`,
            height: "100%",
            borderRadius: "999px",
            background: colors.fill,
            transition: "width 860ms linear",
          }}
        />
      </div>

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", fontSize: "11px", color: "#c9c9c9" }}>
        <span>{`进度 ${progress.toFixed(1)}%`}</span>
        <span>{isInstallStage ? "速度 -" : `速度 ${formatSpeed(task.speed)}`}</span>
      </div>

      {isInstallStage ? (
        <div style={{ fontSize: "11px", color: "#bcbcbc", lineHeight: 1.4 }}>
          {`安装：${installStatusText(installStatus === "pending" ? "installing" : rawInstallStatus || "installing")}${
            installMessage ? ` | ${installMessage}` : ""
          }`}
        </div>
      ) : null}
      {needsExeSelection ? (
        <div style={{ display: "flex", justifyContent: "flex-end" }}>
          <Focusable style={{ flex: "0 0 auto" }}>
            <DialogButton
              onClick={() => options?.onSelectExe?.(task)}
              onOKButton={() => options?.onSelectExe?.(task)}
              disabled={Boolean(options?.busy) || Boolean(options?.selectingExe)}
              style={{
                minWidth: "120px",
                height: "26px",
                borderRadius: "10px",
                border: "1px solid rgba(255, 255, 255, 0.26)",
                background: "rgba(208, 188, 255, 0.18)",
                color: "#f4ecff",
                padding: "0 10px",
              }}
            >
              {options?.selectingExe ? "处理中..." : "选择启动程序"}
            </DialogButton>
          </Focusable>
        </div>
      ) : null}
      {errorReason ? (
        <div style={{ fontSize: "11px", color: "#ff8e8e", lineHeight: 1.4 }}>{`错误：${errorReason}`}</div>
      ) : null}
      {notice ? <div style={{ fontSize: "11px", color: "#ffd18b", lineHeight: 1.4 }}>{notice}</div> : null}
    </div>
  );
}

function RuntimeRepairGameSelectModal(props: {
  candidates: RuntimeRepairCandidate[];
  initialSelectedIds?: string[];
  onConfirm: (gameIds: string[]) => void;
  closeModal?: () => void;
}) {
  const [search, setSearch] = useState<string>("");
  const readyIds = useMemo(
    () => props.candidates.filter((item) => item.prefix_ready).map((item) => String(item.game_id || "")).filter(Boolean),
    [props.candidates],
  );
  const [selectedIds, setSelectedIds] = useState<string[]>(
    (props.initialSelectedIds || []).filter((item) => readyIds.includes(String(item || ""))),
  );

  const filteredCandidates = useMemo(() => {
    const keyword = String(search || "").trim().toLowerCase();
    if (!keyword) return props.candidates;
    return props.candidates.filter((item) => {
      const title = String(item.title || "").toLowerCase();
      const path = String(item.install_path || "").toLowerCase();
      return title.includes(keyword) || path.includes(keyword);
    });
  }, [props.candidates, search]);

  const toggleGame = useCallback((gameId: string, checked: boolean) => {
    const normalized = String(gameId || "").trim();
    if (!normalized) return;
    setSelectedIds((prev) => {
      const set = new Set(prev.map((item) => String(item || "").trim()).filter(Boolean));
      if (checked) set.add(normalized);
      else set.delete(normalized);
      return Array.from(set);
    });
  }, []);

  const okDisabledProps = ((!selectedIds.length ? ({ bOKDisabled: true } as any) : {}) as any);

  return (
    <ConfirmModal
      {...okDisabledProps}
      strTitle="选择 PC 游戏"
      strOKButtonText="下一步"
      strCancelButtonText="取消"
      onOK={() => {
        props.closeModal?.();
        props.onConfirm(selectedIds);
      }}
      onCancel={() => {
        props.closeModal?.();
      }}
    >
      <>
        <div style={{ fontSize: "12px", lineHeight: 1.5, opacity: 0.88 }}>
          {`可修复 ${readyIds.length} / ${props.candidates.length} 个已安装 PC 游戏。未生成 compatdata 的游戏会显示原因并禁用选择。`}
        </div>
        <div style={{ marginTop: "10px" }}>
          <TextField
            value={search}
            label="筛选"
            description="按游戏名或安装路径过滤"
            bShowClearAction={true}
            onChange={(event) => setSearch(String(event?.currentTarget?.value ?? ""))}
          />
        </div>
        <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px" }}>
          <ButtonItem layout="below" onClick={() => setSelectedIds(readyIds)}>
            全选可修复项
          </ButtonItem>
          <ButtonItem layout="below" onClick={() => setSelectedIds([])}>
            清空
          </ButtonItem>
        </div>
        <div style={{ maxHeight: "46vh", overflowY: "auto", display: "grid", gap: "8px" }}>
          {filteredCandidates.map((item) => {
            const gameId = String(item.game_id || "");
            const ready = Boolean(item.prefix_ready);
            const checked = selectedIds.includes(gameId);
            const description = ready
              ? `${item.install_path || "未记录路径"}${item.proton_tool ? ` | ${item.proton_tool}` : ""}`
              : item.prefix_message || "未就绪";
            return (
              <div
                key={`runtime_repair_game_${gameId}`}
                style={{
                  padding: "8px 10px",
                  background: ready ? "rgba(255,255,255,0.04)" : "rgba(255, 108, 108, 0.08)",
                  border: ready ? "1px solid rgba(255,255,255,0.06)" : "1px solid rgba(255, 108, 108, 0.16)",
                }}
              >
                <ToggleField
                  label={item.title || gameId || "未命名游戏"}
                  description={description}
                  checked={checked}
                  onChange={(value: boolean) => toggleGame(gameId, value)}
                  disabled={!ready}
                />
              </div>
            );
          })}
        </div>
      </>
    </ConfirmModal>
  );
}

function RuntimeRepairPackageSelectModal(props: {
  packages: RuntimeRepairPackage[];
  defaultSelectedIds: string[];
  selectedGameCount: number;
  onConfirm: (packageIds: string[]) => void;
  closeModal?: () => void;
}) {
  const defaultSelected = useMemo(
    () => props.defaultSelectedIds.map((item) => String(item || "").trim()).filter(Boolean),
    [props.defaultSelectedIds],
  );
  const [selectedIds, setSelectedIds] = useState<string[]>(defaultSelected);

  const togglePackage = useCallback((packageId: string, checked: boolean) => {
    const normalized = String(packageId || "").trim();
    if (!normalized) return;
    setSelectedIds((prev) => {
      const set = new Set(prev.map((item) => String(item || "").trim()).filter(Boolean));
      if (checked) set.add(normalized);
      else set.delete(normalized);
      return Array.from(set);
    });
  }, []);

  const packageIds = useMemo(
    () => props.packages.map((item) => String(item.package_id || "")).filter(Boolean),
    [props.packages],
  );
  const okDisabledProps = ((!selectedIds.length ? ({ bOKDisabled: true } as any) : {}) as any);

  return (
    <ConfirmModal
      {...okDisabledProps}
      strTitle="选择运行库"
      strOKButtonText="开始修复"
      strCancelButtonText="取消"
      onOK={() => {
        props.closeModal?.();
        props.onConfirm(selectedIds);
      }}
      onCancel={() => {
        props.closeModal?.();
      }}
    >
      <>
        <div style={{ fontSize: "12px", lineHeight: 1.5, opacity: 0.88 }}>
          {`将为已选 ${props.selectedGameCount} 个游戏依次安装所选运行库。默认勾选 VC++ 2015-2022（x64/x86）。`}
        </div>
        <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px" }}>
          <ButtonItem layout="below" onClick={() => setSelectedIds(defaultSelected)}>
            恢复默认
          </ButtonItem>
          <ButtonItem layout="below" onClick={() => setSelectedIds(packageIds)}>
            全选
          </ButtonItem>
          <ButtonItem layout="below" onClick={() => setSelectedIds([])}>
            清空
          </ButtonItem>
        </div>
        <div style={{ maxHeight: "46vh", overflowY: "auto", display: "grid", gap: "8px" }}>
          {props.packages.map((item) => {
            const packageId = String(item.package_id || "");
            const checked = selectedIds.includes(packageId);
            const sizeText = item.size_bytes > 0 ? ` | ${formatBytes(item.size_bytes)}` : "";
            const availabilityText = item.global_available ? "已检测到 CommonRedist" : "未检测到 CommonRedist（若游戏自带安装器仍可尝试）";
            return (
              <div
                key={`runtime_repair_pkg_${packageId}`}
                style={{
                  padding: "8px 10px",
                  background: "rgba(255,255,255,0.04)",
                  border: "1px solid rgba(255,255,255,0.06)",
                }}
              >
                <ToggleField
                  label={item.label || packageId}
                  description={`${item.description || ""}${sizeText}\n${availabilityText}`}
                  checked={checked}
                  onChange={(value: boolean) => togglePackage(packageId, value)}
                />
              </div>
            );
          })}
        </div>
      </>
    </ConfirmModal>
  );
}

function CloudSaveRestoreModal(props: {
  loggedIn: boolean;
  closeModal?: () => void;
}) {
  const [cloudSaveRestoreState, setCloudSaveRestoreState] = useState<CloudSaveRestoreState>(EMPTY_CLOUD_SAVE_RESTORE_STATE);
  const [loadingRestoreOptions, setLoadingRestoreOptions] = useState<boolean>(false);
  const [loadingRestoreEntries, setLoadingRestoreEntries] = useState<boolean>(false);
  const [planningRestore, setPlanningRestore] = useState<boolean>(false);
  const [applyingRestore, setApplyingRestore] = useState<boolean>(false);
  const [restoreOptions, setRestoreOptions] = useState<CloudSaveRestoreGameOption[]>([]);
  const [selectedRestoreGameKey, setSelectedRestoreGameKey] = useState<string>("");
  const [selectedRestoreVersion, setSelectedRestoreVersion] = useState<string>("");
  const [restoreEntries, setRestoreEntries] = useState<CloudSaveRestoreEntry[]>([]);
  const [selectedRestoreEntryIds, setSelectedRestoreEntryIds] = useState<string[]>([]);
  const [targetCandidates, setTargetCandidates] = useState<string[]>([]);
  const [selectedRestoreTargetDir, setSelectedRestoreTargetDir] = useState<string>("");
  const restoreOptionsLoadingRef = useRef<boolean>(false);

  const selectedRestoreGame = useMemo(
    () => restoreOptions.find((item) => item.game_key === selectedRestoreGameKey) || null,
    [restoreOptions, selectedRestoreGameKey],
  );
  const restoreVersionOptions = useMemo(
    () =>
      (selectedRestoreGame?.versions || []).map((item) => ({
        data: item.version_name,
        label: `${item.display_time || item.version_name} | ${formatBytes(item.size_bytes)}`,
      })),
    [selectedRestoreGame],
  );
  const restoreGameOptions = useMemo(
    () =>
      restoreOptions.map((item) => ({
        data: item.game_key,
        label: item.available
          ? `${item.game_title}（${item.versions.length} 个版本）`
          : `${item.game_title}（不可恢复：${item.reason || "无可用版本"}）`,
      })),
    [restoreOptions],
  );

  useEffect(() => {
    if (!restoreOptions.length) {
      setSelectedRestoreGameKey("");
      setSelectedRestoreVersion("");
      return;
    }
    const exists = restoreOptions.some((item) => item.game_key === selectedRestoreGameKey);
    if (!exists) {
      const fallback = restoreOptions.find((item) => item.available && item.versions.length > 0);
      setSelectedRestoreGameKey(String(fallback?.game_key || ""));
    }
  }, [restoreOptions, selectedRestoreGameKey]);

  useEffect(() => {
    if (!selectedRestoreGame) {
      setSelectedRestoreVersion("");
      return;
    }
    const exists = selectedRestoreGame.versions.some((item) => item.version_name === selectedRestoreVersion);
    if (!exists) {
      setSelectedRestoreVersion(String(selectedRestoreGame.versions[0]?.version_name || ""));
    }
  }, [selectedRestoreGame, selectedRestoreVersion]);

  useEffect(() => {
    setRestoreEntries([]);
    setSelectedRestoreEntryIds([]);
    setTargetCandidates([]);
    setSelectedRestoreTargetDir("");
  }, [selectedRestoreGameKey, selectedRestoreVersion]);

  const refreshCloudSaveRestoreStatus = useCallback(async () => {
    const result = await getTianyiCloudSaveRestoreStatus();
    if (result.status !== "success") {
      throw new Error(result.message || "读取恢复状态失败");
    }
    const nextState = normalizeCloudSaveRestoreState(result.data?.state);
    setCloudSaveRestoreState(nextState);
    if (Array.isArray(nextState.selected_entry_ids) && nextState.selected_entry_ids.length > 0) {
      setSelectedRestoreEntryIds((prev) => (prev.length > 0 ? prev : nextState.selected_entry_ids));
    }
    const nextTargetDir = String(nextState.selected_target_dir || "").trim();
    if (nextTargetDir) {
      setSelectedRestoreTargetDir((prev) => prev || nextTargetDir);
    }
  }, []);

  const refreshRestoreOptions = useCallback(
    async (quiet = false) => {
      if (restoreOptionsLoadingRef.current) return;
      restoreOptionsLoadingRef.current = true;
      setLoadingRestoreOptions(true);
      try {
        const result = await listTianyiCloudSaveRestoreOptions();
        if (result.status !== "success" || !result.data) {
          throw new Error(result.message || "读取云存档列表失败");
        }
        const games = Array.isArray(result.data.games) ? result.data.games : [];
        setRestoreOptions(games);
        const fallback = games.find((item) => item.available && item.versions.length > 0);
        if (fallback) setSelectedRestoreGameKey((prev) => prev || String(fallback.game_key || ""));
        if (!quiet) {
          toaster.toast({ title: "Freedeck", body: `已刷新云存档版本（游戏 ${games.length} 个）` });
        }
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        restoreOptionsLoadingRef.current = false;
        setLoadingRestoreOptions(false);
      }
    },
    [],
  );

  useEffect(() => {
    if (!props.loggedIn) return;
    let alive = true;
    (async () => {
      try {
        await refreshCloudSaveRestoreStatus();
        await refreshRestoreOptions(true);
      } catch (error) {
        if (alive) toaster.toast({ title: "Freedeck", body: String(error) });
      }
    })();
    return () => {
      alive = false;
    };
  }, [props.loggedIn, refreshCloudSaveRestoreStatus, refreshRestoreOptions]);

  useEffect(() => {
    if (!cloudSaveRestoreState.running) return;
    let alive = true;
    let timer = 0;
    const poll = async () => {
      if (!alive) return;
      try {
        await refreshCloudSaveRestoreStatus();
      } catch {
      } finally {
        if (alive) timer = window.setTimeout(poll, 1200);
      }
    };
    timer = window.setTimeout(poll, 1200);
    return () => {
      alive = false;
      if (timer) window.clearTimeout(timer);
    };
  }, [cloudSaveRestoreState.running, refreshCloudSaveRestoreStatus]);

  const onLoadRestoreEntries = useCallback(async () => {
    if (loadingRestoreEntries) return;
    if (!selectedRestoreGame || !selectedRestoreVersion) {
      toaster.toast({ title: "Freedeck", body: "请先选择游戏和版本" });
      return;
    }
    setLoadingRestoreEntries(true);
    try {
      const result = await listTianyiCloudSaveRestoreEntries({
        game_id: String(selectedRestoreGame.game_id || ""),
        game_key: String(selectedRestoreGame.game_key || ""),
        game_title: String(selectedRestoreGame.game_title || ""),
        version_name: String(selectedRestoreVersion || ""),
      });
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "读取存档项失败");
      }
      const rows = Array.isArray(result.data.entries) ? result.data.entries : [];
      setRestoreEntries(rows);
      setSelectedRestoreEntryIds(rows.map((item) => String(item.entry_id || "")).filter(Boolean));
      setTargetCandidates([]);
      setSelectedRestoreTargetDir("");
      toaster.toast({ title: "Freedeck", body: `已读取存档项 ${rows.length} 个` });
      await refreshCloudSaveRestoreStatus();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setLoadingRestoreEntries(false);
    }
  }, [loadingRestoreEntries, refreshCloudSaveRestoreStatus, selectedRestoreGame, selectedRestoreVersion]);

  const toggleRestoreEntry = useCallback((entryId: string, checked: boolean) => {
    const id = String(entryId || "").trim();
    if (!id) return;
    setSelectedRestoreEntryIds((prev) => {
      const set = new Set(prev.map((item) => String(item || "").trim()).filter(Boolean));
      if (checked) set.add(id);
      else set.delete(id);
      return Array.from(set);
    });
  }, []);

  const runRestoreApply = useCallback(
    async (planId: string, confirmOverwrite: boolean) => {
      const normalizedPlanId = String(planId || "").trim();
      if (!normalizedPlanId) return;
      setApplyingRestore(true);
      try {
        const result = await applyTianyiCloudSaveRestore({
          plan_id: normalizedPlanId,
          confirm_overwrite: Boolean(confirmOverwrite),
        });
        if (result.status !== "success" || !result.data) {
          throw new Error(result.message || "执行恢复失败");
        }
        toaster.toast({ title: "Freedeck", body: String(result.data.message || "恢复流程已结束") });
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setApplyingRestore(false);
        await refreshCloudSaveRestoreStatus();
      }
    },
    [refreshCloudSaveRestoreStatus],
  );

  const onStartCloudSaveRestore = useCallback(async () => {
    if (planningRestore || applyingRestore) return;
    if (!selectedRestoreGame || !selectedRestoreVersion) {
      toaster.toast({ title: "Freedeck", body: "请先选择要恢复的游戏和版本" });
      return;
    }
    if (!restoreEntries.length) {
      toaster.toast({ title: "Freedeck", body: "请先读取存档项" });
      return;
    }
    if (!selectedRestoreEntryIds.length) {
      toaster.toast({ title: "Freedeck", body: "请至少选择一个存档项" });
      return;
    }

    setPlanningRestore(true);
    try {
      const result = await planTianyiCloudSaveRestore({
        game_id: String(selectedRestoreGame.game_id || ""),
        game_key: String(selectedRestoreGame.game_key || ""),
        game_title: String(selectedRestoreGame.game_title || ""),
        version_name: String(selectedRestoreVersion || ""),
        selected_entry_ids: selectedRestoreEntryIds,
        target_dir: String(selectedRestoreTargetDir || ""),
      });
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "生成恢复计划失败");
      }

      const plan = result.data;
      const nextCandidates = Array.isArray(plan.target_candidates) ? plan.target_candidates : [];
      setTargetCandidates(nextCandidates);
      if (nextCandidates.length === 1) {
        setSelectedRestoreTargetDir(nextCandidates[0]);
      } else if (nextCandidates.length > 1 && !selectedRestoreTargetDir) {
        setSelectedRestoreTargetDir(nextCandidates[0]);
      }

      if (!plan.accepted) {
        toaster.toast({ title: "Freedeck", body: String(plan.message || "请先完成恢复前置步骤") });
        await refreshCloudSaveRestoreStatus();
        return;
      }

      const planId = String(plan.plan_id || "");
      if (!planId) {
        throw new Error("恢复计划缺少 plan_id");
      }
      if (plan.requires_confirmation) {
        const samples = Array.isArray(plan.conflict_samples) ? plan.conflict_samples.slice(0, 5) : [];
        showModal(
          <ConfirmModal
            strTitle="确认覆盖存档"
            strDescription={
              `检测到 ${Number(plan.conflict_count || 0)} 个冲突文件，确认后会覆盖原有存档。` +
              (samples.length ? `\n\n示例：\n${samples.join("\n")}` : "")
            }
            strOKButtonText="确认覆盖"
            strCancelButtonText="取消"
            onOK={() => {
              void runRestoreApply(planId, true);
            }}
            onCancel={() => {
              void runRestoreApply(planId, false);
            }}
          />,
        );
      } else {
        await runRestoreApply(planId, false);
      }
      await refreshCloudSaveRestoreStatus();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setPlanningRestore(false);
    }
  }, [
    applyingRestore,
    planningRestore,
    refreshCloudSaveRestoreStatus,
    restoreEntries.length,
    runRestoreApply,
    selectedRestoreEntryIds,
    selectedRestoreGame,
    selectedRestoreTargetDir,
    selectedRestoreVersion,
  ]);

  const cloudSaveRestoreSummaryText = useMemo(() => {
    const current = cloudSaveRestoreState;
    if (current.running) {
      const stage = cloudSaveStageText(current.stage);
      return `${stage}：已恢复 ${current.restored_files} 个文件 / ${current.restored_entries} 个存档项`;
    }
    const last = current.last_result;
    if (last && String(last.status || "").trim()) {
      return `最近一次：${String(last.message || last.status || "未开始")}`;
    }
    return "尚未执行云存档恢复";
  }, [cloudSaveRestoreState]);

  const restoreBusy =
    loadingRestoreOptions ||
    loadingRestoreEntries ||
    planningRestore ||
    applyingRestore ||
    cloudSaveRestoreState.running;

  return (
    <ConfirmModal
      strTitle="下载云存档"
      strOKButtonText="关闭"
      strCancelButtonText="取消"
      onOK={() => {
        props.closeModal?.();
      }}
      onCancel={() => {
        props.closeModal?.();
      }}
    >
      <>
        {!props.loggedIn ? (
          <div style={{ fontSize: "12px", lineHeight: 1.6 }}>请先登录天翼云账号，再下载和恢复云存档。</div>
        ) : (
          <>
            <div style={{ fontSize: "12px", lineHeight: 1.6, marginBottom: "10px" }}>{cloudSaveRestoreSummaryText}</div>
            <div style={{ fontSize: "11px", lineHeight: 1.5, opacity: 0.88, marginBottom: "10px" }}>
              {`${cloudSaveRestoreState.message || "未开始"} | ${clampProgress(cloudSaveRestoreState.progress).toFixed(1)}%`}
            </div>
            <div style={{ display: "grid", gap: "10px", maxHeight: "56vh", overflowY: "auto" }}>
              <ButtonItem layout="below" onClick={() => void refreshRestoreOptions()} disabled={restoreBusy}>
                {loadingRestoreOptions ? "刷新中..." : "刷新云存档列表"}
              </ButtonItem>
              <DropdownItem
                label="选择游戏"
                description="按游戏分组显示云端版本"
                rgOptions={restoreGameOptions}
                selectedOption={selectedRestoreGameKey}
                disabled={restoreBusy || restoreGameOptions.length <= 0}
                onChange={(option) => setSelectedRestoreGameKey(String(option?.data || ""))}
              />
              <DropdownItem
                label="选择版本时间"
                description="按时间倒序"
                rgOptions={restoreVersionOptions}
                selectedOption={selectedRestoreVersion}
                disabled={restoreBusy || !selectedRestoreGame || restoreVersionOptions.length <= 0}
                onChange={(option) => setSelectedRestoreVersion(String(option?.data || ""))}
              />
              <ButtonItem
                layout="below"
                onClick={onLoadRestoreEntries}
                disabled={restoreBusy || !selectedRestoreGame || !selectedRestoreVersion}
              >
                {loadingRestoreEntries ? "读取中..." : "读取可选存档项"}
              </ButtonItem>

              {restoreEntries.length > 0 &&
                restoreEntries.map((entry) => {
                  const entryId = String(entry.entry_id || "");
                  const checked = selectedRestoreEntryIds.includes(entryId);
                  return (
                    <div
                      key={`restore_entry_${entryId}`}
                      style={{
                        padding: "8px 10px",
                        background: "rgba(255,255,255,0.04)",
                        border: "1px solid rgba(255,255,255,0.06)",
                      }}
                    >
                      <ToggleField
                        label={`${entry.entry_name || entryId}${entry.file_count ? `（${entry.file_count} 文件）` : ""}`}
                        checked={checked}
                        onChange={(value: boolean) => toggleRestoreEntry(entryId, value)}
                        disabled={restoreBusy}
                      />
                    </div>
                  );
                })}

              {restoreEntries.length > 0 && (
                <div style={{ display: "flex", gap: "8px", width: "100%" }}>
                  <ButtonItem
                    layout="below"
                    onClick={() => {
                      setSelectedRestoreEntryIds(
                        restoreEntries.map((item) => String(item.entry_id || "")).filter(Boolean),
                      );
                    }}
                    disabled={restoreBusy}
                  >
                    全选
                  </ButtonItem>
                  <ButtonItem layout="below" onClick={() => setSelectedRestoreEntryIds([])} disabled={restoreBusy}>
                    清空
                  </ButtonItem>
                </div>
              )}

              {targetCandidates.length > 1 && (
                <DropdownItem
                  label="恢复目标目录"
                  description="检测到多个候选目录，请明确选择"
                  rgOptions={targetCandidates.map((path) => ({ data: path, label: path }))}
                  selectedOption={selectedRestoreTargetDir}
                  disabled={restoreBusy}
                  onChange={(option) => setSelectedRestoreTargetDir(String(option?.data || ""))}
                />
              )}

              <ButtonItem
                layout="below"
                onClick={onStartCloudSaveRestore}
                disabled={!selectedRestoreGame || !selectedRestoreVersion || !selectedRestoreEntryIds.length || restoreBusy}
              >
                {planningRestore ? "规划中..." : applyingRestore ? "恢复中..." : "下载并恢复云存档"}
              </ButtonItem>
            </div>
          </>
        )}
      </>
    </ConfirmModal>
  );
}

function SteamExeSelectModal(props: {
  taskTitle: string;
  installPath: string;
  candidates: string[];
  onConfirm: (exeRelPath: string) => void;
}) {
  const options = useMemo(
    () =>
      (Array.isArray(props.candidates) ? props.candidates : [])
        .map((rel) => String(rel || "").trim())
        .filter(Boolean)
        .map((rel) => ({ data: rel, label: rel })),
    [props.candidates],
  );
  const [selected, setSelected] = useState<string>(() => String(options[0]?.data || ""));
  useEffect(() => {
    setSelected(String(options[0]?.data || ""));
  }, [options]);

  const description = `检测到多个可执行文件，请选择要加入 Steam 的启动程序。\n\n游戏：${props.taskTitle || "-"}\n安装目录：${
    props.installPath || "-"
  }\n\n提示：已自动排除 uninstall.exe`;

  return (
    <ConfirmModal
      strTitle="选择启动程序"
      strDescription={description}
      strOKButtonText="加入 Steam"
      strCancelButtonText="稍后"
      onOK={() => {
        if (!selected) return;
        props.onConfirm(selected);
      }}
      onCancel={() => {}}
    >
      <DropdownItem
        label="启动程序"
        rgOptions={options}
        selectedOption={selected}
        onChange={(option) => setSelected(String(option?.data || ""))}
      />
    </ConfirmModal>
  );
}

function StorageMountSelectModal(props: {
  title: string;
  description: string;
  mounts: StorageMount[];
  onConfirm: (mount: StorageMount) => void;
}) {
  const options = useMemo(
    () =>
      (Array.isArray(props.mounts) ? props.mounts : [])
        .map((mount) => ({
          data: String(mount.path || "").trim(),
          label: (() => {
            const label = String(mount.label || mount.path || "").trim();
            const free = Number(mount.free_bytes || 0);
            const total = Number(mount.total_bytes || 0);
            if (free > 0 && total > 0) {
              return `${label}（可用 ${formatBytes(free)} / ${formatBytes(total)}）`;
            }
            return label || String(mount.path || "").trim();
          })(),
        }))
        .filter((item) => Boolean(item.data)),
    [props.mounts],
  );
  const [selected, setSelected] = useState<string>(() => String(options[0]?.data || ""));
  useEffect(() => {
    setSelected(String(options[0]?.data || ""));
  }, [options]);

  return (
    <ConfirmModal
      strTitle={props.title}
      strDescription={props.description}
      strOKButtonText="使用此设备"
      strCancelButtonText="取消"
      onOK={() => {
        const path = String(selected || "").trim();
        if (!path) return;
        const mount = (Array.isArray(props.mounts) ? props.mounts : []).find((item) => String(item.path || "") === path);
        if (!mount) return;
        props.onConfirm(mount);
      }}
      onCancel={() => {}}
    >
      <DropdownItem
        label="外接设备"
        rgOptions={options}
        selectedOption={selected}
        onChange={(option) => setSelected(String(option?.data || ""))}
      />
    </ConfirmModal>
  );
}

function openExternalUrl(rawUrl: string): void {
  const url = String(rawUrl || "").trim();
  if (!url) return;

  try {
    Navigation.NavigateToExternalWeb(url);
    return;
  } catch {
    // 忽略并回退到备用跳转。
  }

  try {
    const steamClient = (window as unknown as {
      SteamClient?: { Browser?: { OpenUrl?: (u: string) => void } };
    }).SteamClient;
    if (steamClient?.Browser?.OpenUrl) {
      steamClient.Browser.OpenUrl(url);
      return;
    }
  } catch {
    // 忽略并回退到 window.open。
  }

  const popup = window.open(url, "_blank");
  if (!popup) window.location.href = url;
}

function withQuery(rawUrl: string, query: Record<string, string | number | boolean | null | undefined>): string {
  const urlText = String(rawUrl || "").trim();
  if (!urlText) return "";
  try {
    const url = new URL(urlText);
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) continue;
      const encoded = typeof value === "boolean" ? (value ? "1" : "0") : String(value);
      url.searchParams.set(key, encoded);
    }
    return url.toString();
  } catch {
    return urlText;
  }
}

function describeOpenError(result: ApiResponse<UrlResponse>, fallback: string): string {
  const message = String(result.message || fallback);
  const reason = String(result.reason || "").trim();
  if (!reason) return message;
  if (!result.diagnostics) return `${message}（${reason}）`;
  try {
    return `${message}（${reason}）\n${JSON.stringify(result.diagnostics)}`;
  } catch {
    return `${message}（${reason}）`;
  }
}

function toPayload(settings: SettingsState): SettingsPayload {
  return {
    download_dir: String(settings.download_dir || ""),
    install_dir: String(settings.install_dir || ""),
    emulator_dir: String(settings.emulator_dir || ""),
    split_count: Math.max(1, Math.min(64, Number(settings.split_count || 16))),
    aria2_fast_mode: Boolean(settings.aria2_fast_mode),
    force_ipv4: Boolean(settings.force_ipv4),
    auto_switch_line: Boolean(settings.auto_switch_line),
    page_size: Math.max(10, Math.min(200, Number(settings.page_size || 50))),
    auto_delete_package: Boolean(settings.auto_delete_package),
    auto_install: true,
    lsfg_enabled: Boolean(settings.lsfg_enabled),
    show_playtime_widget: Boolean(settings.show_playtime_widget),
    cloud_save_auto_upload: Boolean(settings.cloud_save_auto_upload),
  };
}

function normalizeCloudSaveUploadState(raw: Partial<CloudSaveUploadState> | undefined): CloudSaveUploadState {
  const source = raw || {};
  const lastRaw = source.last_result || EMPTY_CLOUD_SAVE_STATE.last_result;
  return {
    ...EMPTY_CLOUD_SAVE_STATE,
    ...source,
    stage: String(source.stage || "idle"),
    message: String(source.message || "未开始"),
    reason: String(source.reason || ""),
    running: Boolean(source.running),
    progress: clampProgress(Number(source.progress || 0)),
    current_game: String(source.current_game || ""),
    total_games: Math.max(0, Number(source.total_games || 0)),
    processed_games: Math.max(0, Number(source.processed_games || 0)),
    uploaded: Math.max(0, Number(source.uploaded || 0)),
    skipped: Math.max(0, Number(source.skipped || 0)),
    failed: Math.max(0, Number(source.failed || 0)),
    results: Array.isArray(source.results) ? source.results : [],
    last_result: {
      ...EMPTY_CLOUD_SAVE_STATE.last_result,
      ...lastRaw,
      stage: String(lastRaw.stage || ""),
      reason: String(lastRaw.reason || ""),
      message: String(lastRaw.message || ""),
      timestamp: String(lastRaw.timestamp || ""),
      started_at: Math.max(0, Number(lastRaw.started_at || 0)),
      finished_at: Math.max(0, Number(lastRaw.finished_at || 0)),
      total_games: Math.max(0, Number(lastRaw.total_games || 0)),
      processed_games: Math.max(0, Number(lastRaw.processed_games || 0)),
      uploaded: Math.max(0, Number(lastRaw.uploaded || 0)),
      skipped: Math.max(0, Number(lastRaw.skipped || 0)),
      failed: Math.max(0, Number(lastRaw.failed || 0)),
      results: Array.isArray(lastRaw.results) ? lastRaw.results : [],
    },
  };
}

function normalizeCloudSaveRestoreState(raw: Partial<CloudSaveRestoreState> | undefined): CloudSaveRestoreState {
  const source = raw || {};
  const lastRaw = source.last_result || EMPTY_CLOUD_SAVE_RESTORE_STATE.last_result;
  return {
    ...EMPTY_CLOUD_SAVE_RESTORE_STATE,
    ...source,
    stage: String(source.stage || "idle"),
    message: String(source.message || "未开始"),
    reason: String(source.reason || ""),
    running: Boolean(source.running),
    progress: clampProgress(Number(source.progress || 0)),
    target_game_id: String(source.target_game_id || ""),
    target_game_title: String(source.target_game_title || ""),
    target_game_key: String(source.target_game_key || ""),
    target_version: String(source.target_version || ""),
    selected_entry_ids: Array.isArray(source.selected_entry_ids) ? source.selected_entry_ids : [],
    selected_target_dir: String(source.selected_target_dir || ""),
    requires_confirmation: Boolean(source.requires_confirmation),
    conflict_count: Math.max(0, Number(source.conflict_count || 0)),
    conflict_samples: Array.isArray(source.conflict_samples) ? source.conflict_samples : [],
    restored_files: Math.max(0, Number(source.restored_files || 0)),
    restored_entries: Math.max(0, Number(source.restored_entries || 0)),
    results: Array.isArray(source.results) ? source.results : [],
    last_result: {
      ...EMPTY_CLOUD_SAVE_RESTORE_STATE.last_result,
      ...lastRaw,
      status: String(lastRaw.status || ""),
      reason: String(lastRaw.reason || ""),
      message: String(lastRaw.message || ""),
      target_dir: String(lastRaw.target_dir || ""),
      restored_files: Math.max(0, Number(lastRaw.restored_files || 0)),
      restored_entries: Math.max(0, Number(lastRaw.restored_entries || 0)),
      conflicts_overwritten: Math.max(0, Number(lastRaw.conflicts_overwritten || 0)),
      results: Array.isArray(lastRaw.results) ? lastRaw.results : [],
    },
  };
}

function normalizeRuntimeRepairState(raw: Partial<RuntimeRepairState> | undefined): RuntimeRepairState {
  const source = raw || {};
  const lastRaw = source.last_result || EMPTY_RUNTIME_REPAIR_STATE.last_result;
  return {
    ...EMPTY_RUNTIME_REPAIR_STATE,
    ...source,
    stage: String(source.stage || "idle"),
    message: String(source.message || "未开始"),
    reason: String(source.reason || ""),
    running: Boolean(source.running),
    progress: clampProgress(Number(source.progress || 0)),
    total_games: Math.max(0, Number(source.total_games || 0)),
    processed_games: Math.max(0, Number(source.processed_games || 0)),
    total_steps: Math.max(0, Number(source.total_steps || 0)),
    completed_steps: Math.max(0, Number(source.completed_steps || 0)),
    succeeded_steps: Math.max(0, Number(source.succeeded_steps || 0)),
    skipped_steps: Math.max(0, Number(source.skipped_steps || 0)),
    failed_steps: Math.max(0, Number(source.failed_steps || 0)),
    current_game_id: String(source.current_game_id || ""),
    current_game_title: String(source.current_game_title || ""),
    current_package_id: String(source.current_package_id || ""),
    current_package_label: String(source.current_package_label || ""),
    results: Array.isArray(source.results) ? source.results : [],
    last_result: {
      ...EMPTY_RUNTIME_REPAIR_STATE.last_result,
      ...lastRaw,
      stage: String(lastRaw.stage || ""),
      reason: String(lastRaw.reason || ""),
      message: String(lastRaw.message || ""),
      started_at: Math.max(0, Number(lastRaw.started_at || 0)),
      finished_at: Math.max(0, Number(lastRaw.finished_at || 0)),
      duration_seconds: Math.max(0, Number(lastRaw.duration_seconds || 0)),
      total_games: Math.max(0, Number(lastRaw.total_games || 0)),
      processed_games: Math.max(0, Number(lastRaw.processed_games || 0)),
      total_steps: Math.max(0, Number(lastRaw.total_steps || 0)),
      completed_steps: Math.max(0, Number(lastRaw.completed_steps || 0)),
      succeeded_steps: Math.max(0, Number(lastRaw.succeeded_steps || 0)),
      skipped_steps: Math.max(0, Number(lastRaw.skipped_steps || 0)),
      failed_steps: Math.max(0, Number(lastRaw.failed_steps || 0)),
      results: Array.isArray(lastRaw.results) ? lastRaw.results : [],
    },
  };
}

function formatBytes(value: number): string {
  let size = Number(value || 0);
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

function cloudSaveStageText(stage: string): string {
  const value = String(stage || "").trim().toLowerCase();
  if (value === "listing") return "拉取版本中";
  if (value === "planning") return "规划中";
  if (value === "ready") return "待确认";
  if (value === "applying") return "恢复中";
  if (value === "scanning") return "扫描中";
  if (value === "packaging") return "打包中";
  if (value === "uploading") return "上传中";
  if (value === "completed") return "已完成";
  if (value === "failed") return "失败";
  return "空闲";
}

function cloudSaveItemStatusText(status: string): string {
  const value = String(status || "").trim().toLowerCase();
  if (value === "uploaded") return "成功";
  if (value === "skipped") return "跳过";
  if (value === "failed") return "失败";
  return value || "-";
}

function cloudSaveItemReasonText(item: CloudSaveUploadItem): string {
  const reason = String(item.reason || "").trim();
  if (!reason) return "";
  const value = reason.toLowerCase();
  let base = reason;
  if (value === "prefix_unresolved") base = "前缀未解析";
  else if (value === "save_path_not_found") base = "未找到存档目录";
  else if (value === "title_id_missing") base = "缺少 Switch Title ID";
  else if (value === "eden_data_root_unresolved") base = "未找到 Eden 存档根目录";
  else if (value === "package_failed") base = "打包失败";
  else if (value === "upload_failed") base = "上传失败";
  else if (value === "unexpected_error") base = "未知错误";

  if (value === "prefix_unresolved") {
    const diag = (item.diagnostics || {}) as Record<string, unknown>;
    const prefix = (diag["prefix_unresolved"] || diag["compat"] || {}) as Record<string, unknown>;
    const sub = String(prefix["reason"] || "").trim();
    if (sub) return `${base}（${sub}）`;
  }
  return base;
}

async function copyToClipboard(text: string): Promise<boolean> {
  const value = String(text || "");
  if (!value) return false;
  try {
    await navigator.clipboard.writeText(value);
    return true;
  } catch {
    // ignore
  }
  try {
    const textarea = document.createElement("textarea");
    textarea.value = value;
    textarea.style.position = "fixed";
    textarea.style.left = "-9999px";
    textarea.style.top = "0";
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(textarea);
    return ok;
  } catch {
    return false;
  }
}

type AnyFunction = (...args: unknown[]) => unknown;

interface PatchHandle {
  unpatch: () => void;
}

const UI_PREFS_STORAGE_KEY = "freedeck_ui_prefs_v1";

type UiPrefs = {
  show_playtime_widget: boolean;
};

const uiPrefsState: UiPrefs = {
  show_playtime_widget: true,
};

const uiPrefsListeners = new Set<() => void>();

function hydrateUiPrefsFromStorage(): void {
  if (typeof window === "undefined") return;
  try {
    const stored = window.localStorage?.getItem(UI_PREFS_STORAGE_KEY);
    if (!stored) return;
    const parsed = JSON.parse(stored);
    if (!parsed || typeof parsed !== "object") return;
    const map = parsed as Record<string, unknown>;
    if (typeof map.show_playtime_widget !== "undefined") {
      uiPrefsState.show_playtime_widget = Boolean(map.show_playtime_widget);
    }
  } catch {
    // 忽略解析失败，回退默认值。
  }
}

function persistUiPrefsToStorage(): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage?.setItem(UI_PREFS_STORAGE_KEY, JSON.stringify(uiPrefsState));
  } catch {
    // 忽略存储失败。
  }
}

function setUiPrefs(patch: Partial<UiPrefs>, persist: boolean = true): void {
  let changed = false;
  if (typeof patch.show_playtime_widget !== "undefined") {
    const next = Boolean(patch.show_playtime_widget);
    if (next !== uiPrefsState.show_playtime_widget) {
      uiPrefsState.show_playtime_widget = next;
      changed = true;
    }
  }
  if (!changed) return;
  if (persist) persistUiPrefsToStorage();
  for (const listener of Array.from(uiPrefsListeners)) {
    try {
      listener();
    } catch {
      // 忽略监听器异常。
    }
  }
}

function setUiPrefsFromSettings(settings: Partial<SettingsState> | null | undefined): void {
  if (!settings || typeof settings !== "object") return;
  if (typeof settings.show_playtime_widget === "undefined") return;
  setUiPrefs({ show_playtime_widget: Boolean(settings.show_playtime_widget) }, true);
}

function useUiPrefShowPlaytimeWidget(): boolean {
  const [, forceRender] = useState<number>(0);
  useEffect(() => {
    const listener = () => forceRender((prev) => prev + 1);
    uiPrefsListeners.add(listener);
    return () => {
      uiPrefsListeners.delete(listener);
    };
  }, []);
  return Boolean(uiPrefsState.show_playtime_widget);
}

hydrateUiPrefsFromStorage();

const LIBRARY_TIME_CACHE_TTL_MS = 5 * 60 * 1000;
const LIBRARY_TIME_NEGATIVE_CACHE_TTL_MS = 3 * 1000;
const LIBRARY_TIME_RETRY_MS = 3 * 1000;
const LIBRARY_TIME_REFRESH_MS = 2 * 60 * 1000;
const libraryTimeCache = new Map<string, { updatedAt: number; payload: LibraryGameTimeStatsData }>();

interface SteamAppsGameActionApi {
  RegisterForGameActionStart?: (cb: (...args: unknown[]) => void) => { unregister?: () => void };
  RegisterForGameActionEnd?: (cb: (...args: unknown[]) => void) => { unregister?: () => void };
  GetRunningApps?: () => unknown;
  GetRunningAppIDs?: () => unknown;
  GetRunningAppIds?: () => unknown;
  GetRunningAppID?: () => unknown;
  GetRunningAppId?: () => unknown;
}

function parseGameActionAppIdCandidate(value: unknown): number {
  const parseAppIdFromBigInt = (input: bigint): number => {
    if (input === 0n) return 0;
    if (input < 0n) {
      const unsigned = Number(BigInt.asUintN(32, input));
      return unsigned > 0 ? unsigned : 0;
    }
    if (input <= 0xffffffffn) {
      return Number(input) >>> 0;
    }

    // Steam 有时会给出 CGameID（64-bit）。
    // - 低 32 位：AppID(低 24 位) + type(高 8 位)
    // - 高 32 位：modid / shortcut id
    // 对 shortcut(type=2)，真正的「非 Steam AppID」存放在高 32 位。
    const low32 = input & 0xffffffffn;
    const high32 = Number((input >> 32n) & 0xffffffffn) >>> 0;

    const type = Number((low32 >> 24n) & 0xffn);
    if (type === 2 && high32 > 0) return high32;

    const lowVal = Number(low32) >>> 0;
    if (type === 0 && lowVal > 0) return lowVal;

    const appId24 = Number(low32 & 0xffffffn) >>> 0;
    if (appId24 > 0) return appId24;

    if (high32 > 0) return high32;
    return lowVal > 0 ? lowVal : 0;
  };

  if (typeof value === "bigint") {
    return parseAppIdFromBigInt(value);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const truncated = Math.trunc(value);
    if (!Number.isFinite(truncated) || truncated === 0) return 0;
    if (Number.isSafeInteger(truncated) && Math.abs(truncated) > 0xffffffff) {
      try {
        const parsed = parseAppIdFromBigInt(BigInt(truncated));
        if (parsed > 0) return parsed;
      } catch {
        // ignore
      }
    }
    // Steam 非 Steam shortcut 的 appid 可能是带符号 int32（高位为 1 时会变成负数）。
    // 统一转换为 uint32，确保 start/end 事件能正确映射到 shortcuts.vdf 的 appid_unsigned。
    return truncated >>> 0;
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (/^-?\d+$/.test(text)) {
      try {
        const parsed = parseAppIdFromBigInt(BigInt(text));
        if (parsed > 0) return parsed;
      } catch {
        // ignore and fall back to parseInt
      }
      const parsed = Number.parseInt(text, 10);
      if (!Number.isFinite(parsed) || parsed === 0) return 0;
      return Math.trunc(parsed) >>> 0;
    }
    return 0;
  }
  if (!value || typeof value !== "object") {
    return 0;
  }

  const obj = value as Record<string, unknown>;
  const keys = [
    "app_id",
    "appid",
    "appId",
    "unAppID",
    "nAppID",
    "m_unAppID",
    "m_nAppID",
    "gameid",
    "gameId",
    "game_id",
    "m_ulGameID",
    "m_gameID",
  ];
  for (const key of keys) {
    const parsed = parseGameActionAppIdCandidate(obj[key]);
    if (parsed > 0) return parsed;
  }
  return 0;
}

function resolveGameActionAppId(args: unknown[]): string {
  const ordered = [args[1], args[0], args[2], ...args];
  for (const value of ordered) {
    const parsed = parseGameActionAppIdCandidate(value);
    if (parsed > 0) return String(parsed);
  }
  return "";
}

function parseLibraryRouteAppIdFromLocation(): number {
  if (typeof window === "undefined") return 0;
  const href = String(window.location?.href || "");
  const hash = String(window.location?.hash || "");
  const search = `${href} ${hash}`;
  const match = search.match(/(?:#\/|\/)library\/app\/(-?\d+)/i) || search.match(/\blibrary\/app\/(-?\d+)/i);
  if (!match) return 0;
  return parseGameActionAppIdCandidate(String(match[1] || "").trim());
}

function parseLibraryRouteAppIdFromArgs(args: unknown[]): number {
  const candidates = [...args];
  for (const candidate of candidates) {
    if (typeof candidate === "number" || typeof candidate === "string") {
      const parsed = parseGameActionAppIdCandidate(candidate);
      if (parsed > 0) return parsed;
      continue;
    }
    if (!candidate || typeof candidate !== "object") continue;
    const map = candidate as Record<string, unknown>;
    const match = map.match as Record<string, unknown> | undefined;
    const params = (match?.params as Record<string, unknown> | undefined) ?? (map.params as Record<string, unknown> | undefined);
    const paramAppId = params ? parseGameActionAppIdCandidate(params.appid ?? params.appId ?? params.app_id) : 0;
    if (paramAppId > 0) return paramAppId;
    const routeProps = (map.routeProps as Record<string, unknown> | undefined) ?? (map.route_props as Record<string, unknown> | undefined);
    if (routeProps && typeof routeProps === "object") {
      const routeMatch = (routeProps.match as Record<string, unknown> | undefined) ?? undefined;
      const routeParams =
        (routeMatch?.params as Record<string, unknown> | undefined) ?? (routeProps.params as Record<string, unknown> | undefined);
      const routeParamAppId = routeParams ? parseGameActionAppIdCandidate(routeParams.appid ?? routeParams.appId ?? routeParams.app_id) : 0;
      if (routeParamAppId > 0) return routeParamAppId;
    }
    const direct = parseGameActionAppIdCandidate(map.appid ?? map.appId ?? map.app_id);
    if (direct > 0) return direct;
  }
  return 0;
}

function invalidateLibraryTimeCacheByAppId(appId: string): void {
  const prefix = `${String(appId || "").trim()}::`;
  if (!prefix || prefix === "::") return;
  for (const key of Array.from(libraryTimeCache.keys())) {
    if (key.startsWith(prefix)) {
      libraryTimeCache.delete(key);
    }
  }
}

function resolveSteamAppsGameActionApi(): SteamAppsGameActionApi | null {
  if (typeof window === "undefined") return null;
  const apps = (
    window as unknown as {
      SteamClient?: { Apps?: SteamAppsGameActionApi };
    }
  ).SteamClient?.Apps;
  if (!apps || typeof apps !== "object") return null;
  return apps;
}

const RUNNING_APP_POLL_MS = 3500;

async function callSteamAppsApi(apps: SteamAppsGameActionApi, methodName: string): Promise<unknown> {
  const fn = (apps as unknown as Record<string, unknown>)[methodName];
  if (typeof fn !== "function") return null;
  try {
    const result = (fn as AnyFunction).call(apps);
    if (result && typeof (result as { then?: unknown }).then === "function") {
      return await (result as Promise<unknown>);
    }
    return result;
  } catch {
    return null;
  }
}

function extractRunningAppCandidates(payload: unknown): unknown[] {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== "object") return [];
  const map = payload as Record<string, unknown>;
  const keys = ["running_apps", "runningApps", "apps", "appids", "appIds", "app_ids", "rgRunningApps", "running"];
  for (const key of keys) {
    const value = map[key];
    if (Array.isArray(value)) return value;
  }
  return [];
}

async function resolveRunningAppIds(apps: SteamAppsGameActionApi): Promise<string[]> {
  const methods = ["GetRunningApps", "GetRunningAppIDs", "GetRunningAppIds", "GetRunningAppID", "GetRunningAppId"];
  let payload: unknown = null;
  for (const method of methods) {
    const next = await callSteamAppsApi(apps, method);
    if (next !== null && typeof next !== "undefined") {
      payload = next;
      break;
    }
  }

  const candidates = extractRunningAppCandidates(payload);
  const resolved: string[] = [];
  for (const value of candidates) {
    const parsed = parseGameActionAppIdCandidate(value);
    if (parsed > 0) resolved.push(String(parsed));
  }
  if (resolved.length === 0) {
    const single = parseGameActionAppIdCandidate(payload);
    if (single > 0) resolved.push(String(single));
  }

  const unique: string[] = [];
  const seen = new Set<string>();
  for (const appId of resolved) {
    if (seen.has(appId)) continue;
    seen.add(appId);
    unique.push(appId);
  }
  return unique;
}

function installGlobalGameActionReporter(): () => void {
  let disposed = false;
  let retryTimer: number | null = null;
  let pollTimer: number | null = null;
  let pollInFlight = false;
  let teardownListener: (() => void) | null = null;

  const activeAppIds = new Set<string>();
  const runningAppIds = new Set<string>();

  const clearRetry = () => {
    if (retryTimer === null) return;
    if (typeof window !== "undefined") {
      window.clearTimeout(retryTimer);
    }
    retryTimer = null;
  };

  const scheduleRetry = () => {
    if (disposed || retryTimer !== null || typeof window === "undefined") return;
    retryTimer = window.setTimeout(() => {
      retryTimer = null;
      tryInstall();
    }, 1500);
  };

  const clearPoll = () => {
    if (pollTimer === null) return;
    window.clearInterval(pollTimer);
    pollTimer = null;
  };

  const reportAction = (phase: "start" | "end", args: unknown[]) => {
    const appId = resolveGameActionAppId(args);
    if (!appId) return;
    const appIdNum = Number(appId);
    // 仅记录非 Steam shortcut（高位为 1）的启动/退出事件，避免影响正版 Steam 游戏体验。
    if (Number.isFinite(appIdNum) && appIdNum < 0x80000000) return;

    if (phase === "start") {
      if (activeAppIds.has(appId)) return;
      activeAppIds.add(appId);
    } else {
      activeAppIds.delete(appId);
    }

    invalidateLibraryTimeCacheByAppId(appId);
    void recordTianyiGameAction({
      phase,
      app_id: appId,
      action_name: "",
    }).catch(() => {
      // 忽略事件上报失败，避免影响主流程。
    });
  };

  const pollRunningApps = async () => {
    if (disposed || pollInFlight) return;
    pollInFlight = true;
    try {
      const apps = resolveSteamAppsGameActionApi();
      if (!apps) return;
      const currentIds = await resolveRunningAppIds(apps);
      const currentSet = new Set<string>(currentIds);

      for (const appId of currentSet) {
        if (!appId) continue;
        if (runningAppIds.has(appId)) continue;
        runningAppIds.add(appId);
        reportAction("start", [appId]);
      }
      for (const appId of Array.from(runningAppIds)) {
        if (currentSet.has(appId)) continue;
        runningAppIds.delete(appId);
        reportAction("end", [appId]);
      }
    } catch {
      // 忽略轮询异常，避免影响插件主流程。
    } finally {
      pollInFlight = false;
    }
  };

  const syncRunningAppsOnce = () => {
    void pollRunningApps();
  };

  const startPollInterval = () => {
    if (disposed || pollTimer !== null || typeof window === "undefined") return;
    pollTimer = window.setInterval(() => {
      void pollRunningApps();
    }, RUNNING_APP_POLL_MS);
    void pollRunningApps();
  };

  const tryInstall = () => {
    if (disposed) return;

    let apps: SteamAppsGameActionApi | null = null;
    try {
      apps = resolveSteamAppsGameActionApi();
    } catch {
      scheduleRetry();
      return;
    }
    if (!apps) {
      scheduleRetry();
      return;
    }

    if (teardownListener) return;

    const registerStart = apps?.RegisterForGameActionStart;
    const registerEnd = apps?.RegisterForGameActionEnd;
    if (typeof registerStart !== "function" || typeof registerEnd !== "function") {
      startPollInterval();
      scheduleRetry();
      return;
    }

    let startListener: { unregister?: () => void } | undefined;
    let endListener: { unregister?: () => void } | undefined;
    try {
      startListener = registerStart((...args: unknown[]) => {
        reportAction("start", args);
      });
      endListener = registerEnd((...args: unknown[]) => {
        reportAction("end", args);
      });
    } catch {
      startPollInterval();
      scheduleRetry();
      return;
    }

    clearPoll();
    syncRunningAppsOnce();

    teardownListener = () => {
      activeAppIds.clear();
      runningAppIds.clear();
      try {
        startListener?.unregister?.();
      } catch {
        // 忽略反注册异常。
      }
      try {
        endListener?.unregister?.();
      } catch {
        // 忽略反注册异常。
      }
    };
  };

  tryInstall();
  return () => {
    disposed = true;
    clearRetry();
    clearPoll();
    if (teardownListener) {
      teardownListener();
      teardownListener = null;
    }
  };
}

function hasPositiveLibraryTimeSnapshot(payload: LibraryGameTimeStatsData | null | undefined): boolean {
  if (!payload) return false;
  const playtimeSeconds = Number(payload.my_playtime_seconds || 0);
  const lastPlayedAt = Number(payload.last_played_at || 0);
  return (
    Boolean(payload.my_playtime_active) ||
    (Number.isFinite(playtimeSeconds) && playtimeSeconds > 0) ||
    (Number.isFinite(lastPlayedAt) && lastPlayedAt > 0)
  );
}

function wrapReactType(node: unknown, prop = "type"): Record<string, unknown> | null {
  if (!node || typeof node !== "object") return null;
  const owner = node as Record<string, unknown>;
  const current = owner[prop];
  if (!current || typeof current !== "object") return null;
  const currentMap = current as Record<string, unknown>;
  if (Boolean(currentMap.__FREDECK_WRAPPED)) return currentMap;
  const wrapped: Record<string, unknown> = { ...currentMap, __FREDECK_WRAPPED: true };
  owner[prop] = wrapped;
  return wrapped;
}

function afterPatch(object: Record<string, unknown>, property: string, handler: (args: unknown[], ret: unknown) => unknown): PatchHandle {
  const original = object[property];
  if (typeof original !== "function") {
    return { unpatch: () => {} };
  }

  const originalFn = original as AnyFunction;
  const patched: AnyFunction = function patchedFunction(this: unknown, ...args: unknown[]) {
    const result = originalFn.apply(this, args);
    const next = handler.call(this, args, result);
    return typeof next === "undefined" ? result : next;
  };

  try {
    Object.assign(patched, originalFn);
  } catch {
    // 忽略函数属性拷贝失败。
  }
  try {
    Object.defineProperty(patched, "toString", {
      value: () => originalFn.toString(),
      configurable: true,
    });
  } catch {
    // 忽略 toString 覆盖失败。
  }

  object[property] = patched;
  return {
    unpatch: () => {
      if (object[property] === patched) {
        object[property] = original;
      }
    },
  };
}

function resolveSpliceTarget(children: unknown): unknown[] | null {
  if (!children || typeof children !== "object") return null;
  const root = children as Record<string, unknown>;
  const part1 = root.props as Record<string, unknown> | undefined;
  const part2 = part1?.children as unknown[];
  if (!Array.isArray(part2) || part2.length < 2) return null;
  const part3 = part2[1] as Record<string, unknown> | undefined;
  const part4 = part3?.props as Record<string, unknown> | undefined;
  const part5 = part4?.children as Record<string, unknown> | undefined;
  const part6 = part5?.props as Record<string, unknown> | undefined;
  const list = part6?.children;
  if (Array.isArray(list)) return list;

  const visited = new Set<object>();
  const queue: unknown[] = [children];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (Array.isArray(current)) {
      if (current.length > 0) {
        const hasOverviewCarrier = current.some((node) => {
          if (!node || typeof node !== "object") return false;
          const nodeProps = (node as Record<string, unknown>).props as Record<string, unknown> | undefined;
          const inner = (nodeProps?.children as Record<string, unknown> | undefined)?.props as Record<string, unknown> | undefined;
          return typeof inner?.overview !== "undefined" || typeof inner?.details !== "undefined";
        });
        if (hasOverviewCarrier) return current;
      }
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    for (const value of Object.values(map)) {
      if (!value) continue;
      if (typeof value === "object") queue.push(value);
    }
  }

  return null;
}

function findLibraryInsertIndex(nodes: unknown[]): number {
  return nodes.findIndex((child) => {
    if (!child || typeof child !== "object") return false;
    const props = (child as Record<string, unknown>).props as Record<string, unknown> | undefined;
    if (!props) return false;
    const hasFocusFlag = typeof props.childFocusDisabled !== "undefined";
    const hasNavRef = typeof props.navRef !== "undefined";
    const innerProps = (props.children as Record<string, unknown> | undefined)?.props as Record<string, unknown> | undefined;
    const hasDetails = typeof innerProps?.details !== "undefined";
    const hasOverview = typeof innerProps?.overview !== "undefined";
    const hasFastRender = typeof innerProps?.bFastRender !== "undefined";
    return hasFocusFlag && hasNavRef && hasDetails && hasOverview && hasFastRender;
  });
}

function resolveLibraryInsertIndex(nodes: unknown[]): number {
  const exact = findLibraryInsertIndex(nodes);
  if (exact >= 0) return exact;

  const firstNative = nodes.findIndex((child) => {
    if (!child || typeof child !== "object") return false;
    const key = String((child as Record<string, unknown>).key || "");
    return !key.startsWith("freedeck-library-times-");
  });
  if (firstNative >= 0) return Math.min(firstNative + 1, nodes.length);

  return Math.min(1, nodes.length);
}

const LIBRARY_TIME_BLOCK_ID = "freedeck-library-times";

function isLibraryTimeNode(child: unknown): boolean {
  if (!child || typeof child !== "object") return false;
  const key = String((child as Record<string, unknown>).key || "");
  if (key.startsWith("freedeck-library-times-")) return true;
  const typeMap = (child as Record<string, unknown>).type as Record<string, unknown> | undefined;
  const typeName = String(typeMap?.displayName || typeMap?.name || "");
  if (typeName === "LibraryTimeBlock") return true;
  const childProps = (child as Record<string, unknown>).props as Record<string, unknown> | undefined;
  return String(childProps?.id || "") === LIBRARY_TIME_BLOCK_ID;
}

function ensureNodeChildrenArray(node: Record<string, unknown>): unknown[] {
  const props = (node.props as Record<string, unknown> | undefined) || {};
  node.props = props;
  const children = props.children;
  if (Array.isArray(children)) return children;
  if (typeof children === "undefined") {
    const next: unknown[] = [];
    props.children = next;
    return next;
  }
  const next: unknown[] = [children];
  props.children = next;
  return next;
}

function findInReactTreeLike(node: unknown, filter: (value: unknown) => boolean): unknown | null {
  if (!node || typeof node !== "object") return null;
  const visited = new Set<object>();
  const queue: unknown[] = [node];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (filter(current)) return current;

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    const walkKeys = ["props", "children", "child", "sibling"];
    for (const key of walkKeys) {
      const value = map[key];
      if (value && typeof value === "object") queue.push(value);
    }
    for (const value of Object.values(map)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }
  return null;
}

function resolveStaticClassByKey(key: string): string {
  const root = staticClasses as unknown;
  if (!root || typeof root !== "object") return "";
  const visited = new Set<object>();
  const queue: unknown[] = [root];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    if (typeof map[key] === "string" && String(map[key]).trim()) {
      return String(map[key]).trim();
    }
    for (const value of Object.values(map)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }
  return "";
}

function resolveLibraryOverlayHost(tree: unknown): Record<string, unknown> | null {
  const topCapsuleClass = resolveStaticClassByKey("TopCapsule");
  const headerClass = resolveStaticClassByKey("Header");
  const appDetailsRootClass = resolveStaticClassByKey("AppDetailsRoot");
  const markers = [topCapsuleClass, appDetailsRootClass, headerClass].filter(Boolean);

  const classMatched = findInReactTreeLike(tree, (value) => {
    if (!value || typeof value !== "object") return false;
    const props = (value as Record<string, unknown>).props as Record<string, unknown> | undefined;
    if (!props) return false;
    const className = String(props.className || "");
    const children = props.children;
    if (!className || typeof children === "undefined") return false;
    return markers.some((marker) => className.includes(marker));
  });
  if (classMatched && typeof classMatched === "object") return classMatched as Record<string, unknown>;

  // 回退：命中包含 overview/details 的容器，确保至少能显示。
  const fallback = findInReactTreeLike(tree, (value) => {
    if (!value || typeof value !== "object") return false;
    const props = (value as Record<string, unknown>).props as Record<string, unknown> | undefined;
    if (!props) return false;
    const children = props.children;
    if (!children || typeof children !== "object" || Array.isArray(children)) return false;
    const inner = (children as Record<string, unknown>).props as Record<string, unknown> | undefined;
    return typeof inner?.overview !== "undefined" && typeof inner?.details !== "undefined";
  });
  return fallback && typeof fallback === "object" ? (fallback as Record<string, unknown>) : null;
}

function resolveLibraryPlaySectionHost(tree: unknown): Record<string, unknown> | null {
  if (!tree || typeof tree !== "object") return null;
  const visited = new Set<object>();
  const queue: unknown[] = [tree];
  let best: { node: Record<string, unknown>; score: number } | null = null;

  const containsPlayButton = (node: unknown): boolean => {
    return Boolean(
      findInReactTreeLike(node, (value) => {
        if (!value || typeof value !== "object") return false;
        const props = (value as Record<string, unknown>).props as Record<string, unknown> | undefined;
        const className = String(props?.className || "");
        return className.includes("PlayButton") || className.includes("ActionButton");
      }),
    );
  };

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    const props = map.props as Record<string, unknown> | undefined;
    const className = String(props?.className || "");
    const children = props?.children;
    if (
      className &&
      typeof children !== "undefined" &&
      !className.includes("PlayButton") &&
      !className.includes("ActionButton") &&
      (className.includes("PlaySection") ||
        className.includes("PlayControls") ||
        className.includes("PlayBar") ||
        className.includes("appdetailsplaysection_"))
    ) {
      if (containsPlayButton(map)) {
        let score = 0;
        if (className.includes("PlaySection")) score += 50;
        if (className.includes("PlayControls")) score += 35;
        if (className.includes("PlayBar")) score += 25;
        if (className.includes("appdetailsplaysection_")) score += 20;
        // 优先注入到 PlaySection 的 InnerContainer（布局更稳定、不会被外层裁切导致“看不见”）。
        if (className.includes("InnerContainer")) score += 60;
        const childCount = Array.isArray(children) ? children.length : 1;
        score += Math.min(20, childCount);
        if (!best || score > best.score) {
          best = { node: map, score };
        }
      }
    }

    const walkKeys = ["props", "children", "child", "sibling"];
    for (const key of walkKeys) {
      const value = map[key];
      if (value && typeof value === "object") queue.push(value);
    }
    for (const value of Object.values(map)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }

  return best?.node || null;
}

function resolveLibraryOverviewNode(tree: unknown): Record<string, unknown> | null {
  const hit = findInReactTreeLike(tree, (value) => {
    if (!value || typeof value !== "object") return false;
    const map = value as Record<string, unknown>;
    const appId = parseGameActionAppIdCandidate(map);
    if (!Number.isFinite(appId) || appId <= 0) return false;
    const title =
      String(map.display_name || "").trim() ||
      String(map.name || "").trim() ||
      String(map.app_name || "").trim() ||
      String(map.title || "").trim();
    return Boolean(title);
  });
  return hit && typeof hit === "object" ? (hit as Record<string, unknown>) : null;
}

function resolvePatchHandleUnpatch(handle: unknown): () => void {
  if (handle && typeof handle === "object" && typeof (handle as { unpatch?: unknown }).unpatch === "function") {
    return () => {
      try {
        (handle as { unpatch: () => void }).unpatch();
      } catch {
        // 忽略移除 patch 异常。
      }
    };
  }
  if (typeof handle === "function") {
    return () => {
      try {
        (handle as () => void)();
      } catch {
        // 忽略移除 patch 异常。
      }
    };
  }
  return () => {};
}

function LibraryTimeBlock({
  appId,
  title,
  detailsRef,
}: {
  appId: number;
  title: string;
  detailsRef?: Record<string, unknown> | null;
}) {
  const appIdText = String(Math.max(0, Number(appId || 0)));
  const titleText = String(title || "").trim();
  const showPlaytimeWidget = useUiPrefShowPlaytimeWidget();
  const cacheKey = `${appIdText}::${titleText.toLowerCase()}`;
  const initialCached = (() => {
    const cached = libraryTimeCache.get(cacheKey);
    if (!cached) return null;
    const ttl = hasPositiveLibraryTimeSnapshot(cached.payload) ? LIBRARY_TIME_CACHE_TTL_MS : LIBRARY_TIME_NEGATIVE_CACHE_TTL_MS;
    if (Date.now() - cached.updatedAt > ttl) {
      libraryTimeCache.delete(cacheKey);
      return null;
    }
    return cached.payload;
  })();

  const [data, setData] = useState<LibraryGameTimeStatsData | null>(initialCached);

  useEffect(() => {
    let alive = true;
    let timerId: number | null = null;
    let inFlight = false;

    const clearTimer = () => {
      if (timerId === null) return;
      window.clearTimeout(timerId);
      timerId = null;
    };

    if (!showPlaytimeWidget) {
      setData(null);
      return () => {
        alive = false;
        clearTimer();
      };
    }

    const scheduleNext = (payload: LibraryGameTimeStatsData | null) => {
      if (!alive) return;
      clearTimer();
      if (payload && payload.managed === false) return;
      const waitMs = payload?.my_playtime_active
        ? LIBRARY_TIME_RETRY_MS
        : hasPositiveLibraryTimeSnapshot(payload)
          ? LIBRARY_TIME_REFRESH_MS
          : LIBRARY_TIME_RETRY_MS;
      timerId = window.setTimeout(() => {
        void fetchStats();
      }, waitMs);
    };

    const fetchStats = async () => {
      if (!alive || inFlight) return;
      if (!appIdText || appIdText === "0") return;
      inFlight = true;
      try {
        const result = await withTimeout(
          getTianyiLibraryGameTimeStats({
            app_id: appIdText,
            title: titleText,
          }),
          11000,
          "library_time_stats_timeout",
        );
        if (!alive) return;
        if (result.status !== "success") {
          scheduleNext(null);
          return;
        }
        const payload = (result.data || {}) as LibraryGameTimeStatsData;
        libraryTimeCache.set(cacheKey, { updatedAt: Date.now(), payload });
        setData(payload);
        scheduleNext(payload);
      } catch {
        // 忽略库页面时长读取失败，不影响原页面。
        scheduleNext(null);
      } finally {
        inFlight = false;
      }
    };

    if (!appIdText || appIdText === "0") {
      setData(null);
      return () => {
        alive = false;
        clearTimer();
      };
    }

    const cached = libraryTimeCache.get(cacheKey);
    const cachedTtl = cached
      ? (hasPositiveLibraryTimeSnapshot(cached.payload) ? LIBRARY_TIME_CACHE_TTL_MS : LIBRARY_TIME_NEGATIVE_CACHE_TTL_MS)
      : 0;
    if (cached && Date.now() - cached.updatedAt <= cachedTtl) {
      setData(cached.payload);
      if (cached.payload.managed === false) {
        return () => {
          alive = false;
          clearTimer();
        };
      }
      // 正缓存按周期刷新；空缓存立即重查，避免首次进入库页显示太慢。
      if (hasPositiveLibraryTimeSnapshot(cached.payload)) {
        scheduleNext(cached.payload);
      } else {
        void fetchStats();
      }
      return () => {
        alive = false;
        clearTimer();
      };
    }

    void fetchStats();

    return () => {
      alive = false;
      clearTimer();
    };
  }, [showPlaytimeWidget, appIdText, cacheKey, titleText]);

  useEffect(() => {
    if (!detailsRef || typeof detailsRef !== "object") return;
    if (!data || data.managed === false) return;
    const seconds = Number(data.my_playtime_seconds || 0);
    if (!Number.isFinite(seconds) || seconds <= 0) return;
    // Steam details 的 nPlaytimeForever 单位为「分钟」，不是小时。
    // 之前写入小时会导致库页面显示成“几分钟”。
    const next = Math.floor(Math.max(0, seconds) / 60);
    const current = Number((detailsRef as { nPlaytimeForever?: unknown }).nPlaytimeForever);
    if (Number.isFinite(current) && Math.abs(current - next) < 0.01) return;
    try {
      (detailsRef as { nPlaytimeForever?: unknown }).nPlaytimeForever = next;
    } catch {
      // 忽略写入 Steam details 失败。
    }
  }, [detailsRef, data]);

  if (!showPlaytimeWidget) return null;
  if (data && data.managed === false) return null;

  const myPlaytime = formatPlaytimeText(data?.my_playtime_seconds || 0, data?.my_playtime_text);
  const lastPlayed = formatLastPlayedText(data?.last_played_at);
  const activeSuffix = data?.my_playtime_active ? "（进行中）" : "";
  const loading = !data;

  return (
    <div
      id={LIBRARY_TIME_BLOCK_ID}
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-start",
        gap: "2px",
        flex: "0 1 240px",
        minWidth: 0,
        maxWidth: "320px",
        padding: "4px 8px",
        borderRadius: 2,
        background: "rgba(0,0,0,0.20)",
        border: "1px solid rgba(255,255,255,0.10)",
        boxShadow: "none",
        overflow: "hidden",
        pointerEvents: "none",
        color: "rgba(255, 255, 255, 0.92)",
        textShadow: "0 1px 2px rgba(0,0,0,0.65)",
      }}
    >
      <div
        style={{
          fontSize: "13px",
          fontWeight: 700,
          lineHeight: 1.2,
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
        }}
      >
        {loading ? "已玩：读取中..." : `已玩：${myPlaytime}${activeSuffix}`}
      </div>
      <div
        style={{
          fontSize: "11px",
          opacity: 0.9,
          lineHeight: 1.2,
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
        }}
      >
        {loading ? "最后运行：读取中..." : `最后运行：${lastPlayed}`}
      </div>
    </div>
  );
}

function installLibraryPlaytimePatch(): () => void {
  const hookApi = routerHook as unknown as {
    addPatch?: (route: string, patcher: (props: unknown) => unknown) => unknown;
  };
  if (typeof hookApi.addPatch !== "function") {
    return () => {};
  }

  const patchHandle = hookApi.addPatch("/library/app/:appid", (props: unknown) => {
    if (!props || typeof props !== "object") return props;
    const propsMap = props as Record<string, unknown>;
    const childNode = propsMap.children as Record<string, unknown> | undefined;
    const childProps = childNode?.props as Record<string, unknown> | undefined;
    if (!childProps) return props;

    const renderFunc = childProps.renderFunc;
    if (typeof renderFunc !== "function") return props;
    if (Boolean((renderFunc as unknown as Record<string, unknown>).__FREDECK_LIBRARY_RENDER_PATCHED)) return props;

    const renderPatch = afterPatch(childProps, "renderFunc", (renderArgs, ret1) => {
      if (!ret1 || typeof ret1 !== "object") return ret1;
      const routeAppId = parseLibraryRouteAppIdFromArgs(renderArgs) || parseLibraryRouteAppIdFromLocation();
      if (!routeAppId) return ret1;
      // 仅对非 Steam shortcut（高位为 1）注入组件，避免对正版 Steam 游戏库页面造成闪烁/抽动。
      if (routeAppId < 0x80000000) return ret1;

      if (!uiPrefsState.show_playtime_widget) return ret1;

      const ret1Map = ret1 as Record<string, unknown>;
      const overviewNode = resolveLibraryOverviewNode(ret1Map);
      const gameTitle = String(
        overviewNode?.display_name || overviewNode?.name || overviewNode?.app_name || overviewNode?.title || "",
      ).trim();
      const appId = routeAppId;

      const detailsRef = (() => {
        const inner = (ret1 as unknown as { props?: { children?: { props?: Record<string, unknown> } } }).props?.children?.props;
        const details = inner?.details;
        return details && typeof details === "object" ? (details as Record<string, unknown>) : null;
      })();

      const nextComponent = (
        <LibraryTimeBlock
          key={`freedeck-library-times-${appId}`}
          appId={appId}
          title={gameTitle}
          detailsRef={detailsRef}
        />
      );

      const host = resolveLibraryPlaySectionHost(ret1Map) || resolveLibraryOverlayHost(ret1Map);
      if (!host) return ret1;

      const hostChildren = ensureNodeChildrenArray(host);
      for (let index = hostChildren.length - 1; index >= 0; index -= 1) {
        if (isLibraryTimeNode(hostChildren[index])) {
          hostChildren.splice(index, 1);
        }
      }

      const playButtonIndex = hostChildren.findIndex((child) => {
        return Boolean(
          findInReactTreeLike(child, (value) => {
            if (!value || typeof value !== "object") return false;
            const props = (value as Record<string, unknown>).props as Record<string, unknown> | undefined;
            return String(props?.className || "").includes("PlayButton");
          }),
        );
      });
      const insertIndex = playButtonIndex >= 0 ? Math.min(playButtonIndex + 1, hostChildren.length) : hostChildren.length;
      hostChildren.splice(insertIndex, 0, nextComponent);
      return ret1;
    });

    const patchedRender = childProps.renderFunc as Record<string, unknown>;
    patchedRender.__FREDECK_LIBRARY_RENDER_PATCHED = true;
    patchedRender.__FREDECK_LIBRARY_RENDER_UNPATCH = renderPatch.unpatch;
    return props;
  });

  return resolvePatchHandleUnpatch(patchHandle);
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutMessage: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = window.setTimeout(() => {
      reject(new Error(timeoutMessage));
    }, timeoutMs);
    promise
      .then((value) => {
        window.clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        window.clearTimeout(timer);
        reject(error);
      });
  });
}

async function pickFolder(startPath: string): Promise<string> {
  const base = String(startPath || "/home/deck").trim() || "/home/deck";
  const result = await openFilePicker(FileSelectionType.FOLDER, base, false, true);
  return String(result?.realpath || result?.path || "").trim();
}

function isTaskAlreadyInstalled(task: TaskItem): boolean {
  const installStatus = String(task.install_status || "").trim().toLowerCase();
  const steamStatus = String(task.steam_import_status || "").trim().toLowerCase();
  if (installStatus === "installed" && steamStatus !== "needs_exe") return true;
  const downloadStatus = String(task.status || "").trim().toLowerCase();
  if (downloadStatus === "complete" && String(task.installed_path || "").trim() && steamStatus !== "needs_exe") return true;
  return false;
}

function isTaskActive(task: TaskItem): boolean {
  const status = String(task.status || "").trim().toLowerCase();
  const installStatus = String(task.install_status || "").trim().toLowerCase();
  if (installStatus === "installing") return true;
  if (status === "complete" && installStatus && !["installed", "failed", "skipped", "canceled", "bundled"].includes(installStatus)) return true;
  if (!status) return false;
  return !["complete", "error", "removed"].includes(status);
}

function countActiveTasks(tasks: TaskItem[]): number {
  let count = 0;
  for (const task of tasks || []) {
    if (isTaskActive(task)) count += 1;
  }
  return count;
}

function resolvePanelPollMode(state: PanelState): PanelPollMode {
  const visible = !document.hidden;
  if (!visible) return PANEL_POLL_MODE_BACKGROUND;
  return countActiveTasks(state.tasks || []) > 0 ? PANEL_POLL_MODE_ACTIVE : PANEL_POLL_MODE_IDLE;
}

function pollIntervalByMode(mode: PanelPollMode): number {
  if (mode === PANEL_POLL_MODE_ACTIVE) return PANEL_ACTIVE_POLL_MS;
  if (mode === PANEL_POLL_MODE_BACKGROUND) return PANEL_BACKGROUND_POLL_MS;
  return PANEL_IDLE_POLL_MS;
}

function SettingsPage() {
  const [loading, setLoading] = useState<boolean>(true);
  const [saving, setSaving] = useState<boolean>(false);
  const [checkingMediaMounts, setCheckingMediaMounts] = useState<boolean>(false);
  const [clearingLogin, setClearingLogin] = useState<boolean>(false);
  const [clearingBaiduLogin, setClearingBaiduLogin] = useState<boolean>(false);
  const [savingCtfileToken, setSavingCtfileToken] = useState<boolean>(false);
  const [clearingCtfileToken, setClearingCtfileToken] = useState<boolean>(false);
  const [startingBaiduCapture, setStartingBaiduCapture] = useState<boolean>(false);
  const [downloadingSwitchEmulator, setDownloadingSwitchEmulator] = useState<boolean>(false);
  const [switchEmulatorStatus, setSwitchEmulatorStatus] =
    useState<SwitchEmulatorStatusData & { loaded: boolean }>(EMPTY_SWITCH_EMULATOR_STATUS);
  const [activeTab, setActiveTab] = useState<string>("paths");
  const [settings, setSettings] = useState<SettingsState>(EMPTY_SETTINGS);
  const [login, setLogin] = useState<LoginState>(EMPTY_STATE.login);
  const [baiduLogin, setBaiduLogin] = useState<LoginState>(EMPTY_STATE.baidu_login);
  const [ctfileLogin, setCtfileLogin] = useState<CtfileLoginState>(EMPTY_STATE.ctfile_login);
  const [ctfileTokenDraft, setCtfileTokenDraft] = useState<string>("");
  const [baiduLoginCapture, setBaiduLoginCapture] = useState<Record<string, unknown>>({});
  const [catalogDate, setCatalogDate] = useState<string>("2026-02-23");
  const [catalogCsvPath, setCatalogCsvPath] = useState<string>("");
  const [updatingCatalog, setUpdatingCatalog] = useState<boolean>(false);
  const [cloudSaveUploadState, setCloudSaveUploadState] = useState<CloudSaveUploadState>(EMPTY_CLOUD_SAVE_STATE);
  const [startingCloudSaveUpload, setStartingCloudSaveUpload] = useState<boolean>(false);
  const [runtimeRepairState, setRuntimeRepairState] = useState<RuntimeRepairState>(EMPTY_RUNTIME_REPAIR_STATE);
  const [loadingRuntimeRepairWizard, setLoadingRuntimeRepairWizard] = useState<boolean>(false);
  const [startingRuntimeRepair, setStartingRuntimeRepair] = useState<boolean>(false);
  const [reimportingMissingSteamGames, setReimportingMissingSteamGames] = useState<boolean>(false);
  const [splitDraft, setSplitDraft] = useState<number>(16);
  const settingsContainerRef = useRef<HTMLDivElement | null>(null);
  const tabStabilityCss = useMemo(
    () => `
      .freedeck-settings-root [class*="TabContentsScroll"],
      .freedeck-settings-root [class*="TabContents"],
      .freedeck-settings-root [class*="TabContent"],
      .freedeck-settings-root [class*="ScrollPanel"] {
        scrollbar-gutter: stable both-edges !important;
        overflow-y: auto !important;
      }
      .freedeck-settings-root [class*="TabHeaderRowWrapper"],
      .freedeck-settings-root [class*="TabRowTabs"],
      .freedeck-settings-root [class*="TabsRowScroll"],
      .freedeck-settings-root [class*="TabRow"] {
        transition: none !important;
        animation: none !important;
        scroll-behavior: auto !important;
      }
      .freedeck-settings-root [role="tablist"] {
        scroll-behavior: auto !important;
      }
    `,
    [],
  );

  useEffect(() => {
    const classMap = getGamepadTabClassMap();
    if (!classMap) return;
    const styleId = "freedeck-settings-tabs-no-jitter";
    if (document.getElementById(styleId)) return;
	    const style = document.createElement("style");
	    style.id = styleId;
	    const scope = ".freedeck-settings-root";
	    const rules: string[] = [];
	    if (classMap.TabsRowScroll) {
	      rules.push(`${scope} .${classMap.TabsRowScroll}{scroll-behavior:auto !important;}`);
	    }
	    if (classMap.TabRowTabs) {
	      rules.push(`${scope} .${classMap.TabRowTabs}{transition:none !important;}`);
	      rules.push(`${scope} .${classMap.TabRowTabs}{scroll-snap-type:none !important;}`);
	    }
	    if (classMap.Tab) {
	      rules.push(`${scope} .${classMap.Tab}{transition:none !important;}`);
	    }
	    style.textContent = rules.join("\n");
	    document.head.appendChild(style);
	  }, []);

	  const refreshSettings = useCallback(async () => {
	    const result = await withTimeout(
	      getTianyiPanelState(),
	      PANEL_REQUEST_TIMEOUT_MS,
	      "读取设置超时，请稍后重试",
	    );
	    if (result.status !== "success" || !result.data) {
	      throw new Error(result.message || "读取设置失败");
	    }
	    const next = Object.assign({}, EMPTY_SETTINGS, result.data.settings || {});
	    setSettings(next);
	    setUiPrefsFromSettings(next);
	    setLogin(result.data.login || EMPTY_STATE.login);
	    setBaiduLogin(result.data.baidu_login || EMPTY_STATE.baidu_login);
	    setCtfileLogin((result.data as unknown as { ctfile_login?: CtfileLoginState }).ctfile_login || EMPTY_STATE.ctfile_login);
	    setBaiduLoginCapture((result.data.baidu_login_capture as Record<string, unknown>) || {});
	    setSplitDraft(Math.max(1, Math.min(64, Number(next.split_count || 16))));
	  }, []);

  const refreshCatalogVersion = useCallback(async () => {
    try {
      const result = await withTimeout(
        getTianyiCatalogVersion(),
        PANEL_REQUEST_TIMEOUT_MS,
        "读取游戏列表版本超时，请稍后重试",
      );
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "读取游戏列表版本失败");
      }
      const nextDate = String(result.data.date || "").trim() || "2026-02-23";
      setCatalogDate(nextDate);
      setCatalogCsvPath(String(result.data.csv_path || ""));
    } catch {
      // 忽略读取失败，回退默认日期，避免阻塞设置页展示。
      setCatalogDate((prev) => String(prev || "").trim() || "2026-02-23");
    }
  }, []);

  const onUpdateCatalog = useCallback(async () => {
    if (updatingCatalog) return;
    setUpdatingCatalog(true);
    try {
      const result = await withTimeout(
        updateTianyiCatalog(),
        25000,
        "更新游戏列表超时，请稍后重试",
      );
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "更新游戏列表失败");
      }
      const payload = result.data;
      const nextDate = String(payload.date || "").trim() || "2026-02-23";
      setCatalogDate(nextDate);
      setCatalogCsvPath(String(payload.csv_path || ""));
      toaster.toast({ title: "Freedeck", body: String(payload.message || (payload.updated ? `已更新到 ${nextDate}` : `当前已是最新：${nextDate}`)) });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setUpdatingCatalog(false);
    }
  }, [updatingCatalog]);

  const refreshCloudSaveUploadStatus = useCallback(async () => {
    const result = await getTianyiCloudSaveUploadStatus();
    if (result.status !== "success") {
      throw new Error(result.message || "读取云存档状态失败");
    }
    const nextState = normalizeCloudSaveUploadState(result.data?.state);
    setCloudSaveUploadState(nextState);
  }, []);

  const refreshRuntimeRepairStatus = useCallback(async () => {
    const result = await getTianyiRuntimeRepairStatus();
    if (result.status !== "success") {
      throw new Error(result.message || "读取运行库修复状态失败");
    }
    setRuntimeRepairState(normalizeRuntimeRepairState(result.data?.state));
  }, []);

  const onReimportMissingSteamGames = useCallback(async () => {
    if (reimportingMissingSteamGames) return;
    try {
      const result = await withTimeout(
        listTianyiMissingSteamImports(),
        PANEL_REQUEST_TIMEOUT_MS,
        "检测已安装游戏超时，请稍后重试",
      );
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "检测已安装游戏失败");
      }

      const items = Array.isArray(result.data.items) ? result.data.items : [];
      if (!items.length) {
        toaster.toast({ title: "Freedeck", body: "未发现需要重新导入到 Steam 的已安装游戏" });
        return;
      }

      const lines = items.slice(0, 8).map((item) => {
        const title = String(item.title || item.game_id || "未命名游戏").trim();
        const reason = String(item.reason || "").trim();
        return `- ${title}${reason ? `｜${reason}` : ""}`;
      });
      const hiddenCount = Math.max(0, items.length - lines.length);

      showModal(
        <ConfirmModal
          strTitle="重新导入游戏"
          strDescription={
            `检测到 ${items.length} 款已安装但未正确加入 Steam 的游戏。` +
            `\n\n${lines.join("\n")}` +
            (hiddenCount > 0 ? `\n... 还有 ${hiddenCount} 款` : "") +
            "\n\n确认重新导入吗？导入完成后会提示你重启 Steam。"
          }
          strOKButtonText="重新导入"
          strCancelButtonText="取消"
          onOK={() => {
            void (async () => {
              setReimportingMissingSteamGames(true);
              try {
                const runResult = await withTimeout(
                  reimportTianyiMissingSteamImports(),
                  45000,
                  "重新导入游戏超时，请稍后重试",
                );
                if (runResult.status !== "success" || !runResult.data) {
                  throw new Error(runResult.message || "重新导入游戏失败");
                }

                const payload = runResult.data;
                const importedCount = Math.max(0, Number(payload.imported || 0));
                toaster.toast({
                  title: "Freedeck",
                  body:
                    String(payload.message || "").trim() ||
                    `已重新导入 ${importedCount} 款游戏到 Steam`,
                });

                if (importedCount > 0 || Boolean(payload.needs_restart)) {
                  showRestartSteamPrompt(`已重新导入 ${importedCount} 款游戏到 Steam 库，重启 Steam 后可立即生效。`);
                }
              } catch (error) {
                toaster.toast({ title: "Freedeck", body: String(error) });
              } finally {
                setReimportingMissingSteamGames(false);
              }
            })();
          }}
          onCancel={() => {}}
        />,
      );
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    }
  }, [reimportingMissingSteamGames]);

  useEffect(() => {
    if (!cloudSaveUploadState.running) return;
    let alive = true;
    let timer = 0;
    const poll = async () => {
      if (!alive) return;
      try {
        await refreshCloudSaveUploadStatus();
      } catch {
        // 轮询失败时保留当前状态，避免干扰 UI。
      } finally {
        if (alive) timer = window.setTimeout(poll, 1200);
      }
    };
    timer = window.setTimeout(poll, 1200);
    return () => {
      alive = false;
      if (timer) window.clearTimeout(timer);
    };
  }, [cloudSaveUploadState.running, refreshCloudSaveUploadStatus]);

  useEffect(() => {
    if (!runtimeRepairState.running) return;
    let alive = true;
    let timer = 0;
    const poll = async () => {
      if (!alive) return;
      try {
        await refreshRuntimeRepairStatus();
      } catch {
        // 轮询失败时保留当前状态，避免干扰 UI。
      } finally {
        if (alive) timer = window.setTimeout(poll, 1200);
      }
    };
    timer = window.setTimeout(poll, 1200);
    return () => {
      alive = false;
      if (timer) window.clearTimeout(timer);
    };
  }, [refreshRuntimeRepairStatus, runtimeRepairState.running]);

  const savePatch = useCallback(
    async (patch: Partial<SettingsState>, successMessage = "设置已保存") => {
      if (saving) return;
      setSaving(true);
      const shouldPromptRestart =
        typeof patch.show_playtime_widget !== "undefined" &&
        Boolean(patch.show_playtime_widget) !== Boolean(settings.show_playtime_widget);
      try {
        const merged = Object.assign({}, settings, patch);
        const result = await setTianyiSettings(toPayload(merged));
        if (result.status !== "success" || !result.data) {
          throw new Error(result.message || "保存设置失败");
        }
	        const next = Object.assign({}, EMPTY_SETTINGS, result.data || {});
	        setSettings(next);
	        setUiPrefsFromSettings(next);
	        setSplitDraft(Math.max(1, Math.min(64, Number(next.split_count || 16))));
	        toaster.toast({ title: "Freedeck", body: successMessage });
	        if (shouldPromptRestart) {
	          showRestartSteamPrompt("“显示时长”设置已变更，重启 Steam 后可立即生效。");
	        }
	      } catch (error) {
	        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setSaving(false);
      }
    },
    [saving, settings],
  );

  const onPickDownloadDir = useCallback(async () => {
    try {
      void frontendDebugLog({ message: "settings:pick_download_dir:click" });
      const selected = await pickFolder(settings.download_dir || "/home/deck");
      if (!selected) return;
      await savePatch({ download_dir: selected }, `下载目录已更新：${selected}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `选择下载目录失败：${error}` });
    }
  }, [savePatch, settings.download_dir]);

  const onPickInstallDir = useCallback(async () => {
    try {
      void frontendDebugLog({ message: "settings:pick_install_dir:click" });
      const selected = await pickFolder(settings.install_dir || settings.download_dir || "/home/deck");
      if (!selected) return;
      await savePatch({ install_dir: selected }, `安装目录已更新：${selected}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `选择安装目录失败：${error}` });
    }
  }, [savePatch, settings.download_dir, settings.install_dir]);

  const refreshSwitchEmulatorStatus = useCallback(async () => {
    try {
      const result = await getSwitchEmulatorStatus();
      if (result.status !== "success") {
        throw new Error(result.message || "读取 Switch 模拟器状态失败");
      }
      setSwitchEmulatorStatus({
        installed: Boolean(result.data?.installed),
        exe_path: String(result.data?.exe_path || ""),
        message: String(result.data?.message || ""),
        loaded: true,
      });
    } catch {
      setSwitchEmulatorStatus((current) => ({ ...current, loaded: true }));
    }
  }, []);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        await refreshSettings();
        await refreshCatalogVersion();
        await refreshSwitchEmulatorStatus();
        await refreshCloudSaveUploadStatus();
        await refreshRuntimeRepairStatus();
      } catch (error) {
        if (alive) toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [
    refreshCatalogVersion,
    refreshSettings,
    refreshCloudSaveUploadStatus,
    refreshRuntimeRepairStatus,
    refreshSwitchEmulatorStatus,
  ]);

  const fetchMediaMounts = useCallback(async (): Promise<StorageMount[] | null> => {
    if (checkingMediaMounts) return null;
    setCheckingMediaMounts(true);
    try {
      const result = await listMediaMounts();
      if (result.status !== "success") {
        throw new Error(result.message || "读取外接存储失败");
      }
      const mounts = Array.isArray(result.data?.mounts) ? result.data?.mounts : [];
      return mounts
        .map((mount) => ({
          ...mount,
          path: String(mount.path || "").trim(),
          label: typeof mount.label === "string" ? mount.label : undefined,
        }))
        .filter((mount) => Boolean(mount.path));
    } finally {
      setCheckingMediaMounts(false);
    }
  }, [checkingMediaMounts]);

  const onUseDownloadDirFromMedia = useCallback(async () => {
    try {
      void frontendDebugLog({ message: "settings:sd_download_dir:click" });
      const mounts = await fetchMediaMounts();
      if (!mounts) return;
      if (mounts.length <= 0) {
        toaster.toast({ title: "Freedeck", body: "未检测到外接设备（/run/media）" });
        return;
      }

      const applyMount = (mount: StorageMount) => {
        const base = String(mount.path || "").trim();
        if (!base) return;
        const target = `${base}/Game`;
        void frontendDebugLog({ message: "settings:sd_download_dir:apply", details: target });
        void savePatch({ download_dir: target }, `下载目录已更新：${target}`);
      };

      if (mounts.length === 1) {
        applyMount(mounts[0]);
        return;
      }

      showModal(
        <StorageMountSelectModal
          title="选择外接设备"
          description="请选择要作为下载目录的外接设备，Freedeck 将使用：<挂载点>/Game"
          mounts={mounts}
          onConfirm={applyMount}
        />,
      );
    } catch (error) {
      void frontendDebugLog({ message: "settings:sd_download_dir:error", details: String(error) });
      toaster.toast({ title: "Freedeck", body: String(error) });
    }
  }, [fetchMediaMounts, savePatch]);

  const onUseInstallDirFromMedia = useCallback(async () => {
    try {
      void frontendDebugLog({ message: "settings:sd_install_dir:click" });
      const mounts = await fetchMediaMounts();
      if (!mounts) return;
      if (mounts.length <= 0) {
        toaster.toast({ title: "Freedeck", body: "未检测到外接设备（/run/media）" });
        return;
      }

      const applyMount = (mount: StorageMount) => {
        const base = String(mount.path || "").trim();
        if (!base) return;
        const target = `${base}/Game/installed`;
        void frontendDebugLog({ message: "settings:sd_install_dir:apply", details: target });
        void savePatch({ install_dir: target }, `安装目录已更新：${target}`);
      };

      if (mounts.length === 1) {
        applyMount(mounts[0]);
        return;
      }

      showModal(
        <StorageMountSelectModal
          title="选择外接设备"
          description="请选择要作为安装目录的外接设备，Freedeck 将使用：<挂载点>/Game/installed"
          mounts={mounts}
          onConfirm={applyMount}
        />,
      );
    } catch (error) {
      void frontendDebugLog({ message: "settings:sd_install_dir:error", details: String(error) });
      toaster.toast({ title: "Freedeck", body: String(error) });
    }
  }, [fetchMediaMounts, savePatch]);

  const onSaveSplit = useCallback(async () => {
    const value = Number(splitDraft);
    if (!Number.isFinite(value) || value < 1 || value > 64) {
      toaster.toast({ title: "Freedeck", body: "分片数必须在 1 到 64 之间" });
      return;
    }
    await savePatch({ split_count: value }, "下载参数已更新");
  }, [savePatch, splitDraft]);

  const onClearLogin = useCallback(async () => {
    if (clearingLogin) return;
    setClearingLogin(true);
    try {
      const result = await clearTianyiLogin();
      if (result.status !== "success") {
        throw new Error(result.message || "注销失败");
      }
      await refreshSettings();
      await refreshCloudSaveUploadStatus();
      const body = String(result.data?.message || "已注销天翼云账号");
      toaster.toast({ title: "Freedeck", body });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setClearingLogin(false);
    }
  }, [clearingLogin, refreshCloudSaveUploadStatus, refreshSettings]);

  const onBaiduLogin = useCallback(async () => {
    if (startingBaiduCapture) return;
    setStartingBaiduCapture(true);
    try {
      await startBaiduLoginCapture({ timeout_seconds: 240 });
      const urlResult = await getBaiduLoginUrl();
      const url = urlResult.url || urlResult.data?.url || "https://pan.baidu.com/";
      toaster.toast({ title: "Freedeck", body: "请在打开的页面完成百度网盘登录（可扫码），然后返回 Freedeck 等待自动检测..." });
      openExternalUrl(url);
      await refreshSettings();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setStartingBaiduCapture(false);
    }
  }, [refreshSettings, startingBaiduCapture]);

  const onClearBaiduLogin = useCallback(async () => {
    if (clearingBaiduLogin) return;
    setClearingBaiduLogin(true);
    try {
      const result = await clearBaiduLogin();
      if (result.status !== "success") {
        throw new Error(result.message || "注销失败");
      }
      await refreshSettings();
      const body = String(result.data?.message || "已注销百度网盘账号");
      toaster.toast({ title: "Freedeck", body });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setClearingBaiduLogin(false);
    }
  }, [clearingBaiduLogin, refreshSettings]);

  const onOpenCtfileGuide = useCallback(async () => {
    try {
      const urlResult = await getCtfileLoginGuideUrl();
      const url = urlResult.url || urlResult.data?.url || "https://ctfile.qinlili.bid/";
      toaster.toast({ title: "Freedeck", body: "请在打开的页面完成登录并复制 token(session_id) 回来粘贴保存。" });
      openExternalUrl(url);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    }
  }, []);

  const onDownloadSwitchEmulator = useCallback(async () => {
    if (downloadingSwitchEmulator) return;

    const downloadDir = String(settings.download_dir || "").trim();
    const installDir = String(settings.install_dir || "").trim();
    if (!downloadDir || !installDir) {
      toaster.toast({ title: "Freedeck", body: "请先在“路径”页设置下载目录与安装目录" });
      return;
    }

    setDownloadingSwitchEmulator(true);
    try {
      toaster.toast({ title: "Freedeck", body: "正在创建 Switch 模拟器下载任务..." });
      const result = await downloadSwitchEmulator({ download_dir: downloadDir, install_dir: installDir });
      if (result.status !== "success") {
        throw new Error(result.message || "创建下载任务失败");
      }
      toaster.toast({ title: "Freedeck", body: "已开始下载 Switch 模拟器，可在插件主页“下载列表”查看进度" });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `下载 Switch 模拟器失败：${error}` });
    } finally {
      setDownloadingSwitchEmulator(false);
    }
  }, [downloadingSwitchEmulator, settings.download_dir, settings.install_dir]);

  const onSaveCtfileToken = useCallback(async () => {
    if (savingCtfileToken) return;
    const token = String(ctfileTokenDraft || "").trim();
    if (!token) {
      toaster.toast({ title: "Freedeck", body: "token 不能为空" });
      return;
    }
    setSavingCtfileToken(true);
    try {
      const result = await setCtfileToken({ token });
      if (result.status !== "success") {
        throw new Error(result.message || "保存 token 失败");
      }
      await refreshSettings();
      setCtfileTokenDraft("");
      toaster.toast({ title: "Freedeck", body: String(result.data?.message || "CTFile token 已保存") });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setSavingCtfileToken(false);
    }
  }, [ctfileTokenDraft, refreshSettings, savingCtfileToken]);

  const onClearCtfileToken = useCallback(async () => {
    if (clearingCtfileToken) return;
    setClearingCtfileToken(true);
    try {
      const result = await clearCtfileToken();
      if (result.status !== "success") {
        throw new Error(result.message || "清除 token 失败");
      }
      await refreshSettings();
      setCtfileTokenDraft("");
      toaster.toast({ title: "Freedeck", body: String(result.data?.message || "CTFile token 已清除") });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setClearingCtfileToken(false);
    }
  }, [clearingCtfileToken, refreshSettings]);

  const onStartCloudSaveUpload = useCallback(async () => {
    if (startingCloudSaveUpload) return;
    setStartingCloudSaveUpload(true);
    try {
      const result = await startTianyiCloudSaveUpload();
      if (result.status !== "success") {
        throw new Error(result.message || "启动云存档上传失败");
      }
      const data = result.data;
      if (data?.state) {
        setCloudSaveUploadState(normalizeCloudSaveUploadState(data.state));
      }
      toaster.toast({ title: "Freedeck", body: String(data?.message || "云存档上传任务已启动") });
      await refreshCloudSaveUploadStatus();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setStartingCloudSaveUpload(false);
    }
  }, [refreshCloudSaveUploadStatus, startingCloudSaveUpload]);

  const onStartRuntimeRepair = useCallback(
    async (gameIds: string[], packageIds: string[]) => {
      if (startingRuntimeRepair) return;
      setStartingRuntimeRepair(true);
      try {
        const result = await startTianyiRuntimeRepair({
          game_ids: (Array.isArray(gameIds) ? gameIds : []).map((item) => String(item || "").trim()).filter(Boolean),
          package_ids: (Array.isArray(packageIds) ? packageIds : []).map((item) => String(item || "").trim()).filter(Boolean),
        });
        if (result.status !== "success" || !result.data) {
          throw new Error(result.message || "启动运行库修复失败");
        }
        if (result.data.state) {
          setRuntimeRepairState(normalizeRuntimeRepairState(result.data.state));
        }
        toaster.toast({ title: "Freedeck", body: String(result.data.message || "运行库修复任务已启动") });
        await refreshRuntimeRepairStatus();
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setStartingRuntimeRepair(false);
      }
    },
    [refreshRuntimeRepairStatus, startingRuntimeRepair],
  );

  const onOpenRuntimeRepairWizard = useCallback(async () => {
    if (loadingRuntimeRepairWizard || startingRuntimeRepair || runtimeRepairState.running) return;
    setLoadingRuntimeRepairWizard(true);
    try {
      const [candidatesResult, packagesResult] = await Promise.all([
        withTimeout(
          listTianyiRuntimeRepairCandidates(),
          PANEL_REQUEST_TIMEOUT_MS,
          "读取可修复游戏超时，请稍后重试",
        ),
        withTimeout(
          listTianyiRuntimeRepairPackages(),
          PANEL_REQUEST_TIMEOUT_MS,
          "读取运行库包超时，请稍后重试",
        ),
      ]);
      if (candidatesResult.status !== "success" || !candidatesResult.data) {
        throw new Error(candidatesResult.message || "读取可修复游戏失败");
      }
      if (packagesResult.status !== "success" || !packagesResult.data) {
        throw new Error(packagesResult.message || "读取运行库包失败");
      }

      const candidates = Array.isArray(candidatesResult.data.games) ? candidatesResult.data.games : [];
      const packages = Array.isArray(packagesResult.data.packages) ? packagesResult.data.packages : [];
      const readyGameIds = candidates
        .filter((item) => Boolean(item.prefix_ready))
        .map((item) => String(item.game_id || "").trim())
        .filter(Boolean);
      const defaultPackageIds = (Array.isArray(packagesResult.data.default_package_ids) ? packagesResult.data.default_package_ids : [])
        .map((item) => String(item || "").trim())
        .filter(Boolean);

      if (!candidates.length) {
        toaster.toast({ title: "Freedeck", body: "未找到可修复的已安装 PC 游戏" });
        return;
      }
      if (!packages.length) {
        toaster.toast({ title: "Freedeck", body: "未找到可用的运行库包" });
        return;
      }
      if (!readyGameIds.length) {
        toaster.toast({ title: "Freedeck", body: "当前没有已生成 compatdata 的 PC 游戏，请先从 Steam 启动一次目标游戏" });
      }

      const openPackageModal = (selectedGameIds: string[]) => {
        window.setTimeout(() => {
          showModal(
            <RuntimeRepairPackageSelectModal
              packages={packages}
              defaultSelectedIds={defaultPackageIds}
              selectedGameCount={selectedGameIds.length}
              onConfirm={(selectedPackageIds) => {
                void onStartRuntimeRepair(selectedGameIds, selectedPackageIds);
              }}
            />,
          );
        }, 0);
      };

      showModal(
        <RuntimeRepairGameSelectModal
          candidates={candidates}
          initialSelectedIds={readyGameIds}
          onConfirm={openPackageModal}
        />,
      );
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setLoadingRuntimeRepairWizard(false);
    }
  }, [loadingRuntimeRepairWizard, onStartRuntimeRepair, runtimeRepairState.running, startingRuntimeRepair]);

  const onOpenCloudSaveRestoreModal = useCallback(() => {
    showModal(<CloudSaveRestoreModal loggedIn={Boolean(login.logged_in)} />, undefined, {
      strTitle: "下载云存档",
      bNeverPopOut: true,
    });
  }, [login.logged_in]);

  const cloudSaveSummaryText = useMemo(() => {
    const current = cloudSaveUploadState;
    if (current.running) {
      const stage = cloudSaveStageText(current.stage);
      return `${stage}：${current.processed_games}/${current.total_games}，成功 ${current.uploaded}，跳过 ${current.skipped}，失败 ${current.failed}`;
    }
    const last = current.last_result;
    if (last && Number(last.finished_at || 0) > 0) {
      const stage = cloudSaveStageText(last.stage);
      return `最近一次：${stage}，成功 ${last.uploaded}，跳过 ${last.skipped}，失败 ${last.failed}`;
    }
    return "尚未执行云存档上传";
  }, [cloudSaveUploadState]);

  const cloudSaveUploadItems = useMemo(() => {
    const current = Array.isArray(cloudSaveUploadState.results) ? cloudSaveUploadState.results : [];
    if (current.length > 0) return current;
    const last = (cloudSaveUploadState.last_result || {}) as CloudSaveLastResult;
    return Array.isArray(last.results) ? last.results : [];
  }, [cloudSaveUploadState]);

  const showCloudSaveUploadDetails = useCallback(() => {
    const items = cloudSaveUploadItems;
    if (!items.length) {
      toaster.toast({ title: "Freedeck", body: "暂无可展示的云存档上传明细" });
      return;
    }

    const problems = items.filter((item) => {
      const status = String(item.status || "").trim().toLowerCase();
      return status === "skipped" || status === "failed";
    });
    const list = problems.length ? problems : items;
    const maxLines = 60;
    const lines = list.slice(0, maxLines).map((item) => {
      const title = String(item.game_title || item.game_key || item.game_id || "-");
      const statusText = cloudSaveItemStatusText(item.status);
      const reasonText = cloudSaveItemReasonText(item);
      const diag = (item.diagnostics || {}) as Record<string, unknown>;
      const shortcut = (diag["shortcut"] || {}) as Record<string, unknown>;
      const shortcutMessage = String(shortcut["message"] || "").trim();
      const extra = shortcutMessage && String(item.reason || "").trim().toLowerCase() === "prefix_unresolved" ? `｜${shortcutMessage}` : "";
      return `${statusText}｜${title}${reasonText ? `｜${reasonText}` : ""}${extra}`;
    });
    const hidden = Math.max(0, list.length - lines.length);
    const hint =
      problems.length > 0
        ? "提示：前缀未解析通常需要先从 Steam 启动一次该游戏（生成 compatdata），或检查快捷方式是否仍存在。"
        : "提示：点击“复制 JSON”可获得完整诊断信息。";

    showModal(
      <ConfirmModal
        strTitle="云存档上传明细"
        strDescription={`${hint}\n\n${lines.join("\n")}${hidden > 0 ? `\n... 还有 ${hidden} 条` : ""}`}
        strOKButtonText="关闭"
        strCancelButtonText="复制 JSON"
        onOK={() => {}}
        onCancel={() => {
          const last = cloudSaveUploadState.last_result as unknown as Record<string, unknown>;
          const payload =
            cloudSaveUploadState.running || Number(last?.["finished_at"] || 0) <= 0 ? (cloudSaveUploadState as unknown as Record<string, unknown>) : last;
          void (async () => {
            const ok = await copyToClipboard(JSON.stringify(payload, null, 2));
            toaster.toast({ title: "Freedeck", body: ok ? "已复制云存档诊断 JSON" : "复制失败（系统不支持剪贴板）" });
          })();
        }}
      />,
    );
  }, [cloudSaveUploadItems, cloudSaveUploadState]);

  const runtimeRepairSummaryText = useMemo(() => {
    const current = runtimeRepairState;
    if (current.running) {
      return `修复中：${current.processed_games}/${current.total_games} 个游戏，${current.completed_steps}/${current.total_steps} 个步骤，成功 ${current.succeeded_steps}，跳过 ${current.skipped_steps}，失败 ${current.failed_steps}`;
    }
    const last = current.last_result;
    if (last && Number(last.finished_at || 0) > 0) {
      return `最近一次：${runtimeRepairStageText(last.stage)}，成功 ${last.succeeded_steps}，跳过 ${last.skipped_steps}，失败 ${last.failed_steps}`;
    }
    return "尚未执行运行库修复";
  }, [runtimeRepairState]);

  const runtimeRepairItems = useMemo(() => {
    const current = Array.isArray(runtimeRepairState.results) ? runtimeRepairState.results : [];
    if (current.length > 0) return current;
    const last = runtimeRepairState.last_result;
    return Array.isArray(last.results) ? last.results : [];
  }, [runtimeRepairState]);

  const runtimeRepairProgressValue = useMemo(() => {
    if (runtimeRepairState.running) {
      return clampProgress(runtimeRepairState.progress);
    }
    const last = runtimeRepairState.last_result;
    if (Number(last.finished_at || 0) <= 0) return 0;
    const totalSteps = Math.max(0, Number(last.total_steps || 0));
    const completedSteps = Math.max(0, Number(last.completed_steps || 0));
    return totalSteps > 0 ? clampProgress((completedSteps / totalSteps) * 100) : 100;
  }, [runtimeRepairState]);

  const runtimeRepairStatusLine = useMemo(() => {
    if (runtimeRepairState.running) {
      const gameTitle = String(runtimeRepairState.current_game_title || runtimeRepairState.current_game_id || "").trim() || "准备中";
      const packageLabel = String(runtimeRepairState.current_package_label || runtimeRepairState.current_package_id || "").trim() || "等待运行库";
      const message = String(runtimeRepairState.message || "").trim();
      return `当前：${gameTitle}｜${packageLabel}${message ? `｜${message}` : ""}`;
    }
    const last = runtimeRepairState.last_result;
    if (Number(last.finished_at || 0) > 0) {
      return String(last.message || runtimeRepairState.message || "运行库修复已结束");
    }
    return "未开始运行库修复";
  }, [runtimeRepairState]);

  const showRuntimeRepairDetails = useCallback(() => {
    if (!runtimeRepairItems.length) {
      toaster.toast({ title: "Freedeck", body: "暂无可展示的运行库修复明细" });
      return;
    }

    const problems = runtimeRepairItems.filter((item) => String(item.status || "").trim().toLowerCase() !== "success");
    const list = problems.length ? problems : runtimeRepairItems;
    const maxLines = 60;
    const lines = list.slice(0, maxLines).map((item) => {
      const title = String(item.game_title || item.game_id || "-");
      const packageLabel = String(item.package_label || item.package_id || "-");
      const statusText = runtimeRepairResultStatusText(item.status);
      const message = String(item.message || item.reason || "").trim();
      const sourceType = String(item.source_type || "").trim();
      const returnCode = Number(item.return_code || 0);
      return `${statusText}｜${title}｜${packageLabel}${message ? `｜${message}` : ""}${sourceType ? `｜${sourceType}` : ""}${
        returnCode && returnCode !== 0 ? `｜code=${returnCode}` : ""
      }`;
    });
    const hidden = Math.max(0, list.length - lines.length);

    showModal(
      <ConfirmModal
        strTitle="运行库修复明细"
        strDescription={`${lines.join("\n")}${hidden > 0 ? `\n... 还有 ${hidden} 条` : ""}`}
        strOKButtonText="关闭"
        strCancelButtonText="复制 JSON"
        onOK={() => {}}
        onCancel={() => {
          const last = runtimeRepairState.last_result as unknown as Record<string, unknown>;
          const payload =
            runtimeRepairState.running || Number(last?.["finished_at"] || 0) <= 0 ? (runtimeRepairState as unknown as Record<string, unknown>) : last;
          void (async () => {
            const ok = await copyToClipboard(JSON.stringify(payload, null, 2));
            toaster.toast({ title: "Freedeck", body: ok ? "已复制运行库修复诊断 JSON" : "复制失败（系统不支持剪贴板）" });
          })();
        }}
      />,
    );
  }, [runtimeRepairItems, runtimeRepairState]);

  const runtimeRepairBusy = saving || loadingRuntimeRepairWizard || startingRuntimeRepair || runtimeRepairState.running;

  const tabs = useMemo(
    () => [
      {
        id: "paths",
        title: "基础",
        content: (
          <>
            <PanelSection title="下载目录">
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{settings.download_dir || "未设置"}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ display: "flex", alignItems: "center", gap: "8px", width: "100%" }}>
                  <div style={{ flex: "1 1 auto", minWidth: 0 }}>
                    <ButtonItem layout="below" onClick={onPickDownloadDir} disabled={saving}>
                      选择下载目录
                    </ButtonItem>
                  </div>
                  <Focusable style={{ flex: "0 0 auto" }}>
                    <DialogButton
                      onClick={onUseDownloadDirFromMedia}
                      onOKButton={onUseDownloadDirFromMedia}
                      disabled={saving || checkingMediaMounts}
                      style={{ minWidth: "86px" }}
                    >
                      {checkingMediaMounts ? "检测中..." : "内存卡"}
                    </DialogButton>
                  </Focusable>
                </div>
              </PanelSectionRow>
            </PanelSection>
            <PanelSection title="安装目录">
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{settings.install_dir || "未设置"}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ display: "flex", alignItems: "center", gap: "8px", width: "100%" }}>
                  <div style={{ flex: "1 1 auto", minWidth: 0 }}>
                    <ButtonItem layout="below" onClick={onPickInstallDir} disabled={saving}>
                      选择安装目录
                    </ButtonItem>
                  </div>
                  <Focusable style={{ flex: "0 0 auto" }}>
                    <DialogButton
                      onClick={onUseInstallDirFromMedia}
                      onOKButton={onUseInstallDirFromMedia}
                      disabled={saving || checkingMediaMounts}
                      style={{ minWidth: "86px" }}
                    >
                      {checkingMediaMounts ? "检测中..." : "内存卡"}
                    </DialogButton>
                  </Focusable>
                </div>
              </PanelSectionRow>
            </PanelSection>

            <PanelSection title="安装行为">
              <PanelSectionRow>
                <ToggleField
                  label="安装后自动删除压缩包"
                  description="仅在自动安装成功后生效"
                  checked={Boolean(settings.auto_delete_package)}
                  onChange={(value: boolean) => savePatch({ auto_delete_package: value })}
                  disabled={saving}
                />
              </PanelSectionRow>
            </PanelSection>

          </>
        ),
      },
      {
        id: "runtime",
        title: "运行",
        content: (
          <>
            <PanelSection title="库主页">
              <PanelSectionRow>
                <ToggleField
                  label="显示时长"
                  description="在 Steam 库主页显示：已玩时长 / 主线时长 / 总时长"
                  checked={Boolean(settings.show_playtime_widget)}
                  onChange={(value: boolean) => savePatch({ show_playtime_widget: value })}
                  disabled={saving}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={onReimportMissingSteamGames}
                  disabled={saving || reimportingMissingSteamGames}
                >
                  {reimportingMissingSteamGames ? "重新导入中..." : "重新导入游戏"}
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>

            <PanelSection title="VC++ 修复">
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={onOpenRuntimeRepairWizard} disabled={runtimeRepairBusy}>
                  {loadingRuntimeRepairWizard ? "读取候选中..." : startingRuntimeRepair ? "启动中..." : runtimeRepairState.running ? "修复进行中..." : "VC++ 修复"}
                </ButtonItem>
              </PanelSectionRow>
              {runtimeRepairState.running ? (
                <>
                  <PanelSectionRow>
                    <div style={{ width: "100%", display: "grid", gap: "8px" }}>
                      <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{runtimeRepairSummaryText}</div>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "8px",
                          fontSize: "11px",
                          lineHeight: 1.45,
                          opacity: 0.88,
                        }}
                      >
                        <span>{runtimeRepairStageText(runtimeRepairState.stage)}</span>
                        <span>{`${runtimeRepairProgressValue.toFixed(1)}%`}</span>
                      </div>
                      <div
                        style={{
                          width: "100%",
                          height: "8px",
                          borderRadius: "999px",
                          background: "rgba(255,255,255,0.12)",
                          overflow: "hidden",
                        }}
                      >
                        <div
                          style={{
                            width: `${runtimeRepairProgressValue.toFixed(1)}%`,
                            height: "100%",
                            borderRadius: "999px",
                            background: "linear-gradient(90deg, #4f9cff 0%, #6ed0ff 100%)",
                            transition: "width 220ms ease",
                          }}
                        />
                      </div>
                      <div style={{ fontSize: "11px", lineHeight: 1.45, opacity: 0.88 }}>{runtimeRepairStatusLine}</div>
                    </div>
                  </PanelSectionRow>
                  <PanelSectionRow>
                    <ButtonItem layout="below" onClick={showRuntimeRepairDetails} disabled={runtimeRepairItems.length <= 0}>
                      查看修复明细
                    </ButtonItem>
                  </PanelSectionRow>
                </>
              ) : null}
            </PanelSection>

            <PanelSection title="启动项">
              <PanelSectionRow>
                <ToggleField
                  label="自动添加小黄鸭（LSFG）"
                  checked={Boolean(settings.lsfg_enabled)}
                  onChange={(value: boolean) => savePatch({ lsfg_enabled: value })}
                  disabled={saving}
                />
              </PanelSectionRow>
            </PanelSection>
          </>
        ),
      },
	      {
	        id: "emulator",
	        title: "模拟器",
	        content: (
	          <>
	            <PanelSection title="Switch">
	              <PanelSectionRow>
	                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>
	                  {`安装状态：${
	                    switchEmulatorStatus.loaded
	                      ? switchEmulatorStatus.installed
	                        ? "已安装"
	                        : "未安装"
	                      : "检测中..."
	                  }`}
	                </div>
	              </PanelSectionRow>
	              <PanelSectionRow>
	                <ButtonItem
	                  layout="below"
	                  onClick={onDownloadSwitchEmulator}
	                  disabled={saving || downloadingSwitchEmulator}
	                >
	                  {downloadingSwitchEmulator ? "创建任务中..." : "下载 Switch 模拟器"}
	                </ButtonItem>
	              </PanelSectionRow>
	            </PanelSection>
	          </>
	        ),
	      },
	      {
	        id: "download",
	        title: "下载",
	        content: (
	          <>
            <PanelSection title="下载参数">
              <PanelSectionRow>
                <ToggleField
                  label="极速模式（32 连接）"
                  description="提高单服务器连接数上限，并在大文件下载时至少使用 32 分片。可能更容易触发限速/临时错误。"
                  checked={Boolean(settings.aria2_fast_mode)}
                  onChange={(value: boolean) => savePatch({ aria2_fast_mode: value }, value ? "已开启极速模式" : "已关闭极速模式")}
                  disabled={saving}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <ToggleField
                  label="强制 IPv4（禁用 IPv6）"
                  description="当部分网络 IPv6 线路下载异常/极慢时可尝试开启。关闭后将允许 IPv6。"
                  checked={Boolean(settings.force_ipv4)}
                  onChange={(value: boolean) => savePatch({ force_ipv4: value }, value ? "已启用强制 IPv4" : "已关闭强制 IPv4")}
                  disabled={saving}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <ToggleField
                  label="自动切线路/重取直链"
                  description="下载持续低速时自动刷新直链并尝试切换线路（带冷却与次数上限）"
                  checked={Boolean(settings.auto_switch_line)}
                  onChange={(value: boolean) =>
                    savePatch({ auto_switch_line: value }, value ? "已开启自动切线路" : "已关闭自动切线路")
                  }
                  disabled={saving}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <SliderField
                  label="aria2 分片数"
                  description="建议 8~32，范围 1~64（极速模式会对大文件强制至少 32）"
                  value={Math.max(1, Math.min(64, Number(splitDraft || 16)))}
                  min={1}
                  max={64}
                  step={1}
                  showValue
                  editableValue
                  onChange={(value: number) => setSplitDraft(Math.max(1, Math.min(64, Number(value || 1))))}
                  disabled={saving}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={onSaveSplit} disabled={saving}>
                  保存下载参数
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>

            <PanelSection title="游戏列表">
              <PanelSectionRow>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    gap: "10px",
                    width: "100%",
                  }}
                >
                  <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{`当前列表日期：${catalogDate || "未知"}`}</div>
                  <DialogButton onClick={onUpdateCatalog} disabled={saving || updatingCatalog}>
                    {updatingCatalog ? "更新中..." : "检查更新"}
                  </DialogButton>
                </div>
              </PanelSectionRow>
              {catalogCsvPath && (
                <PanelSectionRow>
                  <div style={{ fontSize: "11px", opacity: 0.78, lineHeight: 1.4, wordBreak: "break-all" }}>
                    {`CSV：${catalogCsvPath}`}
                  </div>
                </PanelSectionRow>
              )}
            </PanelSection>
          </>
        ),
      },
      {
        id: "account",
        title: "账号",
        content: (
          <>
            <PanelSection title="天翼云账号">
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>
                  {login.logged_in
                    ? `当前已登录：${login.user_account || "未知账号"}`
                    : "当前未登录"}
                </div>
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={onClearLogin}
                  disabled={clearingLogin || saving || !login.logged_in}
                >
                  {clearingLogin ? "注销中..." : "注销天翼云账号"}
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>
            {/* 暂时隐藏百度网盘与城通网盘设置，后续恢复时直接取消注释即可。
            <PanelSection title="百度网盘账号">...</PanelSection>
            <PanelSection title="CTFile（城通网盘）">...</PanelSection>
            */}
          </>
        ),
      },
      {
        id: "cloud-save",
        title: "云存档",
        content: (
          <>
            <PanelSection title="云存档">
              <PanelSectionRow>
                <div style={{ display: "flex", gap: "8px", width: "100%" }}>
                  <div style={{ flex: "1 1 0", minWidth: 0 }}>
                    <ButtonItem
                      layout="below"
                      onClick={onStartCloudSaveUpload}
                      disabled={saving || clearingLogin || startingCloudSaveUpload || !login.logged_in || cloudSaveUploadState.running}
                    >
                      {startingCloudSaveUpload
                        ? "启动中..."
                        : cloudSaveUploadState.running
                          ? "上传进行中..."
                          : "上传云存档"}
                    </ButtonItem>
                  </div>
                  <div style={{ flex: "1 1 0", minWidth: 0 }}>
                    <ButtonItem
                      layout="below"
                      onClick={onOpenCloudSaveRestoreModal}
                      disabled={saving || clearingLogin || !login.logged_in}
                    >
                      下载云存档
                    </ButtonItem>
                  </div>
                </div>
              </PanelSectionRow>
              <PanelSectionRow>
                <ToggleField
                  label="自动上传云存档"
                  description="开启后，每次结束游戏时自动上传当前游戏存档"
                  checked={Boolean(settings.cloud_save_auto_upload)}
                  disabled={saving || clearingLogin || !login.logged_in}
                  onChange={(value: boolean) =>
                    void savePatch(
                      { cloud_save_auto_upload: value },
                      value ? "已开启自动上传云存档" : "已关闭自动上传云存档",
                    )
                  }
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{cloudSaveSummaryText}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "11px", lineHeight: 1.5, opacity: 0.88 }}>
                  {`${cloudSaveUploadState.message || "未开始"} | ${clampProgress(cloudSaveUploadState.progress).toFixed(1)}%`}
                </div>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "11px", lineHeight: 1.5, opacity: 0.72 }}>下载与恢复流程已移动到弹窗中。</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={showCloudSaveUploadDetails} disabled={cloudSaveUploadItems.length <= 0}>
                  查看上传明细（跳过/失败原因）
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>
          </>
        ),
      },
    ],
    [
      clearingLogin,
      login.logged_in,
      login.user_account,
      onPickDownloadDir,
      onPickInstallDir,
      onUseDownloadDirFromMedia,
      onUseInstallDirFromMedia,
      onClearLogin,
      onOpenCloudSaveRestoreModal,
      onReimportMissingSteamGames,
      onOpenRuntimeRepairWizard,
      onStartCloudSaveUpload,
      onSaveSplit,
      savePatch,
      saving,
      checkingMediaMounts,
      downloadingSwitchEmulator,
      onDownloadSwitchEmulator,
      cloudSaveSummaryText,
      cloudSaveUploadItems.length,
      showCloudSaveUploadDetails,
      cloudSaveUploadState.message,
      cloudSaveUploadState.progress,
      cloudSaveUploadState.running,
      startingCloudSaveUpload,
      loadingRuntimeRepairWizard,
      reimportingMissingSteamGames,
      runtimeRepairBusy,
      runtimeRepairItems.length,
      runtimeRepairProgressValue,
      runtimeRepairState.last_result.failed_steps,
      runtimeRepairState.running,
      runtimeRepairState.stage,
      runtimeRepairSummaryText,
      runtimeRepairStatusLine,
      showRuntimeRepairDetails,
      startingRuntimeRepair,
      settings.auto_delete_package,
      settings.aria2_fast_mode,
      settings.auto_switch_line,
      settings.cloud_save_auto_upload,
      settings.lsfg_enabled,
      settings.show_playtime_widget,
      settings.page_size,
      settings.download_dir,
      settings.install_dir,
      switchEmulatorStatus.installed,
      switchEmulatorStatus.loaded,
      splitDraft,
      catalogDate,
      catalogCsvPath,
      onUpdateCatalog,
      updatingCatalog,
    ],
  );
  const focusSettingsTabRow = useCallback(
    (tabId?: string) => {
      const classMap = getGamepadTabClassMap();
      if (!classMap || !settingsContainerRef.current) return;
      const tabClass = classMap.Tab;
      if (!tabClass) return;

      let target: HTMLElement | null = null;
      if (tabId) {
        const tabTitle = String(tabs.find((tab) => tab.id === tabId)?.title || "").trim();
        if (tabTitle) {
          const elements = settingsContainerRef.current.querySelectorAll(`.${tabClass}`);
          for (const element of Array.from(elements)) {
            const item = element as HTMLElement;
            if (String(item.textContent || "").trim() === tabTitle) {
              target = item;
              break;
            }
          }
        }
      }
      if (!target) {
        const activeClass = classMap.Active || classMap.Selected;
        const selector = activeClass ? `.${tabClass}.${activeClass}` : `.${tabClass}`;
        target = settingsContainerRef.current.querySelector(selector) as HTMLElement | null;
      }
      target?.focus?.();
    },
    [tabs],
  );

  useEffect(() => {
    const classMap = getGamepadTabClassMap();
    if (!classMap) return;
    const rowClass = classMap.TabsRowScroll || classMap.TabRowTabs;
    if (!rowClass) return;
    const handle = window.requestAnimationFrame(() => {
      const root = settingsContainerRef.current;
      if (!root) return;
      const row = root.querySelector(`.${rowClass}`) as HTMLElement | null;
      if (!row) return;
      row.style.scrollBehavior = "auto";
      row.scrollLeft = 0;
    });
    return () => window.cancelAnimationFrame(handle);
  }, [activeTab]);

  const onShowSettingsTab = useCallback(
    (tabId: string) => {
      focusSettingsTabRow();
      setActiveTab(tabId);
      window.requestAnimationFrame(() => focusSettingsTabRow(tabId));
    },
    [focusSettingsTabRow],
  );
  if (loading) {
    return (
      <PanelSection title="Freedeck 设置">
        <PanelSectionRow>加载中...</PanelSectionRow>
      </PanelSection>
    );
  }

  return (
    <div
      ref={settingsContainerRef}
      className="freedeck-settings-root"
      style={{
        paddingTop: 48,
        paddingBottom: 24,
        minHeight: "100%",
        boxSizing: "border-box",
        overflowX: "hidden",
      }}
    >
      <style>{tabStabilityCss}</style>
      <Tabs tabs={tabs} activeTab={activeTab} onShowTab={onShowSettingsTab} autoFocusContents={false} />
    </div>
  );
}

function Content() {
  const [loading, setLoading] = useState<boolean>(true);
  const [state, setState] = useState<PanelState>(EMPTY_STATE);
  const [openingLogin, setOpeningLogin] = useState<boolean>(false);
  const [openingLibrary, setOpeningLibrary] = useState<boolean>(false);
  const [cancelingTaskId, setCancelingTaskId] = useState<string>("");
  const [importingSteamTaskId, setImportingSteamTaskId] = useState<string>("");
  const [uninstallingKey, setUninstallingKey] = useState<string>("");
  const syncingRef = useRef<boolean>(false);
  const latestStateRef = useRef<PanelState>(EMPTY_STATE);
  const failedTaskDismissTimersRef = useRef<Record<string, number>>({});
  const promptedExeTasksRef = useRef<Set<string>>(new Set());
  const steamRestartWatchReadyRef = useRef<boolean>(false);
  const steamRestartTaskKeyRef = useRef<Map<string, string>>(new Map());

  const syncState = useCallback(async (pollMode: PanelPollMode = PANEL_POLL_MODE_IDLE) => {
    if (syncingRef.current) return;
    syncingRef.current = true;
    try {
      const result = await withTimeout(
        getTianyiPanelState({
          poll_mode: pollMode,
          visible: !document.hidden,
          has_focus: document.hasFocus(),
        }),
        PANEL_REQUEST_TIMEOUT_MS,
        "读取面板状态超时，请稍后重试",
      );
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "读取状态失败");
      }
      const next = result.data;
	      const normalized: PanelState = {
	        login: next.login || EMPTY_STATE.login,
	        baidu_login: (next as unknown as { baidu_login?: LoginState }).baidu_login || EMPTY_STATE.baidu_login,
	        ctfile_login: (next as unknown as { ctfile_login?: CtfileLoginState }).ctfile_login || EMPTY_STATE.ctfile_login,
	        installed: next.installed || EMPTY_STATE.installed,
	        tasks: next.tasks || [],
	        settings: Object.assign({}, EMPTY_SETTINGS, next.settings || {}),
	        library_url: next.library_url || "",
	        login_capture: (next as unknown as { login_capture?: Record<string, unknown> }).login_capture || {},
	        baidu_login_capture:
	          (next as unknown as { baidu_login_capture?: Record<string, unknown> }).baidu_login_capture || {},
	        power_diagnostics: next.power_diagnostics || {},
	      };
	      setUiPrefsFromSettings(normalized.settings);
	      latestStateRef.current = normalized;
	      setState(normalized);
	    } finally {
	      syncingRef.current = false;
	    }
  }, []);

  useEffect(() => {
    let alive = true;
    let firstRun = true;
    let timer = 0;

    const clearTimer = () => {
      if (!timer) return;
      window.clearTimeout(timer);
      timer = 0;
    };

    const scheduleNext = () => {
      if (!alive) return;
      clearTimer();
      const mode = resolvePanelPollMode(latestStateRef.current);
      const delay = pollIntervalByMode(mode);
      timer = window.setTimeout(() => {
        void runPoll(false);
      }, delay);
    };

    const runPoll = async (showErrorToast: boolean) => {
      if (!alive) return;
      const mode = resolvePanelPollMode(latestStateRef.current);
      try {
        await syncState(mode);
      } catch (error) {
        if (showErrorToast && alive) {
          toaster.toast({ title: "Freedeck", body: String(error) });
        }
      } finally {
        if (firstRun) {
          firstRun = false;
          if (alive) setLoading(false);
        }
        scheduleNext();
      }
    };

    const handleVisibilityChange = () => {
      if (!alive) return;
      clearTimer();
      if (document.hidden) {
        scheduleNext();
        return;
      }
      void runPoll(false);
    };

    const handleFocus = () => {
      if (!alive) return;
      clearTimer();
      void runPoll(false);
    };

    const handleBlur = () => {
      if (!alive) return;
      clearTimer();
      scheduleNext();
    };

    void runPoll(true);
    document.addEventListener("visibilitychange", handleVisibilityChange);
    window.addEventListener("focus", handleFocus);
    window.addEventListener("blur", handleBlur);

    return () => {
      alive = false;
      clearTimer();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleFocus);
      window.removeEventListener("blur", handleBlur);
    };
  }, [syncState]);

  useEffect(() => {
    const timers = failedTaskDismissTimersRef.current;
    const taskIds = new Set<string>();
    const nowMs = Date.now();

    for (const task of state.tasks || []) {
      const taskId = String(task.task_id || "").trim();
      if (!taskId) continue;
      taskIds.add(taskId);

      const status = String(task.status || "").trim().toLowerCase();
      if (status !== "error") continue;
      if (timers[taskId]) continue;

      const updatedAtSec = Number(task.updated_at || 0);
      const updatedAtMs =
        Number.isFinite(updatedAtSec) && updatedAtSec > 0 ? Math.floor(updatedAtSec * 1000) : nowMs;
      const dismissAtMs = updatedAtMs + FAILED_TASK_AUTO_DISMISS_SECONDS * 1000;
      const delayMs = Math.max(0, dismissAtMs - nowMs);

      timers[taskId] = window.setTimeout(() => {
        delete timers[taskId];
        void (async () => {
          try {
            const result = await cancelTianyiTask({ task_id: taskId });
            if (result.status === "success") {
              await syncState();
            }
          } catch {
            // 忽略自动清理失败，用户仍可手动清除。
          }
        })();
      }, delayMs);
    }

    for (const [taskId, handle] of Object.entries(timers)) {
      if (!taskIds.has(taskId)) {
        window.clearTimeout(handle);
        delete timers[taskId];
      }
    }
  }, [state.tasks, syncState]);

  useEffect(() => {
    return () => {
      const timers = failedTaskDismissTimersRef.current;
      for (const handle of Object.values(timers)) {
        window.clearTimeout(handle);
      }
      failedTaskDismissTimersRef.current = {};
    };
  }, []);

  const onLogin = useCallback(async () => {
    if (openingLogin) return;
    setOpeningLogin(true);
    try {
      const result = await getTianyiLoginUrl();
      if (result.status !== "success") {
        throw new Error(describeOpenError(result, "获取登录地址失败"));
      }
      openExternalUrl(result.url || result.data?.url || "");
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setOpeningLogin(false);
    }
  }, [openingLogin]);

  const onOpenLibrary = useCallback(async () => {
    if (openingLibrary) return;
    setOpeningLibrary(true);
    try {
      const result = await getTianyiLibraryUrl();
      if (result.status !== "success") throw new Error(describeOpenError(result, "获取游戏列表地址失败"));
      const baseUrl = result.url || result.data?.url || state.library_url || "";
      openExternalUrl(withQuery(baseUrl, { ts: Date.now() }));
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setOpeningLibrary(false);
    }
  }, [openingLibrary, state.library_url]);

  const onOpenSettings = useCallback(() => {
    try {
      Router.CloseSideMenus?.();
    } catch {
      // 忽略菜单关闭失败。
    }
    try {
      Navigation.Navigate(SETTINGS_ROUTE);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `打开设置失败：${error}` });
    }
  }, []);

  const executeCancelTask = useCallback(
    async (task: TaskItem, deleteFiles?: boolean) => {
      const taskId = String(task.task_id || "").trim();
      if (!taskId || cancelingTaskId) return;
      setCancelingTaskId(taskId);
      try {
        const status = String(task.status || "").trim().toLowerCase();
        const installStatus = String(task.install_status || "").trim().toLowerCase();
        const isInstallCancel = status === "complete" && ["pending", "installing"].includes(installStatus);
        const isInstallDismiss =
          status === "complete" && ["failed", "canceled", "skipped", "bundled"].includes(installStatus);

        const payload: CancelTaskPayload = { task_id: taskId };
        if (typeof deleteFiles === "boolean") payload.delete_files = deleteFiles;

        const result = isInstallCancel
          ? await cancelTianyiInstall(payload)
          : isInstallDismiss
            ? await dismissTianyiTask(payload)
            : await cancelTianyiTask(payload);
        if (result.status !== "success") {
          throw new Error(result.message || "取消失败");
        }

        const keepFiles = typeof deleteFiles === "boolean" && !deleteFiles;
        const removedFiles = typeof deleteFiles === "boolean" && deleteFiles;
        toaster.toast({
          title: "Freedeck",
          body:
            status === "error"
              ? removedFiles
                ? "已清除失败任务并删除文件"
                : keepFiles
                  ? "已清除失败任务（保留文件）"
                  : "已清除失败任务"
              : isInstallCancel
                ? "已取消安装"
                : isInstallDismiss
                  ? removedFiles
                    ? "已清除任务并删除文件"
                    : keepFiles
                      ? "已清除任务（保留文件）"
                      : "已清除任务"
                  : removedFiles
                    ? "已取消下载并删除文件"
                    : keepFiles
                      ? "已取消下载（保留文件）"
                      : "已取消下载",
        });
        await syncState();
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setCancelingTaskId("");
      }
    },
    [cancelingTaskId, syncState],
  );

  const performCancelTask = useCallback(
    (task: TaskItem) => {
      const status = String(task.status || "").trim().toLowerCase();
      const installStatus = String(task.install_status || "").trim().toLowerCase();
      const isClearAction =
        status === "error" || (status === "complete" && ["failed", "canceled", "skipped", "bundled"].includes(installStatus));
      if (isClearAction) {
        const title = String(task.game_title || task.game_name || task.file_name || "该任务");
        showModal(
          <ConfirmModal
            strTitle="清除任务"
            strDescription={`将清除「${title}」的下载/安装记录。\n\n是否同时删除已下载文件？删除后下次需要重新下载。`}
            strOKButtonText="删除文件并清除"
            strCancelButtonText="仅清除列表"
            onOK={() => {
              void executeCancelTask(task, true);
            }}
            onCancel={() => {
              void executeCancelTask(task, false);
            }}
          />,
        );
        return;
      }

      void executeCancelTask(task);
    },
    [executeCancelTask],
  );

  const performImportTaskToSteam = useCallback(
    async (taskId: string, exeRelPath: string) => {
      const targetTaskId = String(taskId || "").trim();
      const rel = String(exeRelPath || "").trim();
      if (!targetTaskId || !rel || importingSteamTaskId) return;
      setImportingSteamTaskId(targetTaskId);
      try {
        const result = await importTianyiTaskToSteam({ task_id: targetTaskId, exe_rel_path: rel });
        if (result.status !== "success") {
          throw new Error(result.message || "导入 Steam 失败");
        }
        const payload = (result.data || {}) as Record<string, unknown>;
        const ok = Boolean(payload.ok);
        const appId = Number(payload.appid_unsigned || 0);
        const message = String(payload.message || "").trim();
        if (ok) {
          toaster.toast({
            title: "Freedeck",
            body: appId > 0 ? `已加入 Steam（AppID ${appId}）` : "已加入 Steam",
          });
        } else {
          toaster.toast({ title: "Freedeck", body: `Steam 导入失败：${message || "未知错误"}` });
        }
        await syncState();
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setImportingSteamTaskId("");
      }
    },
    [importingSteamTaskId, syncState],
  );

  const openExeSelectionForTask = useCallback(
    (task: TaskItem) => {
      const taskId = String(task.task_id || "").trim();
      const title = String(task.game_title || task.game_name || task.file_name || "未命名任务");
      const installPath = String(task.installed_path || "").trim();
      const candidates = Array.isArray(task.steam_exe_candidates) ? task.steam_exe_candidates : [];
      if (!taskId || candidates.length < 2) {
        toaster.toast({ title: "Freedeck", body: "未找到可选择的启动程序" });
        return;
      }
      promptedExeTasksRef.current.add(taskId);
      showModal(
        <SteamExeSelectModal
          taskTitle={title}
          installPath={installPath}
          candidates={candidates}
          onConfirm={(exeRelPath) => {
            void performImportTaskToSteam(taskId, exeRelPath);
          }}
        />,
      );
    },
    [performImportTaskToSteam],
  );

  const installedGames = useMemo(() => state.installed?.preview || [], [state.installed]);
  const installedGameIds = useMemo(() => {
    const set = new Set<string>();
    for (const item of installedGames) {
      const gameId = String(item.game_id || "").trim();
      if (gameId) set.add(gameId);
    }
    return set;
  }, [installedGames]);
  const visibleTasks = useMemo(
    () =>
      (state.tasks || []).filter((task) => {
        if (isTaskAlreadyInstalled(task)) return false;
        const steamNeedsExe = String(task.steam_import_status || "").trim().toLowerCase() === "needs_exe";
        const gameId = String(task.game_id || "").trim();
        if (!steamNeedsExe && gameId && installedGameIds.has(gameId)) return false;
        return true;
      }),
    [installedGameIds, state.tasks],
  );

  useEffect(() => {
    if (importingSteamTaskId) return;
    for (const task of state.tasks || []) {
      const taskId = String(task.task_id || "").trim();
      if (!taskId) continue;
      if (promptedExeTasksRef.current.has(taskId)) continue;
      const downloadStatus = String(task.status || "").trim().toLowerCase();
      const installStatus = String(task.install_status || "").trim().toLowerCase();
      const steamStatus = String(task.steam_import_status || "").trim().toLowerCase();
      const candidates = Array.isArray(task.steam_exe_candidates) ? task.steam_exe_candidates : [];
      if (downloadStatus !== "complete") continue;
      if (installStatus !== "installed") continue;
      if (steamStatus !== "needs_exe") continue;
      if (candidates.length < 2) continue;
      openExeSelectionForTask(task);
      break;
    }
  }, [importingSteamTaskId, openExeSelectionForTask, state.tasks]);

  useEffect(() => {
    if (loading) return;
    const tasks = state.tasks || [];
    const tracked = steamRestartTaskKeyRef.current;
    if (!steamRestartWatchReadyRef.current) {
      for (const task of tasks) {
        const taskId = String(task.task_id || "").trim();
        if (!taskId) continue;
        const installStatus = String(task.install_status || "").trim().toLowerCase();
        const steamStatus = String(task.steam_import_status || "").trim().toLowerCase();
        tracked.set(taskId, `${installStatus}|${steamStatus}`);
      }
      steamRestartWatchReadyRef.current = true;
      return;
    }

    for (const task of tasks) {
      const taskId = String(task.task_id || "").trim();
      if (!taskId) continue;
      const installStatus = String(task.install_status || "").trim().toLowerCase();
      const steamStatus = String(task.steam_import_status || "").trim().toLowerCase();
      const nextKey = `${installStatus}|${steamStatus}`;
      const prevKey = tracked.get(taskId);
      if (prevKey === nextKey) continue;
      tracked.set(taskId, nextKey);
      if (installStatus === "installed" && steamStatus === "done") {
        const title = String(task.game_title || task.game_name || task.file_name || "该游戏");
        showRestartSteamPrompt(`已导入「${title}」到 Steam 库，重启 Steam 后可立即生效。`);
        break;
      }
    }
  }, [loading, state.tasks]);
  const sortedInstalledGames = useMemo(() => {
    const list = [...installedGames];
    list.sort((a, b) =>
      String(a.title || "").localeCompare(String(b.title || ""), "zh-Hans-CN", {
        sensitivity: "base",
        numeric: true,
      }),
    );
    return list;
  }, [installedGames]);
  const loginStatusText = useMemo(() => {
    if (!state.login.logged_in) return "没登录";
    const account = String(state.login.user_account || "").trim() || "未知账号";
    return `已登录：${account}（账号）`;
  }, [state.login.logged_in, state.login.user_account]);

  const performUninstallInstalledGame = useCallback(
    async (item: InstalledGameItem, deleteProtonFiles = false) => {
      const gameId = String(item.game_id || "").trim();
      const installPath = String(item.install_path || "").trim();
      const title = String(item.title || gameId || "该游戏");
      if (!installPath) {
        toaster.toast({ title: "Freedeck", body: "安装路径为空，无法卸载" });
        return;
      }
      if (uninstallingKey) return;

      const key = `${gameId}::${installPath}`;
      setUninstallingKey(key);
      try {
        const result = await uninstallTianyiInstalledGame({
          game_id: gameId,
          install_path: installPath,
          delete_files: true,
          delete_proton_files: deleteProtonFiles,
        });
        if (result.status !== "success") {
          throw new Error(result.message || "卸载失败");
        }
        toaster.toast({
          title: "Freedeck",
          body: result.data?.proton_files_deleted ? `已卸载：${title}（已删除 Proton 文件）` : `已卸载：${title}`,
        });
        const warning = String(result.data?.warning || "").trim();
        if (warning) {
          toaster.toast({ title: "Freedeck", body: warning });
        }
        await syncState();
        showRestartSteamPrompt(`已卸载「${title}」，重启 Steam 后可立即同步库内条目。`);
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setUninstallingKey("");
      }
    },
    [syncState, uninstallingKey],
  );

  const onUninstallInstalledGame = useCallback(
    (item: InstalledGameItem) => {
      const gameId = String(item.game_id || "").trim();
      const installPath = String(item.install_path || "").trim();
      const title = String(item.title || gameId || "该游戏");
      if (!installPath) {
        toaster.toast({ title: "Freedeck", body: "安装路径为空，无法卸载" });
        return;
      }
      if (uninstallingKey) return;
      const Modal = (props: { closeModal?: () => void }) => {
        const [deleteProtonFiles, setDeleteProtonFiles] = useState(false);

        return (
          <ConfirmModal
            strTitle="确认卸载"
            strOKButtonText="确认卸载"
            strCancelButtonText="取消"
            onOK={() => {
              props.closeModal?.();
              void performUninstallInstalledGame(item, deleteProtonFiles);
            }}
            onCancel={() => {
              props.closeModal?.();
            }}
          >
            <>
              <div style={{ fontSize: "13px", lineHeight: 1.55, whiteSpace: "pre-wrap" }}>
                {`确定卸载「${title}」吗？\n\n将删除安装目录：\n${installPath}`}
              </div>
              <div style={{ marginTop: "10px" }}>
                <ToggleField
                  label="同时删除 Proton 文件"
                  description="删除该游戏在 Steam compatdata 中生成的前缀文件"
                  checked={deleteProtonFiles}
                  onChange={(value: boolean) => setDeleteProtonFiles(Boolean(value))}
                />
              </div>
            </>
          </ConfirmModal>
        );
      };

      showModal(<Modal />);
    },
    [performUninstallInstalledGame, uninstallingKey],
  );

  if (loading) {
    return (
      <PanelSection>
        <PanelSectionRow>加载中...</PanelSectionRow>
      </PanelSection>
    );
  }

  return (
    <>
      <PanelSection>
        <PanelSectionRow>
          <div
            style={{
              width: "100%",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={loginStatusText}
          >
            {loginStatusText}
          </div>
        </PanelSectionRow>
        <PanelSectionRow>
          {!state.login.logged_in && (
            <ButtonItem layout="below" onClick={onLogin} disabled={openingLogin || openingLibrary}>
              {openingLogin ? "登录入口准备中..." : "登录"}
            </ButtonItem>
          )}
          <ButtonItem layout="below" onClick={onOpenLibrary} disabled={openingLibrary || openingLogin}>
            {openingLibrary ? "游戏列表准备中..." : "游戏列表"}
          </ButtonItem>
          <ButtonItem layout="below" onClick={onOpenSettings}>设置</ButtonItem>
        </PanelSectionRow>
      </PanelSection>

      {visibleTasks.length > 0 && (
        <PanelSection title={`下载列表（${visibleTasks.length}）`}>
          {visibleTasks.slice(0, 20).map((task) => (
            <PanelSectionRow key={task.task_id}>
              {TaskProgressRow(task, {
                canceling: cancelingTaskId === task.task_id,
                busy: Boolean(cancelingTaskId) || Boolean(importingSteamTaskId),
                selectingExe: importingSteamTaskId === task.task_id,
                onCancel: performCancelTask,
                onSelectExe: openExeSelectionForTask,
              })}
            </PanelSectionRow>
          ))}
        </PanelSection>
      )}

      {sortedInstalledGames.length > 0 && (
        <PanelSection title={`游戏预览（已安装 ${state.installed.total || sortedInstalledGames.length}）`}>
          {sortedInstalledGames.map((item, index) => {
            const uninstallKey = `${item.game_id || ""}::${item.install_path || ""}`;
            const uninstalling = uninstallingKey === uninstallKey;
            return (
              <PanelSectionRow key={`${item.game_id || item.title || "game"}_${index}`}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px", width: "100%" }}>
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <div
                      style={{
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                      title={item.title || "未命名游戏"}
                    >
                      {item.title || "未命名游戏"}
                    </div>
                    <div style={{ fontSize: "12px" }}>
                      {`${item.size_text || "-"}${item.status ? ` | ${item.status}` : ""}`}
                    </div>
                    <div style={{ fontSize: "12px" }}>
                      {`游玩时长：${formatPlaytimeText(item.playtime_seconds || 0, item.playtime_text)}${
                        item.playtime_active ? "（进行中）" : ""
                      }`}
                    </div>
                    <div
                      style={{
                        fontSize: "12px",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                      title={item.install_path || "-"}
                    >
                      {item.install_path || "-"}
                    </div>
                  </div>
                  <Focusable style={{ flex: "0 0 auto" }}>
                    <DialogButton
                      onClick={() => onUninstallInstalledGame(item)}
                      onOKButton={() => onUninstallInstalledGame(item)}
                      disabled={Boolean(uninstallingKey) || uninstalling}
                      style={{
                        minWidth: "88px",
                        borderRadius: "10px",
                        border: "1px solid rgba(255, 255, 255, 0.26)",
                        background: uninstalling ? "rgba(255, 106, 106, 0.34)" : "rgba(255, 106, 106, 0.2)",
                        color: "#ffe8e8",
                      }}
                    >
                      {uninstalling ? "卸载中..." : "卸载"}
                    </DialogButton>
                  </Focusable>
                </div>
              </PanelSectionRow>
            );
          })}
        </PanelSection>
      )}
    </>
  );
}

export default definePlugin(() => {
  routerHook.addRoute(SETTINGS_ROUTE, SettingsPage);
  const unpatchLibraryPlaytime = installLibraryPlaytimePatch();
  let uninstallGameActionReporter = () => {};
  try {
    uninstallGameActionReporter = installGlobalGameActionReporter();
  } catch {
    // 忽略全局游戏事件监听初始化失败，避免影响插件主 UI。
  }

  return {
    name: "freedeck",
    titleView: <div className={staticClasses.Title}>freedeck</div>,
    content: <Content />,
    icon: <FaCloudDownloadAlt />,
    onDismount() {
      uninstallGameActionReporter();
      unpatchLibraryPlaytime();
      routerHook.removeRoute(SETTINGS_ROUTE);
    },
  };
});
