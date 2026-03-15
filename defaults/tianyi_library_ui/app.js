const { createApp } = window.Vue;
const INSTALL_PLAN_CACHE_TTL_MS = 5 * 60 * 1000;
const INSTALL_PLAN_PREFETCH_CONCURRENCY = 2;
const INSTALL_PLAN_FOCUS_PREFETCH_DELAY_MS = 360;
const INSTALL_PLAN_FOCUS_PREFETCH_LOOKAHEAD = 2;
const INSTALL_PLAN_PREFETCH_SEED_COUNT = 6;
const COVER_MISS_CACHE_TTL_MS = 10 * 60 * 1000;
const SWITCH_EMULATOR_NOTICE_KEY = "freedeck_switch_emulator_notice_dismissed";

function buildUrl(path, query = {}) {
  const url = new URL(path, window.location.origin);
  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

async function apiGet(path, query = {}) {
  const url = buildUrl(path, query);
  const resp = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  const contentType = String(resp.headers.get("content-type") || "");
  const isJson = contentType.toLowerCase().includes("application/json");
  if (isJson) {
    try {
      const payload = await resp.json();
      if (payload && typeof payload === "object") return payload;
      return { status: "error", message: `接口返回异常（HTTP ${resp.status}）`, url };
    } catch (error) {
      return { status: "error", message: `解析接口返回失败（HTTP ${resp.status}）`, url, diagnostics: { error: String(error) } };
    }
  }
  let text = "";
  try {
    text = String(await resp.text());
  } catch (_error) {
    text = "";
  }
  return {
    status: "error",
    message: `接口返回非 JSON（HTTP ${resp.status}）`,
    url,
    diagnostics: { content_type: contentType, preview: text.slice(0, 240) },
  };
}

async function apiPost(path, body = {}) {
  const resp = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  return resp.json();
}

createApp({
  data() {
    return {
      view: "library",
      uiMode: "full",
      pageKind: "library",
      catalog: {
        loading: false,
        loaded: false,
        items: [],
        total: 0,
        error: "",
      },
      catalogReloadQueued: false,
      filters: {
        category: "",
        sort: "default",
      },
      categoryMenuOpen: false,
      sortMenuOpen: false,
      catalogGroups: [],
      displayLimit: 96,
      displayStep: 72,
      queryDebounceTimer: null,
      catalogApplyTimer: null,
      settings: {
        download_dir: "",
        install_dir: "",
        split_count: 16,
        page_size: 50,
      },
      list: { total: 0, page: 1, page_size: 50, items: [] },
      query: "",
      activeQuery: "",
      showResults: true,
      hintText: "",
      message: "",
      coverCache: {},
      coverMetaCache: {},
      coverPending: {},
      coverMissCache: {},
      focusedIndex: 0,
      gamepadTimer: null,
      gamepadPressed: {},
      gamepadRepeatState: {},
      gamepadPollMs: 160,
      installDialog: {
        visible: false,
        loading: false,
        submitting: false,
        plan: null,
        game: null,
        variants: [],
        selectedVariantIndex: 0,
        requestSeq: 0,
        versionMenuOpen: false,
      },
      installPlanCache: {},
      installPlanPending: {},
      installPlanPrefetchStarted: false,
      installPlanFocusTimer: null,
      installPlanFocusLastToken: "",
      emulatorNotice: {
        visible: false,
        title: "",
        message: "",
      },
      customImport: {
        share_url: "",
        pwd: "",
        hintText: "",
        loading: false,
        submitting: false,
        plan: null,
        selected: {},
      },
      ad: {
        remoteEnabled: false,
        dismissed: true,
        images: [],
        imageIndex: 0,
        linkUrl: "",
        remoteFlagUrl: "",
      },
    };
  },
  computed: {
    isEmulatorPage() {
      const kind = String(this.pageKind || "").trim().toLowerCase();
      return kind === "emulator_switch";
    },
    isSwitchEmulatorPage() {
      return String(this.pageKind || "").trim().toLowerCase() === "emulator_switch";
    },
    isGbaEmulatorPage() {
      return false;
    },
    emulatorPageSize() {
      return 12;
    },
    emulatorPages() {
      if (!this.isEmulatorPage) return [];
      const items = Array.isArray(this.list && this.list.items) ? this.list.items : [];
      const pageSize = 12;
      const pages = [];
      for (let i = 0; i < items.length; i += pageSize) {
        pages.push(items.slice(i, i + pageSize));
      }
      return pages;
    },
    catalogStatusText() {
      const total = Number(this.catalog && this.catalog.total) || 0;
      const loaded = Array.isArray(this.catalog && this.catalog.items) ? this.catalog.items.length : 0;
      if (this.catalog && this.catalog.loading) {
        if (total > 0) return `同步中 ${loaded}/${total}`;
        return `同步中 ${loaded}/...`;
      }
      if (this.catalog && this.catalog.error) {
        return "游戏列表加载失败";
      }
      if (total > 0) return `已加载 ${total} 个游戏`;
      if (loaded > 0) return `已加载 ${loaded} 个游戏`;
      return "准备中...";
    },
    sortOptions() {
      return [
        { value: "default", label: "默认" },
        { value: "title_asc", label: "名称 A-Z" },
        { value: "size_desc", label: "大小 大→小" },
        { value: "size_asc", label: "大小 小→大" },
      ];
    },
    categoryOptions() {
      const counts = new Map();
      const items = Array.isArray(this.catalog && this.catalog.items) ? this.catalog.items : [];
      for (const item of items) {
        if (!item || typeof item !== "object") continue;
        const category = String(item.categories || "").trim();
        if (!category) continue;
        counts.set(category, (counts.get(category) || 0) + 1);
      }
      const list = Array.from(counts.entries()).map(([value, count]) => ({ value, count, label: `${value} (${count})` }));
      list.sort((a, b) => {
        if (a.count !== b.count) return b.count - a.count;
        return String(a.value).localeCompare(String(b.value), "zh-Hans-CN", { sensitivity: "base" });
      });
      const total = Number((this.catalog && this.catalog.total) || 0) || items.length;
      return [{ value: "", count: total, label: `全部 (${total || 0})` }, ...list];
    },
    selectedCategoryLabel() {
      const target = String((this.filters && this.filters.category) || "").trim();
      const options = Array.isArray(this.categoryOptions) ? this.categoryOptions : [];
      const hit = options.find((opt) => String(opt && opt.value) === target);
      return (hit && hit.label) || (target || "全部");
    },
    selectedSortLabel() {
      const target = String((this.filters && this.filters.sort) || "default").trim() || "default";
      const options = Array.isArray(this.sortOptions) ? this.sortOptions : [];
      const hit = options.find((opt) => String(opt && opt.value) === target);
      return (hit && hit.label) || "默认";
    },
    canLoadMore() {
      const groups = Array.isArray(this.catalogGroups) ? this.catalogGroups : [];
      const limit = Number(this.displayLimit || 0);
      if (!groups.length) return false;
      if (!Number.isFinite(limit) || limit <= 0) return false;
      return limit < groups.length;
    },
    loadMoreFocusIndex() {
      const cardStart = this.uiMode === "full" ? (this.isEmulatorPage ? 2 : 4) : 3;
      const cardCount = Array.isArray(this.list && this.list.items) ? this.list.items.length : 0;
      return cardStart + Math.max(0, cardCount);
    },
    totalPages() {
      const total = Number(this.list.total || 0);
      const pageSize = Number(this.list.page_size || 50);
      if (total <= 0 || pageSize <= 0) return 1;
      return Math.max(1, Math.ceil(total / pageSize));
    },
    installVariantCount() {
      const variants = this.installDialog && Array.isArray(this.installDialog.variants) ? this.installDialog.variants : [];
      return variants.length;
    },
    selectedInstallVersionLabel() {
      const variant = this.currentInstallVariant() || this.installDialog.game || null;
      const index = Number(this.installDialog && this.installDialog.selectedVariantIndex) || 0;
      return this.installVersionLabel(variant, index);
    },
    modalCanConfirm() {
      if (!this.installDialog.plan) return false;
      if (this.installDialog.loading || this.installDialog.submitting) return false;
      return this.storageEnough(this.installDialog.plan);
    },
    customSelectedFileIds() {
      const plan = this.customImport.plan;
      const files = plan && Array.isArray(plan.files) ? plan.files : [];
      const selected = this.customImport.selected && typeof this.customImport.selected === "object" ? this.customImport.selected : {};
      const out = [];
      for (const file of files) {
        if (!file || typeof file !== "object") continue;
        const fileId = String(file.file_id || "").trim();
        if (!fileId) continue;
        if (selected[fileId]) out.push(fileId);
      }
      return out;
    },
    customFileCount() {
      const plan = this.customImport.plan;
      const files = plan && Array.isArray(plan.files) ? plan.files : [];
      return Math.max(0, files.length);
    },
    customDownloadFocusIndex() {
      if (this.view !== "custom") return -1;
      return 4 + this.customFileCount;
    },
    customClearFocusIndex() {
      if (this.view !== "custom") return -1;
      return 4 + this.customFileCount + 1;
    },
    shouldShowResults() {
      if (this.view !== "library") return false;
      return this.uiMode === "full" ? true : Boolean(this.showResults);
    },
    cardFocusStart() {
      return this.uiMode === "full" ? (this.isEmulatorPage ? 2 : 4) : 3;
    },
    adVisible() {
      return false;
    },
    adImageUrl() {
      return "";
    },
  },
  mounted() {
    this.bootstrap();
    this.applyInitialViewFromQuery();
    this._deckyKeyboardSeq = 0;
    this._deckyKeyboardPending = {};
    this._searchKeyboardAutoSuppressUntil = Date.now() + 1200;
    if (this.isSwitchEmulatorPage) {
      void this.checkSwitchEmulatorStatus();
    }
    if (this.uiMode === "search") {
    }
    window.addEventListener("keydown", this.onGlobalKeyDown, { passive: false });
    window.addEventListener("message", this.onDeckyKeyboardMessage);
    document.addEventListener("focusin", this.onDocumentFocusIn);
    window.addEventListener("gamepadconnected", this.onGamepadConnected);
    window.addEventListener("gamepaddisconnected", this.onGamepadDisconnected);
    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("focus", this.onWindowFocus);
    window.addEventListener("blur", this.onWindowBlur);
    this.syncGamepadPolling();
    this.setFocusIndex(this.view === "custom" ? 1 : 0, { scroll: false });
    if (this.view === "library") {
      void this.loadAllCatalog();
    }
  },
  beforeUnmount() {
    window.removeEventListener("keydown", this.onGlobalKeyDown);
    window.removeEventListener("message", this.onDeckyKeyboardMessage);
    document.removeEventListener("focusin", this.onDocumentFocusIn);
    window.removeEventListener("gamepadconnected", this.onGamepadConnected);
    window.removeEventListener("gamepaddisconnected", this.onGamepadDisconnected);
    document.removeEventListener("visibilitychange", this.onVisibilityChange);
    window.removeEventListener("focus", this.onWindowFocus);
    window.removeEventListener("blur", this.onWindowBlur);
    this.stopGamepadPolling();
    if (this.queryDebounceTimer) {
      window.clearTimeout(this.queryDebounceTimer);
      this.queryDebounceTimer = null;
    }
    if (this.catalogApplyTimer) {
      window.clearTimeout(this.catalogApplyTimer);
      this.catalogApplyTimer = null;
    }
    if (this.searchKeyboardFocusTimer) {
      window.clearTimeout(this.searchKeyboardFocusTimer);
      this.searchKeyboardFocusTimer = null;
    }
  },
  methods: {
    getKeyboardHostWindows() {
      const hosts = [];
      const seen = new Set();
      const appendHost = (candidate, label) => {
        if (!candidate) return;
        if (seen.has(candidate)) return;
        seen.add(candidate);
        hosts.push({ target: candidate, label });
      };

      appendHost(window, "self");
      try {
        appendHost(window.parent, "parent");
      } catch (_error) {
        // ignore
      }
      try {
        appendHost(window.top, "top");
      } catch (_error) {
        // ignore
      }
      return hosts;
    },
    getDeckyKeyboardBridgeTargets() {
      const targets = [];
      const seen = new Set();
      const appendTarget = (candidate, label) => {
        if (!candidate || candidate === window) return;
        if (seen.has(candidate)) return;
        if (typeof candidate.postMessage !== "function") return;
        seen.add(candidate);
        targets.push({ target: candidate, label });
      };

      try {
        appendTarget(window.parent, "parent");
      } catch (_error) {
        // ignore
      }
      try {
        appendTarget(window.top, "top");
      } catch (_error) {
        // ignore
      }
      return targets;
    },
    postDeckyBridgeMessage(message) {
      const targets = this.getDeckyKeyboardBridgeTargets();
      const via = [];
      for (const entry of targets) {
        try {
          entry.target.postMessage(message, "*");
          via.push(`bridge.${entry.label}`);
        } catch (_error) {
          // ignore
        }
      }
      return { ok: via.length > 0, via };
    },
    debugLog(message, details = null) {
      const text = String(message || "").trim();
      if (!text) return;
      if (!this._debugLogState || typeof this._debugLogState !== "object") {
        this._debugLogState = { lastAt: 0, lastKey: "" };
      }
      const state = this._debugLogState;
      const now = Date.now();
      const lastAt = Number(state.lastAt || 0) || 0;
      const lastKey = String(state.lastKey || "");
      if (lastKey === text && now - lastAt < 300) return;
      state.lastAt = now;
      state.lastKey = text;

      try {
        apiPost("/api/tianyi/debug/log", { message: text, details }).catch(() => {});
      } catch (_error) {
        // ignore
      }

      if (!this.canUseDeckyKeyboardBridge()) return;
      try {
        this.postDeckyBridgeMessage({ type: "freedeck:debug:log", message: text, details });
      } catch (_error) {
        // ignore
      }
    },
    canUseDeckyKeyboardBridge() {
      let inIframe = false;
      try {
        inIframe = window.self !== window.top;
      } catch (_error) {
        inIframe = true;
      }
      if (!inIframe) return false;
      try {
        if (!window.parent || window.parent === window) return false;
      } catch (_error) {
        return false;
      }
      return this.getDeckyKeyboardBridgeTargets().length > 0;
    },
    showSteamVirtualKeyboard() {
      const attempt = {
        ok: false,
        via: [],
        in_iframe: false,
        bridge_available: false,
        vk_manager_visible: false,
        navigator_virtual_keyboard: false,
        host_labels: [],
        steam_client: {
          available: false,
          available_hosts: [],
          input_show_vk: false,
          input_show_kb: false,
          system_show_vk: false,
          system_show_kb: false,
        },
      };
      const hosts = this.getKeyboardHostWindows();
      attempt.host_labels = hosts.map((entry) => entry.label);
      try {
        try {
          attempt.in_iframe = window.self !== window.top;
        } catch (_error) {
          attempt.in_iframe = true;
        }
        for (const host of hosts) {
          const steamClient = host.target && host.target.SteamClient ? host.target.SteamClient : null;
          const input = steamClient && steamClient.Input;
          const system = steamClient && steamClient.System;
          if (steamClient) {
            attempt.steam_client.available = true;
            attempt.steam_client.available_hosts.push(host.label);
          }
          attempt.steam_client.input_show_vk =
            attempt.steam_client.input_show_vk || Boolean(input && typeof input.ShowVirtualKeyboard === "function");
          attempt.steam_client.input_show_kb =
            attempt.steam_client.input_show_kb || Boolean(input && typeof input.ShowKeyboard === "function");
          attempt.steam_client.system_show_vk =
            attempt.steam_client.system_show_vk || Boolean(system && typeof system.ShowVirtualKeyboard === "function");
          attempt.steam_client.system_show_kb =
            attempt.steam_client.system_show_kb || Boolean(system && typeof system.ShowKeyboard === "function");
          if (input && typeof input.ShowVirtualKeyboard === "function") {
            input.ShowVirtualKeyboard();
            attempt.ok = true;
            attempt.via.push(`${host.label}.steamclient.input.ShowVirtualKeyboard`);
          }
          if (input && typeof input.ShowKeyboard === "function") {
            input.ShowKeyboard();
            attempt.ok = true;
            attempt.via.push(`${host.label}.steamclient.input.ShowKeyboard`);
          }
          if (system && typeof system.ShowVirtualKeyboard === "function") {
            system.ShowVirtualKeyboard();
            attempt.ok = true;
            attempt.via.push(`${host.label}.steamclient.system.ShowVirtualKeyboard`);
          }
          if (system && typeof system.ShowKeyboard === "function") {
            system.ShowKeyboard();
            attempt.ok = true;
            attempt.via.push(`${host.label}.steamclient.system.ShowKeyboard`);
          }
        }
      } catch (_error) {
        // ignore
      }

      try {
        for (const host of hosts) {
          const manager =
            host.target &&
            host.target.SteamUIStore &&
            host.target.SteamUIStore.ActiveWindowInstance &&
            host.target.SteamUIStore.ActiveWindowInstance.VirtualKeyboardManager;
          attempt.vk_manager_visible =
            attempt.vk_manager_visible || Boolean(manager && typeof manager.SetVirtualKeyboardVisible === "function");
          if (manager && typeof manager.SetVirtualKeyboardVisible === "function") {
            manager.SetVirtualKeyboardVisible();
            attempt.ok = true;
            attempt.via.push(`${host.label}.steamui.VirtualKeyboardManager.SetVirtualKeyboardVisible`);
          }
          if (manager && typeof manager.SetVirtualKeyboardVisible_ === "function") {
            manager.SetVirtualKeyboardVisible_();
            attempt.ok = true;
            attempt.via.push(`${host.label}.steamui.VirtualKeyboardManager.SetVirtualKeyboardVisible_`);
          }
        }
      } catch (_error) {
        // ignore
      }

      try {
        const browserVk = navigator && navigator.virtualKeyboard;
        attempt.navigator_virtual_keyboard = Boolean(browserVk && typeof browserVk.show === "function");
        if (browserVk && typeof browserVk.show === "function") {
          browserVk.show();
          attempt.ok = true;
          attempt.via.push("navigator.virtualKeyboard.show");
        }
      } catch (_error) {
        // ignore
      }

      attempt.bridge_available = this.canUseDeckyKeyboardBridge();
      if (!attempt.bridge_available) {
        this._lastKeyboardAttempt = attempt;
        return Boolean(attempt.ok);
      }
      try {
        const bridgeResult = this.postDeckyBridgeMessage({ type: "freedeck:keyboard:show" });
        if (bridgeResult.ok) {
          attempt.ok = true;
          attempt.via.push(...bridgeResult.via);
        }
        this._lastKeyboardAttempt = attempt;
        return Boolean(bridgeResult.ok || attempt.ok);
      } catch (_error) {
        this._lastKeyboardAttempt = attempt;
        return Boolean(attempt.ok);
      }
    },
    requestKeyboardForElement(elementId) {
      const id = String(elementId || "").trim();
      if (!id) return false;
      try {
        const el = document.getElementById(id);
        if (el && typeof el.focus === "function") {
          el.focus();
        }
      } catch (_error) {
        // ignore
      }
      return this.showSteamVirtualKeyboard();
    },
    onDeckyKeyboardMessage(event) {
      const payload = event && event.data;
      if (!payload || typeof payload !== "object") return;
      if (String(payload.type || "") !== "freedeck:keyboard:response") return;
      const requestId = String(payload.requestId || "").trim();
      if (!requestId) return;
      const pending = this._deckyKeyboardPending && typeof this._deckyKeyboardPending === "object" ? this._deckyKeyboardPending : {};
      const resolver = pending[requestId];
      if (typeof resolver !== "function") return;
      try {
        delete pending[requestId];
      } catch (_error) {
        // ignore
      }
      const ok = Boolean(payload.ok);
      const value = String(payload.value || "");
      const reasonRaw = String(payload.reason || "").trim();
      const reason = reasonRaw || (ok ? "ok" : "cancel");
      resolver({ ok, value, reason });
    },
    requestDeckyKeyboard(options = {}) {
      const title = String(options.title || "").trim() || "输入";
      const placeholder = String(options.placeholder || "").trim();
      const value = String(options.value || "");
      const password = Boolean(options.password);
      const field = String(options.field || "");
      const canBridge = this.canUseDeckyKeyboardBridge();

      return (async () => {
        if (canBridge) {
          try {
            const httpResult = await apiPost("/api/tianyi/keyboard/request", {
              title,
              placeholder,
              value,
              password,
              field,
              source: String(this.pageKind || this.uiMode || "library"),
            });
            if (httpResult && String(httpResult.status || "").trim().toLowerCase() === "success") {
              const data = httpResult.data && typeof httpResult.data === "object" ? httpResult.data : {};
              const ok = Boolean(data.ok);
              const nextValue = ok ? String(data.value || "") : value;
              const reasonRaw = String(data.reason || "").trim();
              return { ok, value: nextValue, reason: reasonRaw || (ok ? "ok" : "cancel") };
            }
            this.debugLog("keyboard:http_bridge_unavailable", {
              field,
              status: String((httpResult && httpResult.status) || ""),
              message: String((httpResult && httpResult.message) || ""),
            });
          } catch (_error) {
            this.debugLog("keyboard:http_bridge_error", {
              field,
              error: String(_error),
            });
          }
        }

        if (!canBridge) return { ok: false, value: "", reason: "unavailable" };
        if (!this._deckyKeyboardPending || typeof this._deckyKeyboardPending !== "object") {
          this._deckyKeyboardPending = {};
        }
        if (!Number.isFinite(Number(this._deckyKeyboardSeq || 0))) {
          this._deckyKeyboardSeq = 0;
        }
        this._deckyKeyboardSeq += 1;
        const requestId = `kbd_${Date.now()}_${this._deckyKeyboardSeq}_${Math.random().toString(16).slice(2)}`;

        const message = {
          type: "freedeck:keyboard:request",
          requestId,
          title,
          placeholder,
          value,
          password,
          field,
        };

        return await new Promise((resolve) => {
          let done = false;
          const finish = (result) => {
            if (done) return;
            done = true;
            try {
              delete this._deckyKeyboardPending[requestId];
            } catch (_error) {
              // ignore
            }
            resolve(result);
          };

          const timeoutMs = 20 * 1000;
          const timer = window.setTimeout(() => finish({ ok: false, value: "", reason: "timeout" }), timeoutMs);
          this._deckyKeyboardPending[requestId] = (result) => {
            window.clearTimeout(timer);
            finish(result);
          };

          try {
            const bridgeResult = this.postDeckyBridgeMessage(message);
            if (!bridgeResult.ok) {
              window.clearTimeout(timer);
              finish({ ok: false, value: "", reason: "post_failed" });
            }
          } catch (_error) {
            window.clearTimeout(timer);
            finish({ ok: false, value: "", reason: "post_failed" });
          }
        });
      })();
    },
    isSearchKeyboardEligible() {
      if (this.view !== "library") return false;
      if (this.installDialog && this.installDialog.visible) return false;
      if (this.isCatalogMenuOpen()) return false;
      if (document.hidden) return false;
      return Boolean(document.getElementById("search-input"));
    },
    queueSearchKeyboardOpen(trigger = "focus") {
      if (!this.isSearchKeyboardEligible()) return;
      const token = `${trigger}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      this.searchKeyboardFocusToken = token;
      if (this.searchKeyboardFocusTimer) {
        window.clearTimeout(this.searchKeyboardFocusTimer);
        this.searchKeyboardFocusTimer = null;
      }
      this.searchKeyboardFocusTimer = window.setTimeout(() => {
        this.searchKeyboardFocusTimer = null;
        if (String(this.searchKeyboardFocusToken || "") !== token) return;
        if (!this.isSearchKeyboardEligible()) return;
        const input = document.getElementById("search-input");
        if (!input) return;
        const active = document.activeElement;
        if (active !== input && this.focusedIndex !== 0) return;
        this.debugLog(`ui:search_${trigger}_queued`, {
          mode: this.uiMode,
          page: this.pageKind,
          active_id: active && active.id ? String(active.id) : "",
        });
        void this.openSearchKeyboard(trigger);
      }, 80);
    },
    async openSearchKeyboard(trigger = "focus") {
      if (!this._textInputFocusState || typeof this._textInputFocusState !== "object") {
        this._textInputFocusState = { searchIgnoreUntil: 0, searchLock: false };
      }
      const state = this._textInputFocusState;
      const now = Date.now();
      const autoSuppressUntil = Number(this._searchKeyboardAutoSuppressUntil || 0) || 0;
      if ((trigger === "focus" || trigger === "focusin") && now < autoSuppressUntil) {
        this.debugLog(`ui:search_${trigger}_suppressed`, {
          mode: this.uiMode,
          page: this.pageKind,
          auto_suppress_until: autoSuppressUntil,
          now,
        });
        return false;
      }
      const ignoreUntil = Number(state.searchIgnoreUntil || 0) || 0;
      if (now < ignoreUntil) return false;
      if (state.searchLock) return false;

      state.searchLock = true;
      state.searchIgnoreUntil = now + 900;

      let handled = false;
      try {
        if (this.canUseDeckyKeyboardBridge()) {
          let bridgeHandled = false;
          try {
            bridgeHandled = await this.editSearchQuery();
          } catch (_error) {
            bridgeHandled = false;
          }
          this.debugLog(`ui:search_${trigger}_bridge`, {
            handled: bridgeHandled,
            mode: this.uiMode,
            page: this.pageKind,
          });
          if (bridgeHandled) {
            handled = true;
            return true;
          }
        }
        const shown = this.requestKeyboardForElement("search-input");
        this.debugLog(`ui:search_${trigger}_native`, {
          shown,
          mode: this.uiMode,
          page: this.pageKind,
          keyboard: this._lastKeyboardAttempt || null,
        });
        if (shown) {
          window.setTimeout(() => {
            try {
              if (document.activeElement && document.activeElement.id === "search-input") {
                this.showSteamVirtualKeyboard();
              }
            } catch (_error) {
              // ignore
            }
          }, 180);
          window.setTimeout(() => {
            try {
              if (document.activeElement && document.activeElement.id === "search-input") {
                this.showSteamVirtualKeyboard();
              }
            } catch (_error) {
              // ignore
            }
          }, 420);
        }
        handled = shown;
        return shown;
      } finally {
        state.searchLock = false;
        state.searchIgnoreUntil = Date.now() + (handled ? 1000 : 450);
      }
    },
    onTextInputClick(kind) {
      const key = String(kind || "").trim().toLowerCase();
      if (!key) return;
      if (key === "search") {
        this.queueSearchKeyboardOpen("click");
        return;
      }
      if (key === "custom_share") {
        void (async () => {
          const ok = await this.editCustomShareUrl();
          if (!ok) this.requestKeyboardForElement("custom-share-input");
        })();
        return;
      }
      if (key === "custom_pwd") {
        void (async () => {
          const ok = await this.editCustomPassword();
          if (!ok) this.requestKeyboardForElement("custom-pwd-input");
        })();
      }
    },
    onTextInputFocus(kind) {
      const key = String(kind || "").trim().toLowerCase();
      if (key !== "search") return;
      this.queueSearchKeyboardOpen("focus");
    },
    onDocumentFocusIn(event) {
      const target = event && event.target;
      if (!target || target.id !== "search-input") return;
      this.queueSearchKeyboardOpen("focusin");
    },
    async editSearchQuery() {
      if (!this.canUseDeckyKeyboardBridge()) return false;
      const input = document.getElementById("search-input");
      const placeholder = input ? String(input.getAttribute("placeholder") || "").trim() : "";
      const current = String(this.query || "");
      const result = await this.requestDeckyKeyboard({
        field: "search",
        title: "搜索游戏",
        placeholder: placeholder || "搜索游戏（留空浏览全部）",
        value: current,
      });
      if (!result || !result.ok) {
        const reason = String((result && result.reason) || "").trim().toLowerCase();
        if (reason === "cancel") {
          this.$nextTick(() => this.focusElementByIndex(0, { scroll: false }));
          return true;
        }
        return false;
      }
      this.query = String(result.value || "");
      await this.reloadList(1);
      return true;
    },
    async editCustomShareUrl() {
      if (!this.canUseDeckyKeyboardBridge()) return false;
      const input = document.getElementById("custom-share-input");
      const placeholder = input ? String(input.getAttribute("placeholder") || "").trim() : "";
      const current = String((this.customImport && this.customImport.share_url) || "");
      const result = await this.requestDeckyKeyboard({
        field: "custom_share",
        title: "分享链接",
        placeholder: placeholder || "请输入分享链接",
        value: current,
      });
      if (!result || !result.ok) {
        const reason = String((result && result.reason) || "").trim().toLowerCase();
        if (reason === "cancel") {
          this.$nextTick(() => this.focusElementByIndex(1, { scroll: false }));
          return true;
        }
        return false;
      }
      if (!this.customImport || typeof this.customImport !== "object") this.customImport = {};
      this.customImport.share_url = String(result.value || "");
      this.$nextTick(() => this.focusElementByIndex(1, { scroll: false }));
      return true;
    },
    async editCustomPassword() {
      if (!this.canUseDeckyKeyboardBridge()) return false;
      const input = document.getElementById("custom-pwd-input");
      const placeholder = input ? String(input.getAttribute("placeholder") || "").trim() : "";
      const current = String((this.customImport && this.customImport.pwd) || "");
      const result = await this.requestDeckyKeyboard({
        field: "custom_pwd",
        title: "访问码/密码",
        placeholder: placeholder || "访问码/密码（可选）",
        value: current,
        password: false,
      });
      if (!result || !result.ok) {
        const reason = String((result && result.reason) || "").trim().toLowerCase();
        if (reason === "cancel") {
          this.$nextTick(() => this.focusElementByIndex(2, { scroll: false }));
          return true;
        }
        return false;
      }
      if (!this.customImport || typeof this.customImport !== "object") this.customImport = {};
      this.customImport.pwd = String(result.value || "");
      this.$nextTick(() => this.focusElementByIndex(2, { scroll: false }));
      return true;
    },
    applyInitialViewFromQuery() {
      try {
        const pathname = String(window.location.pathname || "");
        const isEmulator = pathname.endsWith("/emulator.html");
        if (isEmulator) {
          this.pageKind = "emulator_switch";
        } else {
          this.pageKind = "library";
        }
        const params = new URLSearchParams(window.location.search || "");
        const requestedMode = String(params.get("mode") || "")
          .trim()
          .toLowerCase();
        let resolvedMode = requestedMode === "search" || requestedMode === "full" ? requestedMode : "";
        if (!resolvedMode) {
          // 默认行为：
          // - Decky 菜单内嵌 iframe：展示完整列表（full，无广告）
          // - 外部打开（顶层窗口）：展示搜索页（search，带广告）
          let inIframe = false;
          try {
            inIframe = window.self !== window.top;
          } catch (_error) {
            inIframe = true;
          }
          resolvedMode = inIframe ? "full" : "search";
        }
        if (isEmulator) {
          resolvedMode = "full";
        }
        this.uiMode = resolvedMode === "search" ? "search" : "full";
        this.showResults = this.uiMode === "full";
        const view = String(params.get("view") || "")
          .trim()
          .toLowerCase();
        if (!isEmulator && view === "custom") {
          this.switchView("custom");
        }
      } catch (_error) {
        // 忽略解析失败。
      }
    },
    switchView(nextView) {
      const target = String(nextView || "").trim().toLowerCase() === "custom" ? "custom" : "library";
      if (this.installDialog.visible) {
        this.closeInstallDialog(true);
      }
      this.view = target;
      this.categoryMenuOpen = false;
      this.sortMenuOpen = false;
      if (target === "custom") {
        this.customImport.hintText = this.customImport.hintText || "请输入分享链接后点击解析";
        this.setFocusIndex(1, { scroll: false });
      } else {
        this.setFocusIndex(0, { scroll: false });
        if (!this.catalog.loaded && !this.catalog.loading) {
          void this.loadAllCatalog();
        }
        const trimmed = String(this.query || "").trim();
        this.showResults = this.uiMode === "full" ? true : Boolean(trimmed);
        if (this.showResults) {
          this.applyCatalogBrowse({ resetLimit: false, focusAfter: false });
        } else {
          this.list = Object.assign({}, this.list, { total: 0, page: 1, items: [] });
          this.hintText = "";
        }
      }
    },
    openEmulatorPage() {
      if (this.isEmulatorPage) {
        try {
          window.history.back();
          window.setTimeout(() => {
            try {
              if (String(window.location.pathname || "").endsWith("/emulator.html")) {
                window.location.href = "/tianyi/library?mode=full";
              }
            } catch (_error) {
              window.location.href = "/tianyi/library?mode=full";
            }
          }, 250);
          return;
        } catch (_error) {
          // ignore
        }
        window.location.href = "/tianyi/library?mode=full";
        return;
      }
      window.location.href = "/tianyi/library/emulator.html?mode=full";
    },
    switchEmulatorPlatform(platform) {
      const target = "emulator_switch";
      if (String(this.pageKind || "") === target) return;
      if (this.installDialog && this.installDialog.visible) {
        this.closeInstallDialog(true);
      }
      this.pageKind = target;
      this.query = "";
      this.activeQuery = "";
      this.filters.category = "";
      this.filters.sort = "default";
      this.displayLimit = 96;
      this.categoryMenuOpen = false;
      this.sortMenuOpen = false;
      this.installPlanPrefetchStarted = false;
      this.installPlanFocusLastToken = "";

      try {
        const params = new URLSearchParams(window.location.search || "");
        params.delete("emu");
        const qs = params.toString();
        const nextUrl = `${String(window.location.pathname || "")}${qs ? `?${qs}` : ""}`;
        window.history.replaceState({}, "", nextUrl);
      } catch (_error) {
        // ignore
      }

      try {
        document.title = "Freedeck 模拟器 - Switch";
      } catch (_error) {
        // ignore
      }
      void this.checkSwitchEmulatorStatus();
      void this.loadAllCatalog();
      this.setFocusIndex(0, { scroll: false });
    },
    closeEmulatorNotice(options = {}) {
      if (!this.emulatorNotice || typeof this.emulatorNotice !== "object") return;
      this.emulatorNotice.visible = false;
      const dismiss = Boolean(options && options.dismiss);
      if (!dismiss) return;
      try {
        window.localStorage.setItem(SWITCH_EMULATOR_NOTICE_KEY, "1");
      } catch (_error) {
        // ignore
      }
    },
    async checkSwitchEmulatorStatus() {
      if (!this.isSwitchEmulatorPage) return;
      try {
        const dismissed = window.localStorage.getItem(SWITCH_EMULATOR_NOTICE_KEY);
        if (dismissed === "1" || dismissed === "true") return;
      } catch (_error) {
        // ignore
      }
      try {
        const result = await apiGet("/api/tianyi/emulator/switch/status");
        if (!result || result.status !== "success") return;
        const data = result.data && typeof result.data === "object" ? result.data : {};
        if (Boolean(data.installed)) return;
        const message = String(data.message || "").trim();
        this.emulatorNotice = {
          visible: true,
          title: "未安装 Switch 模拟器",
          message:
            message ||
            "检测到未安装 Switch 模拟器。请到 Freedeck 设置 → 模拟器 → 下载 Switch 模拟器，完成后再添加 Switch 游戏到 Steam。",
        };
      } catch (_error) {
        // ignore
      }
    },
    async onEmulatorItemClick(item) {
      if (!item) return;
      await this.openInstallConfirm(item);
    },
    isCatalogMenuOpen() {
      return Boolean(this.categoryMenuOpen || this.sortMenuOpen);
    },
    onQueryInput() {
      if (this.view !== "library") return;
      const trimmed = String(this.query || "").trim();
      if (this.uiMode === "search") {
        this.showResults = Boolean(trimmed);
        if (!trimmed) {
          this.activeQuery = "";
          this.list = Object.assign({}, this.list, { total: 0, page: 1, items: [] });
          this.hintText = "";
          if (this.queryDebounceTimer) {
            window.clearTimeout(this.queryDebounceTimer);
            this.queryDebounceTimer = null;
          }
          return;
        }
      } else {
        this.showResults = true;
      }
      if (this.queryDebounceTimer) {
        window.clearTimeout(this.queryDebounceTimer);
        this.queryDebounceTimer = null;
      }
      this.queryDebounceTimer = window.setTimeout(() => {
        this.queryDebounceTimer = null;
        this.activeQuery = String(this.query || "").trim();
        this.applyCatalogBrowse({ resetLimit: true, focusAfter: false });
      }, 260);
    },
    scheduleCatalogBrowseApply(options = {}) {
      const immediate = Boolean(options && options.immediate);
      if (immediate) {
        if (this.catalogApplyTimer) {
          window.clearTimeout(this.catalogApplyTimer);
          this.catalogApplyTimer = null;
        }
        this.applyCatalogBrowse({ resetLimit: false, focusAfter: false });
        return;
      }
      if (this.catalogApplyTimer) return;
      this.catalogApplyTimer = window.setTimeout(() => {
        this.catalogApplyTimer = null;
        this.applyCatalogBrowse({ resetLimit: false, focusAfter: false });
      }, 180);
    },
    async loadAllCatalog() {
      if (this.catalog.loading) {
        this.catalogReloadQueued = true;
        return;
      }
      this.catalog.loading = true;
      this.catalog.loaded = false;
      this.catalog.error = "";
      this.catalog.items = [];
      this.catalog.total = 0;

      const pageSize = 200;
      let page = 1;
      const isSwitchEmulator = this.isSwitchEmulatorPage;
      const isEmulator = this.isEmulatorPage;
      const switchCategoryId = "527";
      const primaryApiPath = isSwitchEmulator
        ? "/api/tianyi/emulator/switch/catalog"
        : "/api/tianyi/catalog";
      const fallbackApiPath = "/api/tianyi/catalog";
      let apiPath = primaryApiPath;
      let fallbackUsed = false;
      try {
        while (true) {
          const result = await apiGet(apiPath, { q: "", page, page_size: pageSize });
          if (!result || result.status !== "success") {
            if (isSwitchEmulator && !fallbackUsed) {
              fallbackUsed = true;
              apiPath = fallbackApiPath;
              page = 1;
              this.catalog.items = [];
              this.catalog.total = 0;
              continue;
            }
            throw new Error((result && result.message) || "游戏列表加载失败");
          }
          const data = result.data && typeof result.data === "object" ? result.data : {};
          const total = Number(data.total || 0);
          if (Number.isFinite(total) && total > 0 && (!this.catalog.total || this.catalog.total < total)) {
            this.catalog.total = total;
          }
          const rawItems = Array.isArray(data.items) ? data.items : [];
          if (!rawItems.length) break;

          let items = rawItems;
          if (isSwitchEmulator) {
            if (fallbackUsed || apiPath === fallbackApiPath) {
              items = rawItems.filter((item) => String((item && item.category_parent) || "").trim() === switchCategoryId);
            }
          } else if (!isEmulator) {
            items = rawItems.filter((item) => String((item && item.category_parent) || "").trim() !== switchCategoryId);
          }

          if (items.length > 0) {
            this.catalog.items.push(...items);
          }
          if (this.catalog.total > 0 && this.catalog.items.length >= this.catalog.total) {
            this.catalog.loaded = true;
            break;
          }
          if (rawItems.length < pageSize) break;
          page += 1;
          this.scheduleCatalogBrowseApply();
          await new Promise((resolve) => window.setTimeout(resolve, 0));
        }
        if (!this.catalog.loaded) {
          this.catalog.loaded = Boolean(this.catalog.items.length);
        }
        this.catalog.total = this.catalog.items.length;
      } catch (error) {
        this.catalog.error = String(error);
      } finally {
        this.catalog.loading = false;
        this.scheduleCatalogBrowseApply({ immediate: true });
        if (this.catalogReloadQueued) {
          this.catalogReloadQueued = false;
          void this.loadAllCatalog();
        }
      }
    },
    toggleCategoryMenu() {
      if (this.view !== "library") return;
      if (this.catalog.loading && !this.catalog.items.length) return;
      if (this.sortMenuOpen) this.sortMenuOpen = false;
      const nextOpen = !this.categoryMenuOpen;
      this.categoryMenuOpen = nextOpen;
      if (nextOpen) {
        const current = String((this.filters && this.filters.category) || "");
        const options = Array.isArray(this.categoryOptions) ? this.categoryOptions : [];
        let idx = options.findIndex((opt) => String(opt && opt.value) === current);
        if (idx < 0) idx = 0;
        this.setFocusIndex(4 + idx, { scroll: true });
      } else {
        this.setFocusIndex(1, { scroll: false });
      }
    },
    toggleSortMenu() {
      if (this.view !== "library") return;
      if (this.categoryMenuOpen) this.categoryMenuOpen = false;
      const nextOpen = !this.sortMenuOpen;
      this.sortMenuOpen = nextOpen;
      if (nextOpen) {
        const current = String((this.filters && this.filters.sort) || "default") || "default";
        const options = Array.isArray(this.sortOptions) ? this.sortOptions : [];
        let idx = options.findIndex((opt) => String(opt && opt.value) === current);
        if (idx < 0) idx = 0;
        this.setFocusIndex(4 + idx, { scroll: true });
      } else {
        this.setFocusIndex(2, { scroll: false });
      }
    },
    chooseCategory(idx) {
      const options = Array.isArray(this.categoryOptions) ? this.categoryOptions : [];
      const index = Number(idx || 0);
      if (!Number.isFinite(index) || index < 0 || index >= options.length) return;
      const selected = options[index] || {};
      this.filters.category = String(selected.value || "");
      this.categoryMenuOpen = false;
      this.activeQuery = String(this.query || "").trim();
      this.displayLimit = 96;
      this.applyCatalogBrowse({ resetLimit: true, focusAfter: false });
      this.setFocusIndex(1, { scroll: false });
    },
    chooseSort(idx) {
      const options = Array.isArray(this.sortOptions) ? this.sortOptions : [];
      const index = Number(idx || 0);
      if (!Number.isFinite(index) || index < 0 || index >= options.length) return;
      const selected = options[index] || {};
      this.filters.sort = String(selected.value || "default") || "default";
      this.sortMenuOpen = false;
      this.activeQuery = String(this.query || "").trim();
      this.displayLimit = 96;
      this.applyCatalogBrowse({ resetLimit: true, focusAfter: false });
      this.setFocusIndex(2, { scroll: false });
    },
    loadMoreBrowse() {
      const groups = Array.isArray(this.catalogGroups) ? this.catalogGroups : [];
      if (!groups.length) return;
      const step = Number(this.displayStep || 72);
      const prevVisible = Array.isArray(this.list && this.list.items) ? this.list.items.length : 0;
      const nextLimit = Math.max(0, Number(this.displayLimit || 0) + (Number.isFinite(step) && step > 0 ? step : 72));
      this.displayLimit = Math.min(groups.length, nextLimit);
      const nextItems = groups.slice(0, Math.max(0, this.displayLimit));
      this.list.items = nextItems;
      this.list.total = groups.length;
      this.showResults = true;
      if (nextItems.length > prevVisible) {
        void this.prefetchCovers(nextItems.slice(prevVisible));
      }
    },
    resetBrowse() {
      this.query = "";
      this.activeQuery = "";
      this.filters.category = "";
      this.filters.sort = "default";
      this.displayLimit = 96;
      this.applyCatalogBrowse({ resetLimit: true, focusAfter: true });
    },
    sortCatalogGroups(groups) {
      const sort = String((this.filters && this.filters.sort) || "default").trim() || "default";
      const list = Array.isArray(groups) ? [...groups] : [];
      if (sort === "title_asc") {
        list.sort((a, b) => String((a && a.title) || "").localeCompare(String((b && b.title) || ""), "zh-Hans-CN", {
          sensitivity: "base",
          numeric: true,
        }));
        return list;
      }
      if (sort === "size_desc") {
        list.sort((a, b) => Number((b && b.size_bytes) || 0) - Number((a && a.size_bytes) || 0));
        return list;
      }
      if (sort === "size_asc") {
        list.sort((a, b) => Number((a && a.size_bytes) || 0) - Number((b && b.size_bytes) || 0));
        return list;
      }
      return list;
    },
    applyCatalogBrowse(options = {}) {
      if (this.view !== "library") return;
      if (this.isCatalogMenuOpen()) return;

      const resetLimit = Boolean(options && options.resetLimit);
      const focusAfter = options && "focusAfter" in options ? Boolean(options.focusAfter) : true;

      const items = Array.isArray(this.catalog && this.catalog.items) ? this.catalog.items : [];
      const qText = String((this.activeQuery || this.query) || "").trim();
      const qLower = qText.toLowerCase();
      const category = String((this.filters && this.filters.category) || "").trim();

      if (this.uiMode === "search" && !this.showResults && !qText && !category) {
        this.list = Object.assign({}, this.list, { total: 0, page: 1, items: [] });
        this.hintText = "";
        return;
      }

      let filtered = items;
      if (category) {
        filtered = filtered.filter((item) => String((item && item.categories) || "").trim() === category);
      }
      if (qLower) {
        filtered = filtered.filter((item) => {
          if (!item || typeof item !== "object") return false;
          const title = String(item.title || "").toLowerCase();
          const cats = String(item.categories || "").toLowerCase();
          const gid = String(item.game_id || "").toLowerCase();
          return title.includes(qLower) || cats.includes(qLower) || gid.includes(qLower);
        });
      }

      const groupsRaw = this.groupCatalogItems(filtered);
      const groups = this.sortCatalogGroups(groupsRaw);
      this.catalogGroups = groups;

      if (resetLimit) {
        this.displayLimit = 96;
      }
      const limit = Math.max(0, Number(this.displayLimit || 0) || 96);
      const visible = groups.slice(0, limit);

      this.list = Object.assign({}, this.list, {
        total: groups.length,
        page: 1,
        page_size: Math.max(50, Number(this.settings && this.settings.page_size) || 50),
        items: visible,
      });

      const parts = [];
      if (category) parts.push(category);
      if (qText) parts.push(`“${qText}”`);
      const base = parts.length ? `已筛选 ${parts.join(" ")}，共 ${groups.length} 个结果` : `共 ${groups.length} 个游戏`;
      const loadingHint = this.catalog && this.catalog.loading ? `（同步中 ${items.length}/${this.catalog.total || "..."}）` : "";
      this.hintText = `${base}${loadingHint}`;

      if (this.showResults) {
        void this.prefetchCovers(visible);
        this.maybeStartInstallPlanPrefetchSeed();
      }

      if (focusAfter) {
        if (visible.length > 0) {
          const cardStart = this.cardFocusStart;
          this.setFocusIndex(cardStart, { scroll: true });
        } else {
          this.setFocusIndex(0, { scroll: false });
        }
      }
    },
    isCustomFileSelected(file) {
      if (!file || typeof file !== "object") return false;
      const fileId = String(file.file_id || "").trim();
      if (!fileId) return false;
      const selected = this.customImport.selected && typeof this.customImport.selected === "object" ? this.customImport.selected : {};
      return Boolean(selected[fileId]);
    },
    toggleCustomFile(idx) {
      const plan = this.customImport.plan;
      const files = plan && Array.isArray(plan.files) ? plan.files : [];
      const index = Number(idx || 0);
      if (!Number.isFinite(index) || index < 0 || index >= files.length) return;
      const file = files[index];
      const fileId = String((file && file.file_id) || "").trim();
      if (!fileId) return;
      if (!this.customImport.selected || typeof this.customImport.selected !== "object") {
        this.customImport.selected = {};
      }
      this.customImport.selected[fileId] = !this.customImport.selected[fileId];
    },
    clearCustomSelection() {
      this.customImport.selected = {};
      this.flash("已清空选择");
    },
    normalizeCustomShareUrl(rawUrl, rawPwd) {
      const shareUrlRaw = String(rawUrl || "").trim();
      let pwdRaw = String(rawPwd || "").trim();
      if (!shareUrlRaw) return "";

      let urlText = shareUrlRaw;
      if (!pwdRaw) {
        const pwdMatch = shareUrlRaw.match(/(?:提取码|访问码|密码|口令)\s*[:：]?\s*([A-Za-z0-9]{4,16})/i);
        pwdRaw = pwdMatch ? String(pwdMatch[1] || "").trim() : "";
      }

      const cleanTrail = (value) => {
        let out = String(value || "").trim();
        while (true) {
          const next = out.replace(/[)\]】>＞,，。;；!！?？]+$/g, "").trim();
          if (next === out) break;
          out = next;
        }
        return out;
      };

      const httpMatch = shareUrlRaw.match(/https?:\/\/[^\s]+/i);
      if (httpMatch && httpMatch[0]) {
        urlText = cleanTrail(httpMatch[0]);
      } else {
        const tianyiMatch = shareUrlRaw.match(/^(?:www\.|m\.|h5\.)?cloud\.189\.cn\/[^\s]+/i);
        if (tianyiMatch && tianyiMatch[0]) {
          urlText = cleanTrail(`https://${tianyiMatch[0]}`);
        } else {
          const baiduMatch = shareUrlRaw.match(/^(?:www\.|m\.)?pan\.baidu\.com\/[^\s]+/i);
          if (baiduMatch && baiduMatch[0]) {
            urlText = cleanTrail(`https://${baiduMatch[0]}`);
          } else {
            const ctfileMatch = shareUrlRaw.match(/^(?:www\.)?ctfile\.com\/[^\s]+/i);
            if (ctfileMatch && ctfileMatch[0]) {
              urlText = cleanTrail(`https://${ctfileMatch[0]}`);
            }
          }
        }
      }

      const idOnly = /^[A-Za-z0-9]+(?:-[A-Za-z0-9]+){1,4}$/.test(urlText) && !/[\/?#&\s]/.test(urlText);
      const ctfileIdLike = idOnly && /^\d{4,}-/.test(urlText);
      const tianyiCodeOnly = /^[A-Za-z0-9_-]{4,}$/.test(urlText) && !/[\/?#&\s]/.test(urlText) && !urlText.includes("-");

      if (ctfileIdLike) {
        urlText = `https://www.ctfile.com/f/${urlText}`;
      } else if (tianyiCodeOnly) {
        urlText = `https://cloud.189.cn/t/${urlText}`;
      } else if (!urlText.includes("://") && /^(?:www\.|m\.|h5\.)?cloud\.189\.cn\//.test(urlText)) {
        urlText = `https://${urlText}`;
      } else if (!urlText.includes("://") && /^(?:www\.|m\.)?pan\.baidu\.com\//.test(urlText)) {
        urlText = `https://${urlText}`;
      } else if (!urlText.includes("://") && /^(?:www\.)?ctfile\.com\//.test(urlText)) {
        urlText = `https://${urlText}`;
      }

      if (!pwdRaw) return urlText;

      try {
        const url = new URL(urlText);
        url.searchParams.set("pwd", pwdRaw);
        return url.toString();
      } catch (_error) {
        const joiner = urlText.includes("?") ? "&" : "?";
        return `${urlText}${joiner}pwd=${encodeURIComponent(pwdRaw)}`;
      }
    },
    async parseCustomImport() {
      if (this.customImport.loading || this.customImport.submitting) return;
      const shareUrl = this.normalizeCustomShareUrl(this.customImport.share_url, this.customImport.pwd);
      if (!shareUrl) {
        this.flash("请先输入分享链接（天翼云盘 / 百度网盘 / CTFile）");
        this.setFocusIndex(1, { scroll: false });
        return;
      }

      this.customImport.loading = true;
      this.customImport.plan = null;
      this.customImport.selected = {};
      this.customImport.hintText = "正在解析分享内容...";
      this.customImport.share_url = shareUrl;
      this.setFocusIndex(3, { scroll: false });

      try {
        const result = await apiPost("/api/tianyi/install/prepare", {
          game_id: "",
          share_url: shareUrl,
          download_dir: this.settings.download_dir,
          install_dir: this.settings.install_dir,
        });
        if (result.status !== "success") {
          const errorText = this.buildPrepareInstallError(result);
          this.customImport.hintText = errorText;
          this.flash(errorText);
          return;
        }
        const plan = result.data || null;
        this.customImport.plan = plan;
        if (plan && plan.install_dir) {
          this.settings.install_dir = String(plan.install_dir || this.settings.install_dir || "");
        }
        const fileCount = plan && Array.isArray(plan.files) ? plan.files.length : 0;
        const title = plan && plan.game_title ? String(plan.game_title || "").trim() : "";
        this.customImport.hintText = `${title || "解析成功"}，共 ${fileCount} 个文件，请选择要下载的文件`;
        this.setFocusIndex(fileCount > 0 ? 4 : 1, { scroll: false });
      } catch (error) {
        const text = `解析失败：${String(error)}`;
        this.customImport.hintText = text;
        this.flash(text);
      } finally {
        this.customImport.loading = false;
      }
    },
    async startCustomDownload() {
      if (this.customImport.submitting) return;
      if (!this.customImport.plan) {
        this.flash("请先解析分享链接");
        return;
      }
      const fileIds = this.customSelectedFileIds;
      if (!fileIds.length) {
        this.flash("请先选择要下载的文件");
        return;
      }
      const plan = this.customImport.plan;
      // 说明：优先使用用户输入的分享链接（包含可选的 token 参数），避免后端返回 canonical_url 后丢失 token。
      const shareUrlBase = this.customImport.share_url || plan.share_url || "";
      const shareUrl = this.normalizeCustomShareUrl(shareUrlBase, this.customImport.pwd);
      const gameId = String(plan.game_id || "").trim();
      if (!shareUrl) {
        this.flash("分享链接无效，请重新解析");
        return;
      }

      this.customImport.submitting = true;
      try {
        const result = await apiPost("/api/tianyi/install/start", {
          game_id: gameId,
          share_url: shareUrl,
          file_ids: fileIds,
          split_count: this.settings.split_count,
          download_dir: this.settings.download_dir,
          install_dir: this.settings.install_dir,
        });
        if (result.status !== "success") {
          this.flash(result.message || "创建下载任务失败");
          return;
        }
        const baseText = "下载任务已创建，正在下载";
        const notice =
          result && result.data && result.data.plan && result.data.plan.provider_notice
            ? String(result.data.plan.provider_notice || "").trim()
            : "";
        this.flash(notice ? `${baseText}\n${notice}` : baseText);
      } catch (error) {
        this.flash(String(error));
      } finally {
        this.customImport.submitting = false;
      }
    },
    onVisibilityChange() {
      this.syncGamepadPolling();
    },
    onWindowFocus() {
      this.syncGamepadPolling();
    },
    onWindowBlur() {
      this.syncGamepadPolling();
    },
    syncGamepadPolling() {
      const shouldRun = !document.hidden && document.hasFocus();
      if (!shouldRun) {
        this.stopGamepadPolling();
        return;
      }
      this.startGamepadPolling();
    },
    flash(text) {
      this.message = String(text || "");
      if (!this.message) return;
      setTimeout(() => {
        this.message = "";
      }, 2400);
    },
    formatShareAttempt(item) {
      if (!item || typeof item !== "object") return "";
      const profile = String(item.profile || item.step || "").trim();
      const method = String(item.method || "").trim().toUpperCase();
      const host = String(item.host || "").trim();
      const endpoint = String(item.endpoint || "").trim().replace("/api/open/share/", "");
      const status = Number(item.status || 0);
      const reason = String(item.message || "").trim();
      const tags = [profile, method, host].filter(Boolean).join(" ");
      const endpointPart = endpoint ? ` ${endpoint}` : "";
      const statusPart = Number.isFinite(status) && status > 0 ? ` status=${status}` : "";
      const reasonPart = reason ? ` ${reason}` : "";
      return `${tags}${endpointPart}${statusPart}${reasonPart}`.trim();
    },
    summarizeShareAttempts(diagnostics) {
      const attempts = Array.isArray(diagnostics && diagnostics.attempts) ? diagnostics.attempts : [];
      if (!attempts.length) return "";
      const failed = attempts.filter((item) => item && item.ok === false);
      const head = failed[0] || attempts[0] || {};
      const tail = failed[failed.length - 1] || attempts[attempts.length - 1] || {};
      const headText = this.formatShareAttempt(head);
      const tailText = this.formatShareAttempt(tail);
      const failedCount = failed.length;
      let text = `已尝试 ${attempts.length} 条链路，失败 ${failedCount} 条`;
      if (headText) text += `；首个关键尝试：${headText}`;
      if (tailText && tailText !== headText) text += `；最后关键尝试：${tailText}`;
      return text;
    },
    buildPrepareInstallError(result) {
      const base = String((result && result.message) || "安装准备失败").trim();
      const diagnostics = (result && result.diagnostics) || {};
      const attempts = Array.isArray(diagnostics.attempts) ? diagnostics.attempts : [];
      if (!attempts.length) return base;
      const headline = base.includes("shareId") ? base : `${base}（未获取shareId）`;
      const summary = this.summarizeShareAttempts(diagnostics);
      return summary ? `${headline}。${summary}` : headline;
    },
    normalizePath(path) {
      return String(path || "")
        .trim()
        .replace(/\\/g, "/")
        .replace(/\/+$/, "")
        .toLowerCase();
    },
    toNum(value) {
      const num = Number(value || 0);
      return Number.isFinite(num) ? Math.max(0, num) : 0;
    },
    samePathOrNest(pathA, pathB) {
      if (!pathA || !pathB) return false;
      if (pathA === pathB) return true;
      return pathA.startsWith(`${pathB}/`) || pathB.startsWith(`${pathA}/`);
    },
    isSameStorage(plan) {
      if (!plan || typeof plan !== "object") return false;
      const downloadDir = this.normalizePath(plan.download_dir);
      const installDir = this.normalizePath(plan.install_dir);
      if (this.samePathOrNest(downloadDir, installDir)) return true;

      const freeDownload = this.toNum(plan.free_download_bytes);
      const freeInstall = this.toNum(plan.free_install_bytes);
      if (freeDownload > 0 && freeInstall > 0) {
        const diff = Math.abs(freeDownload - freeInstall);
        if (diff <= 64 * 1024 * 1024) return true;
      }
      return false;
    },
    totalRequiredBytes(plan) {
      return this.toNum(plan && plan.required_download_bytes) + this.toNum(plan && plan.required_install_bytes);
    },
    totalRequiredFormula(plan) {
      const packText = this.formatBytes(this.toNum(plan && plan.required_download_bytes));
      const gameText = this.formatBytes(this.toNum(plan && plan.required_install_bytes));
      return `压缩包（${packText}）+游戏本体（${gameText}）`;
    },
    totalFreeBytes(plan) {
      if (!plan || typeof plan !== "object") return 0;
      const freeDownload = this.toNum(plan.free_download_bytes);
      const freeInstall = this.toNum(plan.free_install_bytes);
      if (freeDownload > 0 && freeInstall > 0) {
        return Math.min(freeDownload, freeInstall);
      }
      return Math.max(freeDownload, freeInstall);
    },
    combinedSpaceOk(plan) {
      const need = this.totalRequiredBytes(plan);
      const free = this.totalFreeBytes(plan);
      if (need <= 0) return Boolean(plan && plan.can_install);
      return free >= need;
    },
    storageEnough(plan) {
      if (!plan || typeof plan !== "object") return false;
      if (this.isSameStorage(plan)) return this.combinedSpaceOk(plan);
      return this.downloadSpaceOk(plan) && this.installSpaceOk(plan);
    },
    freeSpaceLabel(plan) {
      if (!plan || typeof plan !== "object") return "-";
      if (this.isSameStorage(plan)) {
        return this.formatBytes(this.totalFreeBytes(plan));
      }
      const freeDownload = this.formatBytes(this.toNum(plan.free_download_bytes));
      const freeInstall = this.formatBytes(this.toNum(plan.free_install_bytes));
      return `下载盘 ${freeDownload} / 安装盘 ${freeInstall}`;
    },
    storageChipLabel(plan) {
      return this.storageEnough(plan) ? "充足" : "不足";
    },
    storageChipClass(plan) {
      return this.storageEnough(plan) ? "chip-good" : "chip-bad";
    },
    protonTierFor(item) {
      const key = this.getCoverCacheKey(item);
      if (!key) return "";
      const meta = this.coverMetaCache[key];
      if (!meta || typeof meta !== "object") return "";
      return String(meta.proton_tier || "").trim();
    },
    protonTierLabel(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (!tier) return "未知";
      if (["platinum", "gold", "native"].includes(tier)) return "绿标";
      if (["silver", "bronze"].includes(tier)) return "黄标";
      if (["borked", "unsupported"].includes(tier)) return "红标";
      return tier.toUpperCase();
    },
    protonTierClass(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (["platinum", "gold", "native"].includes(tier)) return "tier-green";
      if (["silver", "bronze"].includes(tier)) return "tier-yellow";
      if (["borked", "unsupported"].includes(tier)) return "tier-red";
      return "tier-neutral";
    },
    protonBadgeClass(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (["platinum", "gold", "native"].includes(tier)) return "tier-good";
      if (["silver", "bronze"].includes(tier)) return "tier-mid";
      if (["borked", "unsupported"].includes(tier)) return "tier-bad";
      return "tier-unknown";
    },
    protonBadgeMark(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (["platinum", "gold", "native"].includes(tier)) return "✓";
      if (["silver", "bronze"].includes(tier)) return "!";
      if (["borked", "unsupported"].includes(tier)) return "×";
      return "?";
    },
    downloadSpaceOk(plan) {
      if (!plan || typeof plan !== "object") return false;
      const free = this.toNum(plan.free_download_bytes);
      const need = this.toNum(plan.required_download_bytes);
      if (free > 0 || need > 0) return free >= need;
      return Boolean(plan.download_dir_ok);
    },
    installSpaceOk(plan) {
      if (!plan || typeof plan !== "object") return false;
      const free = this.toNum(plan.free_install_bytes);
      const need = this.toNum(plan.required_install_bytes);
      if (free > 0 || need > 0) return free >= need;
      return Boolean(plan.install_dir_ok);
    },
    async bootstrap() {
      try {
        const result = await apiGet("/api/tianyi/state");
        if (result.status !== "success") return;
        const data = result.data || {};
        this.settings = Object.assign({}, this.settings, data.settings || {});
        if (!this.settings.page_size) this.settings.page_size = 50;
        if (!this.settings.split_count) this.settings.split_count = 16;
        if (!this.settings.install_dir) this.settings.install_dir = this.settings.download_dir || "";
        this.maybeStartInstallPlanPrefetchSeed();
      } catch (_error) {
        // 状态读取失败不阻断搜索流程。
      }
    },
    getFocusableCount() {
      if (this.installDialog.visible) {
        const variantCount = this.installVariantCount > 1 ? this.installVariantCount : 0;
        if (variantCount <= 0) return 2;
        const menuOpen = Boolean(this.installDialog && this.installDialog.versionMenuOpen);
        return 2 + 1 + (menuOpen ? variantCount : 0);
      }
      if (this.view === "custom") {
        const base = 4; // back + share + pwd + parse
        if (!this.customImport.plan) return base;
        const fileCount = this.customFileCount;
        return base + fileCount + 2; // files + download + clear
      }
      const headerCount = this.uiMode === "full" ? (this.isEmulatorPage ? 2 : 4) : 3; // search + back / search + filters + emulator
      if (this.uiMode === "full" && this.isCatalogMenuOpen()) {
        const opts = this.categoryMenuOpen ? this.categoryOptions : this.sortMenuOpen ? this.sortOptions : [];
        const menuCount = Array.isArray(opts) ? opts.length : 0;
        return headerCount + Math.max(0, menuCount);
      }
      const cardCount = Number(this.list.items.length || 0);
      const extra = this.canLoadMore ? 1 : 0;
      return headerCount + Math.max(0, cardCount) + extra;
    },
    getCardColumns() {
      if (this.view !== "library") return 1;
      const cards = Array.from(document.querySelectorAll(".game-card"));
      if (cards.length <= 1) return 1;
      const firstTop = cards[0].getBoundingClientRect().top;
      let columns = 0;
      for (const card of cards) {
        const top = card.getBoundingClientRect().top;
        if (Math.abs(top - firstTop) <= 6) {
          columns += 1;
        }
      }
      return Math.max(1, columns || 1);
    },
    focusElementByIndex(index, options = {}) {
      const scroll = options.scroll !== false;
      let el = null;
      if (this.installDialog.visible) {
        const variantCount = this.installVariantCount > 1 ? this.installVariantCount : 0;
        const menuOpen = Boolean(this.installDialog && this.installDialog.versionMenuOpen);
        if (index <= 0) {
          el = document.getElementById("install-cancel");
        } else if (index === 1) {
          el = document.getElementById("install-confirm");
        } else if (variantCount > 0 && index === 2) {
          el = document.getElementById("install-version-select");
        } else if (menuOpen && variantCount > 0 && index >= 3 && index < 3 + variantCount) {
          el = document.querySelector(`.md3-menu-item[data-version-index="${index - 3}"]`);
        }
      } else if (this.view === "custom") {
        const hasPlan = Boolean(this.customImport.plan);
        const fileCount = hasPlan ? this.customFileCount : 0;
        const fileStart = 4;
        const downloadIndex = hasPlan ? fileStart + fileCount : -1;
        const clearIndex = hasPlan ? fileStart + fileCount + 1 : -1;
        if (index === 0) {
          el = document.getElementById("custom-back");
        } else if (index === 1) {
          el = document.getElementById("custom-share-input");
        } else if (index === 2) {
          el = document.getElementById("custom-pwd-input");
        } else if (index === 3) {
          el = document.getElementById("custom-parse-button");
        } else if (index >= fileStart && index < fileStart + fileCount) {
          el = document.querySelector(`.custom-file-row[data-file-index="${index - fileStart}"]`);
        } else if (index === downloadIndex) {
          el = document.getElementById("custom-download-button");
        } else if (index === clearIndex) {
          el = document.getElementById("custom-clear-button");
        }
      } else if (index === 0) {
        el = document.getElementById("search-input");
      } else if (this.uiMode !== "full") {
        if (index === 1) {
          el = document.getElementById("custom-source-button");
        } else if (index === 2) {
          el = document.getElementById("emulator-button");
        } else if (index >= 3) {
          const cardStart = 3;
          const cardIndex = index - cardStart;
          const cardCount = Number(this.list.items.length || 0);
          const loadMoreIndex = cardStart + Math.max(0, cardCount);
          if (this.canLoadMore && index === loadMoreIndex) {
            el = document.getElementById("load-more-button");
          } else {
            el = document.querySelector(`.game-card[data-card-index="${cardIndex}"]`);
          }
        }
      } else if (this.uiMode === "full") {
        if (this.isEmulatorPage) {
          if (index === 1) {
            el = document.getElementById("emulator-button");
          }
        } else if (index === 1) {
          el = document.getElementById("catalog-category-select");
        } else if (index === 2) {
          el = document.getElementById("catalog-sort-select");
        } else if (index === 3) {
          el = document.getElementById("emulator-button");
        }
        if (!this.isEmulatorPage && this.isCatalogMenuOpen() && index >= 4) {
          const optIndex = index - 4;
          if (this.categoryMenuOpen) {
            el = document.querySelector(`.md3-menu-item[data-category-index="${optIndex}"]`);
          } else if (this.sortMenuOpen) {
            el = document.querySelector(`.md3-menu-item[data-sort-index="${optIndex}"]`);
          }
        } else if (index >= (this.isEmulatorPage ? 2 : 4)) {
          const cardStart = this.isEmulatorPage ? 2 : 4;
          const cardIndex = index - cardStart;
          const cardCount = Number(this.list.items.length || 0);
          const loadMoreIndex = cardStart + Math.max(0, cardCount);
          if (this.canLoadMore && index === loadMoreIndex) {
            el = document.getElementById("load-more-button");
          } else {
            el = document.querySelector(`.game-card[data-card-index="${cardIndex}"]`);
          }
        }
      }
      if (!el || typeof el.focus !== "function") return;
      try {
        el.focus({ preventScroll: true });
      } catch (_error) {
        el.focus();
      }
      if (scroll && typeof el.scrollIntoView === "function") {
        const cardStart = this.cardFocusStart;
        if (!this.installDialog.visible && this.view === "library" && index >= cardStart) {
          el.scrollIntoView({ block: "nearest", inline: "nearest", behavior: "smooth" });
        } else if (this.installDialog.visible && index >= 2) {
          el.scrollIntoView({ block: "nearest", inline: "nearest", behavior: "smooth" });
        }
      }
    },
    setFocusIndex(index, options = {}) {
      const total = this.getFocusableCount();
      if (total <= 0) return;
      const next = Math.max(0, Math.min(total - 1, Number(index || 0)));
      this.focusedIndex = next;
      this.$nextTick(() => this.focusElementByIndex(next, options));
      this.scheduleFocusedInstallPlanPrefetch(next);
    },
    moveFocus(direction) {
      if (this.installDialog.visible) {
        const total = this.getFocusableCount();
        if (total <= 0) return;
        let next = this.focusedIndex;
        if (["left", "up"].includes(direction)) next = Math.max(0, next - 1);
        if (["right", "down"].includes(direction)) next = Math.min(total - 1, next + 1);
        this.setFocusIndex(next, { scroll: true });
        return;
      }

      if (this.view === "custom") {
        const total = this.getFocusableCount();
        if (total <= 0) return;
        let next = this.focusedIndex;
        if (["left", "up"].includes(direction)) next = Math.max(0, next - 1);
        if (["right", "down"].includes(direction)) next = Math.min(total - 1, next + 1);
        this.setFocusIndex(next);
        return;
      }

      if (this.isCatalogMenuOpen()) {
        const total = this.getFocusableCount();
        if (total <= 0) return;
        let next = this.focusedIndex;
        if (["left", "up"].includes(direction)) next = Math.max(0, next - 1);
        if (["right", "down"].includes(direction)) next = Math.min(total - 1, next + 1);
        this.setFocusIndex(next, { scroll: true });
        return;
      }

      const total = this.getFocusableCount();
      if (total <= 0) return;

      const columns = this.getCardColumns();
      let next = this.focusedIndex;

      if (this.uiMode === "full") {
        const cardStart = 4;
        const hasCards = total > cardStart;
        if (direction === "left") {
          if (next > cardStart) {
            next -= 1;
          } else if (next === cardStart) {
            next = 3;
          } else if (next === 3) {
            next = 2;
          } else if (next === 2) {
            next = 1;
          } else if (next === 1) {
            next = 0;
          }
        } else if (direction === "right") {
          if (next === 0) {
            next = 1;
          } else if (next === 1) {
            next = 2;
          } else if (next === 2) {
            next = 3;
          } else if (next === 3) {
            if (hasCards) next = cardStart;
          } else if (next >= cardStart && next < total - 1) {
            next += 1;
          }
        } else if (direction === "up") {
          if (next >= cardStart) {
            const up = next - columns;
            next = up >= cardStart ? up : 0;
          } else if (next === 3) {
            next = 2;
          } else if (next === 2) {
            next = 1;
          } else if (next === 1) {
            next = 0;
          }
        } else if (direction === "down") {
          if (next === 0) {
            next = 1;
          } else if (next === 1) {
            next = 2;
          } else if (next === 2) {
            next = 3;
          } else if (next === 3 && hasCards) {
            next = cardStart;
          } else if (next >= cardStart) {
            next = Math.min(total - 1, next + columns);
          }
        }
      } else {
        const cardStart = 3;
        const hasCards = total > cardStart;
        if (direction === "left") {
          if (next > cardStart) next -= 1;
          else if (next === cardStart) next = 2;
          else if (next === 2) next = 1;
          else if (next === 1) next = 0;
        } else if (direction === "right") {
          if (next === 0) {
            next = 1;
          } else if (next === 1) {
            next = 2;
          } else if (next === 2) {
            if (hasCards) next = cardStart;
          } else if (next >= cardStart && next < total - 1) {
            next += 1;
          }
        } else if (direction === "up") {
          if (next >= cardStart) {
            const up = next - columns;
            next = up >= cardStart ? up : 0;
          } else if (next === 2) {
            next = 1;
          } else if (next === 1) {
            next = 0;
          }
        } else if (direction === "down") {
          if (next === 0) {
            if (hasCards) next = cardStart;
          } else if (next === 1) {
            if (hasCards) next = cardStart;
          } else if (next === 2) {
            if (hasCards) next = cardStart;
          } else if (next >= cardStart) {
            next = Math.min(total - 1, next + columns);
          }
        }
      }

      this.setFocusIndex(next);
    },
    async activateFocused() {
      if (this.installDialog.visible) {
        if (this.focusedIndex <= 0) {
          this.closeInstallDialog();
          return;
        }
        if (this.focusedIndex === 1) {
          await this.confirmInstall();
          return;
        }
        const variantCount = this.installVariantCount > 1 ? this.installVariantCount : 0;
        if (variantCount <= 0) return;
        if (this.focusedIndex === 2) {
          this.toggleInstallVersionMenu();
          return;
        }
        await this.chooseInstallVersion(this.focusedIndex - 3);
        return;
      }
      const index = this.focusedIndex;
      if (this.view === "custom") {
        const fileStart = 4;
        const hasPlan = Boolean(this.customImport.plan);
        const fileCount = hasPlan ? this.customFileCount : 0;
        const downloadIndex = hasPlan ? fileStart + fileCount : -1;
        const clearIndex = hasPlan ? fileStart + fileCount + 1 : -1;
        if (index === 0) {
          this.switchView("library");
          return;
        }
        if (index === 1) {
          if (this.canUseDeckyKeyboardBridge()) {
            const ok = await this.editCustomShareUrl();
            if (ok) return;
          }
          this.requestKeyboardForElement("custom-share-input");
          return;
        }
        if (index === 2) {
          if (this.canUseDeckyKeyboardBridge()) {
            const ok = await this.editCustomPassword();
            if (ok) return;
          }
          this.requestKeyboardForElement("custom-pwd-input");
          return;
        }
        if (index === 3) {
          await this.parseCustomImport();
          return;
        }
        if (index >= fileStart && index < fileStart + fileCount) {
          this.toggleCustomFile(index - fileStart);
          return;
        }
        if (index === downloadIndex) {
          await this.startCustomDownload();
          return;
        }
        if (index === clearIndex) {
          this.clearCustomSelection();
          return;
        }
        return;
      }

      if (this.uiMode === "full") {
        if (this.isEmulatorPage) {
          if (index === 0) {
            await this.openSearchKeyboard("select");
            return;
          }
          if (index === 1) {
            this.openEmulatorPage();
            return;
          }

          const cardStart = 2;
          const cardCount = Number(this.list.items.length || 0);
          const loadMoreIndex = cardStart + Math.max(0, cardCount);
          if (this.canLoadMore && index === loadMoreIndex) {
            this.loadMoreBrowse();
            this.setFocusIndex(index, { scroll: true });
            return;
          }

          const cardIndex = index - cardStart;
          const item = this.list.items[cardIndex];
          if (item) await this.openInstallConfirm(item);
          return;
        }

        if (this.isCatalogMenuOpen()) {
          if (this.categoryMenuOpen) {
            if (index === 1) {
              this.toggleCategoryMenu();
              return;
            }
            if (index >= 4) {
              this.chooseCategory(index - 4);
            }
            return;
          }
          if (this.sortMenuOpen) {
            if (index === 2) {
              this.toggleSortMenu();
              return;
            }
            if (index >= 4) {
              this.chooseSort(index - 4);
            }
            return;
          }
        }

        if (index === 0) {
          await this.openSearchKeyboard("select");
          return;
        }
        if (index === 1) {
          this.toggleCategoryMenu();
          return;
        }
        if (index === 2) {
          this.toggleSortMenu();
          return;
        }
        if (index === 3) {
          this.openEmulatorPage();
          return;
        }

        const cardStart = 4;
        const cardCount = Number(this.list.items.length || 0);
        const loadMoreIndex = cardStart + Math.max(0, cardCount);
        if (this.canLoadMore && index === loadMoreIndex) {
          this.loadMoreBrowse();
          this.setFocusIndex(index, { scroll: true });
          return;
        }

        const cardIndex = index - cardStart;
        const item = this.list.items[cardIndex];
        if (item) await this.openInstallConfirm(item);
        return;
      }

      if (index === 0) {
        await this.openSearchKeyboard("select");
        return;
      }
      if (this.uiMode !== "full" && index === 1) {
        this.switchView("custom");
        return;
      }
      if (this.uiMode !== "full" && index === 2) {
        this.openEmulatorPage();
        return;
      }
      const cardStart = 3;
      const cardCount = Number(this.list.items.length || 0);
      const loadMoreIndex = cardStart + Math.max(0, cardCount);
      if (this.canLoadMore && index === loadMoreIndex) {
        this.loadMoreBrowse();
        this.setFocusIndex(index, { scroll: true });
        return;
      }

      const cardIndex = index - cardStart;
      const item = this.list.items[cardIndex];
      if (item) await this.openInstallConfirm(item);
    },
    handleBackAction() {
      if (this.installDialog.visible) {
        if (this.installDialog && this.installDialog.versionMenuOpen) {
          this.installDialog.versionMenuOpen = false;
          this.setFocusIndex(2, { scroll: false });
          return;
        }
        this.closeInstallDialog();
        return;
      }
      if (this.view === "custom") {
        this.switchView("library");
        return;
      }
      if (this.isCatalogMenuOpen()) {
        if (this.categoryMenuOpen) {
          this.categoryMenuOpen = false;
          this.setFocusIndex(1, { scroll: false });
          return;
        }
        if (this.sortMenuOpen) {
          this.sortMenuOpen = false;
          this.setFocusIndex(2, { scroll: false });
          return;
        }
      }
      if (this.focusedIndex !== 0) {
        this.setFocusIndex(0, { scroll: false });
        return;
      }
      const q = String((this.activeQuery || this.query) || "").trim();
      const category = String((this.filters && this.filters.category) || "").trim();
      const sort = String((this.filters && this.filters.sort) || "default").trim() || "default";
      if (q || category || sort !== "default") {
        this.resetBrowse();
        this.setFocusIndex(0, { scroll: false });
        return;
      }
      this.setFocusIndex(0, { scroll: false });
    },
    onGlobalKeyDown(event) {
      const key = String(event.key || "");
      if (["ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "Enter", "Escape"].includes(key)) {
        event.preventDefault();
      }

      if (key === "ArrowUp") this.moveFocus("up");
      if (key === "ArrowDown") this.moveFocus("down");
      if (key === "ArrowLeft") this.moveFocus("left");
      if (key === "ArrowRight") this.moveFocus("right");
      if (key === "Enter") this.activateFocused();
      if (key === "Escape") this.handleBackAction();
    },
    pressEdge(name, pressed, callback) {
      const wasPressed = Boolean(this.gamepadPressed[name]);
      if (pressed && !wasPressed) {
        callback();
      }
      this.gamepadPressed[name] = Boolean(pressed);
    },
    repeatDirectional(name, pressed, callback, nowTs) {
      const repeatDelay = 260;
      const repeatInterval = 110;
      const state = this.gamepadRepeatState[name] || { active: false, nextAt: 0 };
      if (!pressed) {
        state.active = false;
        state.nextAt = 0;
        this.gamepadRepeatState[name] = state;
        return;
      }
      if (!state.active) {
        callback();
        state.active = true;
        state.nextAt = nowTs + repeatDelay;
        this.gamepadRepeatState[name] = state;
        return;
      }
      if (nowTs >= state.nextAt) {
        callback();
        state.nextAt = nowTs + repeatInterval;
      }
      this.gamepadRepeatState[name] = state;
    },
    readPrimaryGamepad() {
      if (!navigator.getGamepads) return null;
      const pads = navigator.getGamepads();
      if (!pads) return null;
      for (const pad of pads) {
        if (pad) return pad;
      }
      return null;
    },
    pollGamepad() {
      const pad = this.readPrimaryGamepad();
      if (!pad) return;

      const buttons = pad.buttons || [];
      const axes = pad.axes || [];
      const axisX = Number(axes[0] || 0);
      const axisY = Number(axes[1] || 0);
      const deadzone = 0.56;
      const nowTs = Date.now();

      const up = Boolean(buttons[12] && buttons[12].pressed) || axisY <= -deadzone;
      const down = Boolean(buttons[13] && buttons[13].pressed) || axisY >= deadzone;
      const left = Boolean(buttons[14] && buttons[14].pressed) || axisX <= -deadzone;
      const right = Boolean(buttons[15] && buttons[15].pressed) || axisX >= deadzone;
      const a = Boolean(buttons[0] && buttons[0].pressed);
      const b = Boolean(buttons[1] && buttons[1].pressed);

      this.repeatDirectional("up", up, () => this.moveFocus("up"), nowTs);
      this.repeatDirectional("down", down, () => this.moveFocus("down"), nowTs);
      this.repeatDirectional("left", left, () => this.moveFocus("left"), nowTs);
      this.repeatDirectional("right", right, () => this.moveFocus("right"), nowTs);
      this.pressEdge("a", a, () => this.activateFocused());
      this.pressEdge("b", b, () => this.handleBackAction());
    },
    startGamepadPolling() {
      if (this.gamepadTimer) return;
      if (document.hidden || !document.hasFocus()) return;
      this.gamepadTimer = window.setInterval(() => this.pollGamepad(), this.gamepadPollMs);
    },
    stopGamepadPolling() {
      if (!this.gamepadTimer) return;
      window.clearInterval(this.gamepadTimer);
      this.gamepadTimer = null;
      this.gamepadPressed = {};
      this.gamepadRepeatState = {};
    },
    onGamepadConnected() {
      this.startGamepadPolling();
    },
    onGamepadDisconnected() {
      const pad = this.readPrimaryGamepad();
      if (!pad) {
        this.gamepadPressed = {};
        this.gamepadRepeatState = {};
      }
    },
    async reloadList(_page = 1) {
      const trimmed = String(this.query || "").trim();
      this.activeQuery = trimmed;
      this.showResults = this.uiMode === "full" ? true : Boolean(trimmed);
      this.categoryMenuOpen = false;
      this.sortMenuOpen = false;
      this.displayLimit = 96;
      if (this.uiMode === "search" && !this.showResults) {
        this.list = Object.assign({}, this.list, { total: 0, page: 1, items: [] });
        this.hintText = "";
        if (!this.catalog.loaded && !this.catalog.loading) {
          void this.loadAllCatalog();
        }
        return;
      }
      if (!this.catalog.loaded && !this.catalog.loading) {
        this.hintText = "正在加载游戏列表...";
        void this.loadAllCatalog();
        return;
      }
      this.applyCatalogBrowse({ resetLimit: true, focusAfter: true });
    },
    groupCatalogItems(items) {
      const rawItems = Array.isArray(items) ? items.filter(Boolean) : [];
      if (!rawItems.length) return [];

      const groups = [];
      const groupMap = new Map();

      const pushToGroup = (groupKey, entry) => {
        if (!groupMap.has(groupKey)) {
          const group = {
            group_key: groupKey,
            variants: [],
            variant_count: 0,
          };
          groupMap.set(groupKey, group);
          groups.push(group);
        }
        const group = groupMap.get(groupKey);
        if (!group) return;
        group.variants.push(entry);
      };

      for (const entry of rawItems) {
        const key = this.catalogGroupKey(entry);
        pushToGroup(key, entry);
      }

      for (const group of groups) {
        const variants = Array.isArray(group.variants) ? group.variants : [];
        const deduped = [];
        const seen = new Set();
        for (const item of variants) {
          if (!item || typeof item !== "object") continue;
          const gameId = String(item.game_id || "").trim();
          const downUrl = String(item.down_url || "").trim();
          const token = `${gameId}||${downUrl}`;
          if (seen.has(token)) continue;
          seen.add(token);
          deduped.push(item);
        }

        deduped.sort((a, b) => {
          const aw = this.variantSortWeight(a && a.title);
          const bw = this.variantSortWeight(b && b.title);
          if (aw !== bw) return aw - bw;
          const at = String((a && a.title) || "");
          const bt = String((b && b.title) || "");
          if (at.length !== bt.length) return at.length - bt.length;
          const as = Number((a && a.size_bytes) || 0);
          const bs = Number((b && b.size_bytes) || 0);
          return bs - as;
        });

        const representative = deduped[0] || variants[0] || {};
        Object.assign(group, representative);
        group.variants = deduped;
        group.variant_count = deduped.length;
      }

      return groups;
    },
    catalogGroupKey(item) {
      if (!item || typeof item !== "object") return `unknown:${Math.random()}`;
      const appId = Number(item.app_id || item.appId || item.steam_appid || 0);
      if (Number.isFinite(appId) && appId > 0) return `appid:${Math.floor(appId)}`;

      const title = String(item.title || "").trim();
      const normalized = this.normalizeGroupTitleKey(title);
      if (normalized) return `title:${normalized}`;
      const fallback = String(item.game_id || title || "").trim();
      return fallback ? `misc:${fallback}` : `unknown:${Math.random()}`;
    },
    normalizeGroupTitleKey(rawTitle) {
      const raw = String(rawTitle || "").replace(/\s+/g, " ").trim();
      if (!raw) return "";
      const parts = raw
        .split(/[\/|｜]/)
        .map((part) => part.trim())
        .filter(Boolean);
      const englishParts = parts.filter((part) => /[A-Za-z]/.test(part));
      const english = englishParts.length > 0 ? englishParts[englishParts.length - 1] : "";
      if (english) {
        let key = english
          .replace(/[\u2010-\u2015]/g, "-")
          .replace(/[^A-Za-z0-9]+/g, " ")
          .trim()
          .toLowerCase();
        key = key.replace(/\b(digital\s+deluxe|deluxe|gold|ultimate|complete|definitive|collector'?s|premium)\s+edition\b/g, "");
        key = key.replace(/\s+edition\b/g, "");
        key = key.replace(/\s+/g, " ").trim();
        return key;
      }

      const cn = parts[0] || raw;
      let key = cn
        .replace(/\s+/g, " ")
        .replace(/(?:\s*[（(【\[].*?[）)】\]])+$/g, "")
        .trim();
      key = key.replace(/(数字豪华版|豪华版|黄金版|终极版|完整版|完全版|决定版|年度版|传奇版|典藏版|加强版)\s*$/g, "").trim();
      return key ? key.toLowerCase() : "";
    },
    variantSortWeight(titleRaw) {
      const title = String(titleRaw || "");
      const lower = title.toLowerCase();
      let weight = 0;
      if (/(豪华版|数字豪华版|黄金版|终极版|完整版|完全版|决定版|年度版|传奇版|典藏版|加强版)/.test(title)) weight += 10;
      if (/(deluxe|digital deluxe|gold|ultimate|complete|definitive|collector|premium)\s+edition/.test(lower)) weight += 10;
      if (/\b(beta|demo|test|playtest)\b/.test(lower)) weight += 30;
      return weight;
    },
    cardSizeLabel(item) {
      const variants = item && Array.isArray(item.variants) ? item.variants : [];
      const count = variants.length;
      if (count > 1) return `${count} 个版本`;
      return String((item && item.size_text) || "-") || "-";
    },
    getCoverCacheKey(item) {
      return String((item && item.game_id) || (item && item.title) || "").trim();
    },
    isCoverMissActive(cacheKey) {
      const key = String(cacheKey || "").trim();
      if (!key) return false;
      const store = this.coverMissCache && typeof this.coverMissCache === "object" ? this.coverMissCache : {};
      const raw = store[key];
      if (!raw) return false;
      const now = Date.now();
      if (raw === true) {
        this.coverMissCache[key] = now;
        return true;
      }
      const ts = Number(raw || 0);
      if (!Number.isFinite(ts) || ts <= 0) {
        this.coverMissCache[key] = now;
        return true;
      }
      if (now - ts > COVER_MISS_CACHE_TTL_MS) {
        delete this.coverMissCache[key];
        return false;
      }
      return true;
    },
    extractBuiltInCover(item) {
      const candidateKeys = ["cover_url", "cover", "image_url", "image", "pic_url", "pic", "thumbnail", "poster"];
      for (const key of candidateKeys) {
        const value = String((item && item[key]) || "").trim();
        if (!value) continue;
        if (value.startsWith("http://") || value.startsWith("https://") || value.startsWith("/")) {
          return value;
        }
      }
      return "";
    },
    buildSteamCoverCandidates(appIdRaw) {
      const appId = Number(appIdRaw || 0);
      if (!Number.isFinite(appId) || appId <= 0) return [];
      const id = Math.floor(appId);
      return [
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${id}/library_600x900_2x.jpg`,
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${id}/library_600x900.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${id}/library_600x900_2x.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${id}/library_600x900.jpg`,
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${id}/capsule_616x353.jpg`,
      ];
    },
    _coverFetchLimiterState() {
      if (!this._coverFetchLimiter || typeof this._coverFetchLimiter !== "object") {
        this._coverFetchLimiter = { active: 0, queue: [] };
      }
      return this._coverFetchLimiter;
    },
    async acquireCoverFetchSlot() {
      const limiter = this._coverFetchLimiterState();
      const maxConcurrent = this.isEmulatorPage ? 2 : 4;
      if (limiter.active < maxConcurrent) {
        limiter.active += 1;
        return;
      }
      await new Promise((resolve) => {
        if (!Array.isArray(limiter.queue)) limiter.queue = [];
        limiter.queue.push(resolve);
      });
      // Slot is transferred from release() to this waiter; do not increment active here.
    },
    releaseCoverFetchSlot() {
      const limiter = this._coverFetchLimiterState();
      const queue = Array.isArray(limiter.queue) ? limiter.queue : [];
      const next = queue.shift();
      limiter.queue = queue;
      if (typeof next === "function") {
        next();
        return;
      }
      limiter.active = Math.max(0, Number(limiter.active || 0) - 1);
    },
    collectCoverCandidates(item) {
      const candidates = [];
      const seen = new Set();
      const push = (value) => {
        const url = String(value || "").trim();
        if (!url) return;
        if (
          !url.startsWith("http://")
          && !url.startsWith("https://")
          && !url.startsWith("/")
          && !url.startsWith("data:image/")
        ) {
          return;
        }
        if (seen.has(url)) return;
        seen.add(url);
        candidates.push(url);
      };

      const cacheKey = this.getCoverCacheKey(item);
      const meta = (cacheKey && this.coverMetaCache[cacheKey] && typeof this.coverMetaCache[cacheKey] === "object")
        ? this.coverMetaCache[cacheKey]
        : null;
      const appId = Number((meta && meta.app_id) || (item && item.app_id) || 0);

      push(this.extractBuiltInCover(item));
      push(meta && meta.square_cover_url);
      this.buildSteamCoverCandidates(appId).forEach((url) => push(url));
      push(meta && meta.cover_url);
      push(cacheKey && this.coverCache[cacheKey]);

      return candidates;
    },
    async prefetchCovers(items) {
      const queue = Array.isArray(items) ? items.filter(Boolean) : [];
      if (!queue.length) return;
      const workerCount = Math.max(1, Math.min(4, queue.length));
      let cursor = 0;
      const worker = async () => {
        while (cursor < queue.length) {
          const current = queue[cursor];
          cursor += 1;
          await this.fetchCoverForItem(current);
        }
      };
      const workers = Array.from({ length: workerCount }, () => worker());
      await Promise.all(workers);
    },
    async fetchCoverForItem(item, options = {}) {
      const cacheKey = this.getCoverCacheKey(item);
      if (!cacheKey) return;
      if (this.isGbaEmulatorPage) return;
      const force = Boolean(options && options.force);
      if (this.coverPending[cacheKey]) return;
      if (!force && this.isCoverMissActive(cacheKey)) return;
      if (force && this.coverMissCache[cacheKey]) delete this.coverMissCache[cacheKey];

      const builtInCover = this.extractBuiltInCover(item);
      if (builtInCover) {
        this.coverCache[cacheKey] = builtInCover;
        return;
      }

      this.coverPending[cacheKey] = true;
      try {
        await this.acquireCoverFetchSlot();
        const result = await apiGet("/api/tianyi/catalog/cover", {
          game_id: String((item && item.game_id) || ""),
          title: String((item && item.title) || ""),
          categories: String((item && item.categories) || ""),
        });
        const data = (result && result.data && typeof result.data === "object") ? result.data : {};
        const coverUrl = String(data.cover_url || "").trim();
        const squareCoverUrl = String(data.square_cover_url || "").trim();
        const protonTier = String(data.protondb_tier || "").trim();
        const appId = Number(data.app_id || 0);
        this.coverMetaCache[cacheKey] = {
          app_id: Number.isFinite(appId) ? appId : 0,
          proton_tier: protonTier,
          cover_url: coverUrl,
          square_cover_url: squareCoverUrl,
        };

        if (result && result.status === "success") {
          const preferred = this.collectCoverCandidates(item)[0] || "";
          if (preferred) {
            this.coverCache[cacheKey] = preferred;
          }
          if (preferred || coverUrl || squareCoverUrl || protonTier || appId > 0) {
            return;
          }
        }
        this.coverMissCache[cacheKey] = Date.now();
      } catch (_error) {
        this.coverMissCache[cacheKey] = Date.now();
      } finally {
        this.releaseCoverFetchSlot();
        delete this.coverPending[cacheKey];
      }
    },
    coverFor(item) {
      const cacheKey = this.getCoverCacheKey(item);
      const candidates = this.collectCoverCandidates(item);
      if (candidates.length > 0) {
        const first = candidates[0];
        if (cacheKey) {
          this.coverCache[cacheKey] = first;
          if (!this.coverPending[cacheKey] && !this.isCoverMissActive(cacheKey)) {
            const meta = this.coverMetaCache[cacheKey];
            if (!meta || !meta.square_cover_url) {
              void this.fetchCoverForItem(item);
            }
          }
        }
        return first;
      }

      const placeholder = this.buildPlaceholderCover(item);
      if (cacheKey) {
        this.coverCache[cacheKey] = placeholder;
        if (!this.coverPending[cacheKey] && !this.isCoverMissActive(cacheKey)) {
          void this.fetchCoverForItem(item);
        }
      }
      return placeholder;
    },
    onCoverError(event, item) {
      if (!event || !event.target) return;
      const img = event.target;
      const attemptedRaw = String(img.dataset.coverAttempts || "").trim();
      const attempted = attemptedRaw ? attemptedRaw.split("||").filter(Boolean) : [];
      const current = String(img.getAttribute("src") || "").trim();
      if (current && !attempted.includes(current)) attempted.push(current);

      const candidates = this.collectCoverCandidates(item);
      const next = candidates.find((url) => !attempted.includes(url));
      if (next) {
        img.dataset.coverAttempts = [...attempted, next].join("||");
        img.src = next;
        return;
      }

      const cacheKey = this.getCoverCacheKey(item);
      if (cacheKey) this.coverMissCache[cacheKey] = Date.now();
      img.src = this.buildPlaceholderCover(item);
    },
    buildPlaceholderCover(item) {
      const title = String((item && item.title) || "Freedeck").trim();
      const category = String((item && item.categories) || "游戏").trim();
      const isSwitch = String((item && item.category_parent) || "").trim() === "527" || Boolean(this.isSwitchEmulatorPage);
      const initials = title
        .replace(/\s+/g, " ")
        .split(/[\/\s\-_:]+/)
        .filter(Boolean)
        .slice(0, 2)
        .map((part) => part.slice(0, 1).toUpperCase())
        .join("");
      const main = (initials || "G").slice(0, 2);
      const escapedTitle = title.replace(/[&<>"]/g, "");
      const escapedCategory = category.replace(/[&<>"]/g, "");
      const bgStart = isSwitch ? "#e60012" : "#222222";
      const bgEnd = isSwitch ? "#8a000b" : "#0f0f0f";
      const cornerFill = isSwitch ? "#ff2b3a" : "#303030";
      const svg = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 900">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="${bgStart}" />
      <stop offset="100%" stop-color="${bgEnd}" />
    </linearGradient>
  </defs>
  <rect width="600" height="900" fill="url(#bg)" />
  <circle cx="520" cy="130" r="160" fill="${cornerFill}" opacity="0.32" />
  <circle cx="140" cy="820" r="210" fill="#2a2a2a" opacity="0.35" />
  <text x="300" y="420" text-anchor="middle" font-size="138" fill="#f0f0f0" font-family="Roboto, Noto Sans SC, Microsoft YaHei" font-weight="500">${main}</text>
  <text x="44" y="770" font-size="30" fill="#d8d8d8" font-family="Roboto, Noto Sans SC, Microsoft YaHei">${escapedCategory}</text>
  <text x="44" y="820" font-size="22" fill="#bdbdbd" font-family="Roboto, Noto Sans SC, Microsoft YaHei">${escapedTitle.slice(0, 28)}</text>
</svg>`;
      return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
    },
    getInstallPlanCacheKey(variant) {
      const target = variant && typeof variant === "object" ? variant : null;
      if (!target) return "";
      const gameId = String(target.game_id || "").trim();
      const downUrl = String(target.down_url || "").trim();
      if (!gameId && !downUrl) return "";
      const downloadDir = String((this.settings && this.settings.download_dir) || "").trim();
      const installDir = String((this.settings && this.settings.install_dir) || "").trim();
      return `${gameId}||${downUrl}||${downloadDir}||${installDir}`;
    },
    readInstallPlanCache(cacheKey) {
      const key = String(cacheKey || "").trim();
      if (!key) return null;
      const store = this.installPlanCache && typeof this.installPlanCache === "object" ? this.installPlanCache : {};
      const entry = store[key];
      if (!entry || typeof entry !== "object") return null;
      const cachedAt = Number(entry.cachedAt || 0);
      if (!Number.isFinite(cachedAt) || cachedAt <= 0) return null;
      if (Date.now() - cachedAt > INSTALL_PLAN_CACHE_TTL_MS) return null;
      const plan = entry.plan;
      return plan && typeof plan === "object" ? plan : null;
    },
    writeInstallPlanCache(cacheKey, plan) {
      const key = String(cacheKey || "").trim();
      if (!key) return;
      if (!plan || typeof plan !== "object") return;
      if (!this.installPlanCache || typeof this.installPlanCache !== "object") {
        this.installPlanCache = {};
      }
      this.installPlanCache[key] = { plan, cachedAt: Date.now() };
    },
    scheduleFocusedInstallPlanPrefetch(focusIndex) {
      if (this.installDialog && this.installDialog.visible) return;
      if (this.view !== "library") return;
      if (typeof this.isCatalogMenuOpen === "function" && this.isCatalogMenuOpen()) return;
      if (this.isGbaEmulatorPage) return;
      const downloadDir = String((this.settings && this.settings.download_dir) || "").trim();
      if (!downloadDir) return;

      const cardStart = this.cardFocusStart;
      const idx = Number(focusIndex || 0);
      if (!Number.isFinite(idx) || idx < cardStart) return;

      const items = Array.isArray(this.list && this.list.items) ? this.list.items : [];
      const cardIndex = idx - cardStart;
      if (cardIndex < 0 || cardIndex >= items.length) return;

      const current = items[cardIndex];
      if (!current || typeof current !== "object") return;
      const gid = String(current.game_id || "").trim();
      const downUrl = String(current.down_url || "").trim();
      if (!downUrl) return;
      const token = `${gid}||${downUrl}`;
      if (!token.trim()) return;
      if (token === String(this.installPlanFocusLastToken || "")) return;
      this.installPlanFocusLastToken = token;

      if (this.installPlanFocusTimer) {
        window.clearTimeout(this.installPlanFocusTimer);
        this.installPlanFocusTimer = null;
      }

      this.installPlanFocusTimer = window.setTimeout(() => {
        this.installPlanFocusTimer = null;
        const batch = [];
        for (let offset = 0; offset <= INSTALL_PLAN_FOCUS_PREFETCH_LOOKAHEAD; offset += 1) {
          const entry = items[cardIndex + offset];
          if (entry && typeof entry === "object") batch.push(entry);
        }
        if (batch.length) {
          void this.prefetchInstallPlans(batch);
        }
      }, INSTALL_PLAN_FOCUS_PREFETCH_DELAY_MS);
    },
    maybeStartInstallPlanPrefetchSeed() {
      if (this.installPlanPrefetchStarted) return;
      if (this.isGbaEmulatorPage) return;
      const downloadDir = String((this.settings && this.settings.download_dir) || "").trim();
      if (!downloadDir) return;
      const items = Array.isArray(this.list && this.list.items) ? this.list.items.filter(Boolean) : [];
      if (!items.length) return;
      this.installPlanPrefetchStarted = true;
      const maxSeed = this.isSwitchEmulatorPage ? Math.max(1, Number(this.emulatorPageSize || 12)) : INSTALL_PLAN_PREFETCH_SEED_COUNT;
      const seedItems = items.slice(0, Math.min(items.length, Math.max(1, maxSeed)));
      window.setTimeout(() => {
        void this.prefetchInstallPlans(seedItems);
      }, 320);
    },
    async prefetchInstallPlans(items) {
      const queue = Array.isArray(items) ? items.filter(Boolean) : [];
      if (!queue.length) return;
      const workerCount = Math.max(1, Math.min(INSTALL_PLAN_PREFETCH_CONCURRENCY, queue.length));
      let cursor = 0;
      const worker = async () => {
        while (cursor < queue.length) {
          const current = queue[cursor];
          cursor += 1;
          await this.prefetchInstallPlanForItem(current);
        }
      };
      const workers = Array.from({ length: workerCount }, () => worker());
      await Promise.all(workers);
    },
    async prefetchInstallPlanForItem(item) {
      const entry = item && typeof item === "object" ? item : null;
      if (!entry) return;

      const rawVariants = entry && Array.isArray(entry.variants) ? entry.variants : [entry];
      const candidates = [];
      const seen = new Set();
      for (const variant of rawVariants) {
        if (!variant || typeof variant !== "object") continue;
        const gameId = String(variant.game_id || "").trim();
        if (!gameId) continue;
        const downUrl = String(variant.down_url || "").trim();
        if (!downUrl) continue;
        const token = `${gameId}||${downUrl}`;
        if (seen.has(token)) continue;
        seen.add(token);
        candidates.push(variant);
      }
      if (!candidates.length) return;
      candidates.sort((a, b) => {
        const av = this.extractVariantVersionToken(a);
        const bv = this.extractVariantVersionToken(b);
        const vcmp = this.compareVersionTokensDesc(av, bv);
        if (vcmp !== 0) return vcmp;
        const aw = this.variantSortWeight(a && a.title);
        const bw = this.variantSortWeight(b && b.title);
        if (aw !== bw) return aw - bw;
        const at = String((a && a.title) || "");
        const bt = String((b && b.title) || "");
        if (at.length !== bt.length) return at.length - bt.length;
        const as = Number((a && a.size_bytes) || 0);
        const bs = Number((b && b.size_bytes) || 0);
        return bs - as;
      });

      const target = candidates[0];
      const cacheKey = this.getInstallPlanCacheKey(target);
      if (!cacheKey) return;
      if (this.readInstallPlanCache(cacheKey)) return;
      if (!this.installPlanPending || typeof this.installPlanPending !== "object") {
        this.installPlanPending = {};
      }
      const existingPending = this.installPlanPending[cacheKey];
      if (existingPending && typeof existingPending.then === "function") return;
      if (existingPending === true) return;
      const pendingPromise = apiPost("/api/tianyi/install/prepare", {
        game_id: String(target.game_id || ""),
        share_url: String(target.down_url || ""),
        download_dir: this.settings.download_dir,
        install_dir: this.settings.install_dir,
      });
      this.installPlanPending[cacheKey] = pendingPromise;
      try {
        const result = await pendingPromise;
        if (result && result.status === "success" && result.data && typeof result.data === "object") {
          this.writeInstallPlanCache(cacheKey, result.data);
        }
      } catch (_error) {
        // ignore
      } finally {
        if (this.installPlanPending[cacheKey] === pendingPromise) {
          delete this.installPlanPending[cacheKey];
        }
      }
    },
    currentInstallVariant() {
      const variants = this.installDialog && Array.isArray(this.installDialog.variants) ? this.installDialog.variants : [];
      const index = Number(this.installDialog && this.installDialog.selectedVariantIndex) || 0;
      if (!variants.length) return null;
      const safeIndex = Math.max(0, Math.min(variants.length - 1, index));
      return variants[safeIndex] || variants[0] || null;
    },
    extractVariantVersionToken(variant) {
      const target = variant && typeof variant === "object" ? variant : null;
      if (!target) return "";
      const openPath = String(target.openpath || target.open_path || "").trim();
      const title = String(target.title || "").trim();
      const haystack = `${openPath} ${title}`;
      const match = haystack.match(/(?:^|[^A-Za-z0-9])v(\d+(?:\.\d+){0,10})(?=[^A-Za-z0-9]|$)/i);
      if (match && match[1]) return String(match[1] || "").trim();
      return "";
    },
    parseVersionParts(token) {
      const text = String(token || "").trim();
      if (!text) return [];
      const parts = text
        .split(".")
        .map((part) => Number.parseInt(String(part || "").replace(/[^\d]/g, ""), 10))
        .filter((num) => Number.isFinite(num));
      return parts;
    },
    compareVersionTokensDesc(tokenA, tokenB) {
      const a = String(tokenA || "").trim();
      const b = String(tokenB || "").trim();
      if (!a && !b) return 0;
      if (a && !b) return -1;
      if (!a && b) return 1;
      const partsA = this.parseVersionParts(a);
      const partsB = this.parseVersionParts(b);
      const maxLen = Math.max(partsA.length, partsB.length, 1);
      for (let i = 0; i < maxLen; i += 1) {
        const va = Number(partsA[i] || 0);
        const vb = Number(partsB[i] || 0);
        if (va === vb) continue;
        return va > vb ? -1 : 1;
      }
      return 0;
    },
    installVersionLabel(variant, idx) {
      const token = this.extractVariantVersionToken(variant);
      if (token) return `v${token}`;
      const index = Number(idx || 0);
      if (Number.isFinite(index) && index >= 0) return `v${index + 1}`;
      return "v1";
    },
    toggleInstallVersionMenu() {
      if (!this.installDialog || !this.installDialog.visible) return;
      if (this.installDialog.loading || this.installDialog.submitting) return;
      const variantCount = this.installVariantCount > 1 ? this.installVariantCount : 0;
      if (variantCount <= 0) return;
      const nextOpen = !this.installDialog.versionMenuOpen;
      this.installDialog.versionMenuOpen = nextOpen;
      if (nextOpen) {
        const idx = Math.max(0, Math.min(variantCount - 1, Number(this.installDialog.selectedVariantIndex || 0)));
        this.setFocusIndex(3 + idx, { scroll: true });
      } else {
        this.setFocusIndex(2, { scroll: false });
      }
    },
    async loadInstallPlanForVariant(variant, options = {}) {
      const target = variant && typeof variant === "object" ? variant : null;
      if (!target) return;

      const closeOnError = Boolean(options && options.closeOnError);
      const focusAfter = options && "focusAfter" in options ? Boolean(options.focusAfter) : true;
      const clearPlan = options && "clearPlan" in options ? Boolean(options.clearPlan) : true;
      const seq = (Number(this.installDialog.requestSeq) || 0) + 1;
      this.installDialog.requestSeq = seq;
      const allowCache = options && "allowCache" in options ? Boolean(options.allowCache) : true;
      if (allowCache) {
        const cacheKey = this.getInstallPlanCacheKey(target);
        const cachedPlan = this.readInstallPlanCache(cacheKey);
        if (cachedPlan) {
          this.installDialog.loading = false;
          this.installDialog.submitting = false;
          this.installDialog.versionMenuOpen = false;
          this.installDialog.plan = cachedPlan;
          if (this.installDialog.plan && this.installDialog.plan.install_dir) {
            this.settings.install_dir = String(this.installDialog.plan.install_dir || this.settings.install_dir || "");
          }
          if (focusAfter) {
            this.setFocusIndex(this.modalCanConfirm ? 1 : 0, { scroll: false });
          }
          return;
        }
      }

      const cacheKey = this.getInstallPlanCacheKey(target);
      if (cacheKey && this.installPlanPending && typeof this.installPlanPending === "object") {
        const inflight = this.installPlanPending[cacheKey];
        if (inflight && typeof inflight.then === "function") {
          try {
            await inflight;
          } catch (_error) {
            // ignore
          }
          const cachedAfter = this.readInstallPlanCache(cacheKey);
          if (cachedAfter) {
            this.installDialog.loading = false;
            this.installDialog.submitting = false;
            this.installDialog.versionMenuOpen = false;
            this.installDialog.plan = cachedAfter;
            if (this.installDialog.plan && this.installDialog.plan.install_dir) {
              this.settings.install_dir = String(this.installDialog.plan.install_dir || this.settings.install_dir || "");
            }
            if (focusAfter) {
              this.setFocusIndex(this.modalCanConfirm ? 1 : 0, { scroll: false });
            }
            return;
          }
        }
      }
      this.installDialog.loading = true;
      this.installDialog.submitting = false;
      this.installDialog.versionMenuOpen = false;
      if (clearPlan) {
        this.installDialog.plan = null;
      }

      if (cacheKey) {
        if (!this.installPlanPending || typeof this.installPlanPending !== "object") {
          this.installPlanPending = {};
        }
        const existingPending = this.installPlanPending[cacheKey];
        if (existingPending && typeof existingPending.then === "function") {
          try {
            await existingPending;
          } catch (_error) {
            // ignore
          }
          const cachedAfterExisting = this.readInstallPlanCache(cacheKey);
          if (cachedAfterExisting) {
            if (seq !== this.installDialog.requestSeq) return;
            this.installDialog.loading = false;
            this.installDialog.plan = cachedAfterExisting;
            if (this.installDialog.plan && this.installDialog.plan.install_dir) {
              this.settings.install_dir = String(this.installDialog.plan.install_dir || this.settings.install_dir || "");
            }
            if (focusAfter) {
              this.setFocusIndex(this.modalCanConfirm ? 1 : 0, { scroll: false });
            }
            return;
          }
        }
      }

      const requestPromise = apiPost("/api/tianyi/install/prepare", {
        game_id: String(target.game_id || ""),
        share_url: String(target.down_url || ""),
        download_dir: this.settings.download_dir,
        install_dir: this.settings.install_dir,
      });
      if (cacheKey) {
        this.installPlanPending[cacheKey] = requestPromise;
      }
      let result = null;
      try {
        result = await requestPromise;
      } finally {
        if (cacheKey && this.installPlanPending && this.installPlanPending[cacheKey] === requestPromise) {
          delete this.installPlanPending[cacheKey];
        }
      }

      if (seq !== this.installDialog.requestSeq) {
        return;
      }

      this.installDialog.loading = false;
      if (result.status !== "success") {
        const errorText = this.buildPrepareInstallError(result);
        this.flash(errorText);
        if (closeOnError) {
          this.closeInstallDialog(true);
        }
        return;
      }

      this.installDialog.plan = result.data || null;
      this.writeInstallPlanCache(this.getInstallPlanCacheKey(target), this.installDialog.plan);
      if (this.installDialog.plan && this.installDialog.plan.install_dir) {
        this.settings.install_dir = String(this.installDialog.plan.install_dir || this.settings.install_dir || "");
      }
      if (focusAfter) {
        this.setFocusIndex(this.modalCanConfirm ? 1 : 0, { scroll: false });
      }
    },
    async chooseInstallVersion(idx) {
      if (!this.installDialog || !this.installDialog.visible) return;
      if (this.installDialog.loading || this.installDialog.submitting) return;
      const variants = Array.isArray(this.installDialog.variants) ? this.installDialog.variants : [];
      if (variants.length <= 1) return;
      const index = Number(idx || 0);
      if (!Number.isFinite(index) || index < 0 || index >= variants.length) return;

      if (index !== Number(this.installDialog.selectedVariantIndex || 0)) {
        this.installDialog.selectedVariantIndex = index;
        this.installDialog.game = variants[index];
        void this.fetchCoverForItem(variants[index], { force: true });
      }

      this.installDialog.versionMenuOpen = false;
      this.setFocusIndex(2, { scroll: false });
      await this.loadInstallPlanForVariant(variants[index], { closeOnError: false, focusAfter: false, clearPlan: false });
    },
    async selectInstallVariant(idx) {
      await this.chooseInstallVersion(idx);
    },
    async openInstallConfirm(item) {
      if (!item) return;
      const rawVariants = item && Array.isArray(item.variants) ? item.variants : [item];
      const candidates = [];
      const seen = new Set();
      for (const entry of rawVariants) {
        if (!entry || typeof entry !== "object") continue;
        const gameId = String(entry.game_id || "").trim();
        if (!gameId) continue;
        const downUrl = String(entry.down_url || "").trim();
        if (!downUrl) continue;
        const token = `${gameId}||${downUrl}`;
        if (seen.has(token)) continue;
        seen.add(token);
        candidates.push(entry);
      }
      if (!candidates.length) return;

      candidates.sort((a, b) => {
        const av = this.extractVariantVersionToken(a);
        const bv = this.extractVariantVersionToken(b);
        const vcmp = this.compareVersionTokensDesc(av, bv);
        if (vcmp !== 0) return vcmp;
        const aw = this.variantSortWeight(a && a.title);
        const bw = this.variantSortWeight(b && b.title);
        if (aw !== bw) return aw - bw;
        const at = String((a && a.title) || "");
        const bt = String((b && b.title) || "");
        if (at.length !== bt.length) return at.length - bt.length;
        const as = Number((a && a.size_bytes) || 0);
        const bs = Number((b && b.size_bytes) || 0);
        return bs - as;
      });

      const selected = candidates[0];
      void this.fetchCoverForItem(selected, { force: true });
      this.installDialog.visible = true;
      this.installDialog.variants = candidates;
      this.installDialog.selectedVariantIndex = 0;
      this.installDialog.game = selected;
      this.installDialog.versionMenuOpen = false;
      this.setFocusIndex(0, { scroll: false });
      await this.loadInstallPlanForVariant(selected, { closeOnError: true, focusAfter: true, clearPlan: true });
    },
    closeInstallDialog(force = false) {
      if (!force && this.installDialog.submitting) return;
      this.installDialog.visible = false;
      this.installDialog.loading = false;
      this.installDialog.submitting = false;
      this.installDialog.plan = null;
      this.installDialog.game = null;
      this.installDialog.variants = [];
      this.installDialog.selectedVariantIndex = 0;
      this.installDialog.versionMenuOpen = false;
      this.setFocusIndex(0, { scroll: false });
    },
    async confirmInstall() {
      if (!this.installDialog.plan || !this.modalCanConfirm) return;
      this.installDialog.submitting = true;
      const plan = this.installDialog.plan;
      const variant = this.currentInstallVariant() || this.installDialog.game || {};
      const fileIds = Array.isArray(plan.files)
        ? plan.files.map((item) => String((item && item.file_id) || "")).filter(Boolean)
        : [];

      const result = await apiPost("/api/tianyi/install/start", {
        game_id: String(plan.game_id || ""),
        share_url: String(plan.share_url || (variant && variant.down_url) || ""),
        file_ids: fileIds,
        split_count: this.settings.split_count,
        download_dir: this.settings.download_dir,
        install_dir: this.settings.install_dir,
      });
      this.installDialog.submitting = false;
      if (result.status !== "success") {
        this.flash(result.message || "创建安装任务失败");
        return;
      }
      const baseText = "安装任务已创建，正在下载";
      const notice =
        result && result.data && result.data.plan && result.data.plan.provider_notice
          ? String(result.data.plan.provider_notice || "").trim()
          : "";
      this.flash(notice ? `${baseText}\n${notice}` : baseText);
      this.closeInstallDialog(true);
    },
    formatBytes(bytes) {
      const num = Number(bytes || 0);
      if (!Number.isFinite(num) || num <= 0) return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      let value = num;
      let idx = 0;
      while (value >= 1024 && idx < units.length - 1) {
        value /= 1024;
        idx += 1;
      }
      if (idx === 0) return `${Math.floor(value)} ${units[idx]}`;
      return `${value.toFixed(2)} ${units[idx]}`;
    },
    formatBytesCompact(bytes) {
      const num = Number(bytes || 0);
      if (!Number.isFinite(num) || num <= 0) return "0B";
      if (num >= 1024 ** 3) return `${(num / (1024 ** 3)).toFixed(2)}G`;
      if (num >= 1024 ** 2) return `${(num / (1024 ** 2)).toFixed(2)}M`;
      if (num >= 1024) return `${(num / 1024).toFixed(2)}K`;
      return `${Math.floor(num)}B`;
    },
    splitModalTitle(title) {
      const raw = String(title || "").replace(/\s+/g, " ").trim();
      if (!raw) {
        return { cn: "-", en: "GAME INSTALL" };
      }

      const sepParts = raw
        .split(/[\/|｜]/)
        .map((part) => part.trim())
        .filter(Boolean);
      if (sepParts.length >= 2) {
        const cn = (sepParts[0].match(/[\u3400-\u9FFF0-9A-Za-z：:·《》、（）()\-]+/g) || [sepParts[0]]).join("").trim() || "-";
        const en = sepParts.slice(1).join(" ").replace(/[^A-Za-z0-9\s:.'\-&!?()]/g, " ").replace(/\s+/g, " ").trim().toUpperCase() || "GAME INSTALL";
        return { cn, en };
      }

      const cnParts = raw.match(/[\u3400-\u9FFF0-9：:·《》、（）()]+/g) || [];
      const cn = cnParts.join("").trim() || raw;
      const englishMatches = raw.match(/[A-Za-z][A-Za-z0-9\s:.'\-&!?()]+/g) || [];
      const en = englishMatches.join(" ").replace(/\s+/g, " ").trim().toUpperCase();
      return { cn, en: en || "GAME INSTALL" };
    },
    modalChineseTitle(title) {
      return this.splitModalTitle(title).cn || "-";
    },
    modalEnglishTitle(title) {
      return this.splitModalTitle(title).en || "GAME INSTALL";
    },
    statusClass(ok) {
      return ok ? "space-ok" : "space-bad";
    },
  },
}).mount("#app");
