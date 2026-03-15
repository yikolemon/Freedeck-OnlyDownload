# aria2 内置二进制说明

插件会按以下优先级寻找 aria2：

1. 环境变量 `FRIENDECK_ARIA2_BIN`
2. `defaults/aria2/linux-x64/aria2c`（x86_64）
3. `defaults/aria2/linux-arm64/aria2c`（aarch64）
4. `defaults/aria2/aria2c`
5. 系统 `aria2c`（兜底）

当前仓库已内置以下二进制：

- `linux-x64/aria2c`：`aria2 1.36.0 static linux amd64`
- `linux-arm64/aria2c`：`aria2 1.36.0 static linux arm64`

来源：

- <https://github.com/P3TERX/Aria2-Pro-Core/releases/tag/1.36.0_2021.08.22>
