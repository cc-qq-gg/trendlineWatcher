#!/usr/bin/env bash
# 用 pm2 拉起仓库内的全部服务（定义见 ecosystem.config.js）。
#
#   ./scripts/pm2_start.sh                              # 全部
#   ./scripts/pm2_start.sh trendline-web astock-daily   # 只起指定的几个
#
# 页面都在 trendline-web 的 5000 上：趋势线 /、A 股 /biga、StochRSI /stochrsi 等，
# A 股不再单独占 8012（本地调试仍可 uv run python -m astock.app）。
#
# 前置：node + npm i -g pm2；uv（curl -LsSf https://astral.sh/uv/install.sh | sh）；
# config_constants.py（不入库，从本地拷）；A 股库（data/astock/index_daily.csv）
# 缺了本脚本会先建一次。
#
# 环境只有一处真相：pyproject.toml + uv.lock。本脚本每次跑 `uv sync --inexact`
# （幂等，没变化时几十毫秒），pm2 里各进程用 `uv run --no-sync` 复用同一个 .venv。
set -euo pipefail
cd "$(dirname "$0")/.."

# pm2 daemon 的 cron_restart 按启动时的时区触发：首次起 pm2 之前定好 TZ，
# 否则 14:50 会按 UTC 跑成 22:50（已有 daemon 的话要先 pm2 kill 再重跑本脚本）
export TZ="${TZ:-Asia/Shanghai}"
if [ "$(date +%Z)" != "CST" ]; then
  echo "警告：系统时区是 $(date +%Z)，建议 sudo timedatectl set-timezone Asia/Shanghai"
fi

command -v pm2 >/dev/null || { echo "没装 pm2：npm i -g pm2"; exit 1; }
command -v uv >/dev/null || {
  echo "没装 uv：curl -LsSf https://astral.sh/uv/install.sh | sh"
  echo "（装完新开一个 shell，或 export PATH=\"\$HOME/.local/bin:\$PATH\"）"
  exit 1
}
# pm2 daemon 的 PATH 往往不含 ~/.local/bin，ecosystem 只认绝对路径
export UV_BIN="$(command -v uv)"
echo "uv: ${UV_BIN}"

[ -f config_constants.py ] || echo "警告：缺 config_constants.py（不入库），发邮件和趋势线 Web 会起不来"

# 环境同步：首次会按 uv.lock 建 .venv 并装齐依赖（含 TA-Lib，需要系统库 ta-lib）；
# --inexact 保留手工装的额外包，避免「不在 lock 里」的包被顺手卸掉
echo "同步环境（uv.lock）…"
"$UV_BIN" sync --inexact || {
  echo "警告：uv sync 失败；TA-Lib 编译不过时先装系统库："
  echo "  curl -LO https://github.com/ta-lib/ta-lib/releases/download/v0.4.0/ta-lib-0.4.0-src.tar.gz"
  echo "  tar -xzf ta-lib-0.4.0-src.tar.gz && cd ta-lib && ./configure --prefix=/usr && make && sudo make install"
  exit 1
}

# A 股库（data/）不入库：缺了就先建一次。页面首次访问也会自愈触发采集，但 CLI 建库
# 能立刻看到结果，首次部署不必靠「打开页面等 10 秒」
if [ ! -f data/astock/index_daily.csv ]; then
  echo "A 股库缺失，先增量建库…"
  "$UV_BIN" run --no-sync python -m astock.data --sync || echo "警告：建库失败，页面首次访问会自动重试"
fi

# 运行期依赖自查：uv sync 只保证 lock 里的包，模块名对不上/系统库缺失它看不出来
missing="$("$UV_BIN" run --no-sync python - <<'PY'
import importlib.util
mods = [("flask", "flask"), ("pandas", "pandas"), ("numpy", "numpy"), ("requests", "requests"),
        ("schedule", "schedule"), ("talib", "TA-Lib"), ("ccxt", "ccxt"),
        ("Crypto", "pycryptodome"), ("cryptography", "cryptography")]
print(" ".join(pkg for mod, pkg in mods if importlib.util.find_spec(mod) is None))
PY
)"
if [ -n "$missing" ]; then
  echo "警告：缺依赖 ${missing}；先 uv add ${missing}（TA-Lib 还要先装系统库 ta-lib）"
fi

mkdir -p logs/pm2

# 已存在同名 app 时必须先删再起：pm2 start 对运行中的同名 app 只做 restartProcessId，
# script/args 沿用旧值（改了 ecosystem 也不会生效，表现为「配置改了进程还是老命令」）。
# astock-app 是上一版独立 8012 进程的残留，一并清掉。
APPS=(astock-daily astock-sync trendline-web btc-strategy-worker watcher-stochrsi astock-app)

if [ $# -gt 0 ]; then
  only="$(IFS=,; echo "$*")"
  pm2 delete "$@" >/dev/null 2>&1 || true
  pm2 start ecosystem.config.js --only "$only" --update-env
else
  pm2 delete "${APPS[@]}" >/dev/null 2>&1 || true
  pm2 start ecosystem.config.js --update-env
fi

# pm2 start 会立刻把每个 app 跑一次。两个一次性任务不需要在这里 stop：它们都带
# --cron 窗口守卫，窗口外启动只是空转几秒、打印一行「跳过」就退出（autorestart=false
# 会自动停在 stopped），窗口内启动等于一次手动补跑，正是想要的。
# 以前这里用 `pm2 stop` 压住，结果是进程在 import pandas 途中被 SIGINT 打断，
# 日志里只剩 KeyboardInterrupt，反而看不出到底跑没跑。

pm2 save
pm2 ls

# 探一下页面：/biga 未登录应当 302 跳到 /login，说明蓝图挂上了。
# uv run 冷启动要先把 pandas 拉进来（~2s），给 5 次机会再判死
code=""
for _ in 1 2 3 4 5; do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:5000/biga/ || true)"
  [ "$code" != "000" ] && [ -n "$code" ] && break
  sleep 1
done
case "$code" in
  200|302) echo "/biga 已就绪（HTTP ${code}，未登录会跳登录页）" ;;
  000|"")  echo "警告：5000 没响应，可能 trendline-web 没起或启动报错，看 pm2 logs trendline-web" ;;
  *)
    echo "警告：/biga 返回 HTTP ${code}，看 pm2 logs trendline-web"
    # 常见坑：5000 被别的程序（旧版服务、别的项目）先占了，pm2 起得来但请求全被它接走
    echo "  当前占着 5000 的进程："
    if command -v lsof >/dev/null 2>&1; then
      lsof -nP -iTCP:5000 -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print "    " $1 " pid=" $2}' || true
    elif command -v ss >/dev/null 2>&1; then
      ss -ltnp 2>/dev/null | awk '$4 ~ /:5000$/ {print "    " $0}' || true
    fi
    ;;
esac

echo
echo "常用：# pm2 logs astock-daily --lines 50 / pm2 logs trendline-web / pm2 restart trendline-web / pm2 startup（开机自启，需 sudo）"
