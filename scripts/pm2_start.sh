#!/usr/bin/env bash
# 用 pm2 拉起仓库内的全部服务（定义见 ecosystem.config.js）。
#
#   ./scripts/pm2_start.sh                              # 全部
#   ./scripts/pm2_start.sh trendline-web astock-daily   # 只起指定的几个
#
# 页面都在 trendline-web 的 5000 上：趋势线 /、A 股 /biga、StochRSI /stochrsi 等，
# A 股不再单独占 8012（本地调试仍可 .venv/bin/python -m astock.app）。
#
# 前置：node + npm i -g pm2；venv（PYTHON_BIN 可覆盖）；config_constants.py（不入库，
# 从本地拷）；A 股库（data/astock/index_daily.csv）缺了本脚本会先建一次。
set -euo pipefail
cd "$(dirname "$0")/.."

# pm2 daemon 的 cron_restart 按启动时的时区触发：首次起 pm2 之前定好 TZ，
# 否则 14:50 会按 UTC 跑成 22:50（已有 daemon 的话要先 pm2 kill 再重跑本脚本）
export TZ="${TZ:-Asia/Shanghai}"
if [ "$(date +%Z)" != "CST" ]; then
  echo "警告：系统时区是 $(date +%Z)，建议 sudo timedatectl set-timezone Asia/Shanghai"
fi

PY="${PYTHON_BIN:-.venv/bin/python}"

command -v pm2 >/dev/null || { echo "没装 pm2：npm i -g pm2"; exit 1; }
[ -x "$PY" ] || {
  echo "找不到 ${PY}；先 python -m venv .venv && .venv/bin/pip install -r requirements.txt，"
  echo "或者用 PYTHON_BIN=/opt/venv/bin/python ./scripts/pm2_start.sh 指定解释器"
  exit 1
}
[ -f config_constants.py ] || echo "警告：缺 config_constants.py（不入库），发邮件和趋势线 Web 会起不来"

# A 股库（data/）不入库：缺了就先建一次。页面首次访问也会自愈触发采集，但 CLI 建库
# 能立刻看到结果，首次部署不必靠「打开页面等 10 秒」
if [ ! -f data/astock/index_daily.csv ]; then
  echo "A 股库缺失，先增量建库…"
  "$PY" -m astock.data --sync || echo "警告：建库失败，页面首次访问会自动重试"
fi

# 运行期依赖自查：requirements.txt 漏了 requests/schedule/TA-Lib，全新环境会「起来了就崩」
missing="$("$PY" - <<'PY'
import importlib.util
mods = [("flask", "flask"), ("pandas", "pandas"), ("numpy", "numpy"), ("requests", "requests"),
        ("schedule", "schedule"), ("talib", "TA-Lib"), ("ccxt", "ccxt"),
        ("Crypto", "pycryptodome"), ("cryptography", "cryptography")]
print(" ".join(pkg for mod, pkg in mods if importlib.util.find_spec(mod) is None))
PY
)"
if [ -n "$missing" ]; then
  echo "警告：缺依赖 ${missing}；先 $PY -m pip install ${missing}（TA-Lib 还要先装系统库 ta-lib）"
fi

mkdir -p logs/pm2

# 旧配置里 A 股是独立 8012 进程，现在挂到 trendline-web 的 /biga 下，残留的先清掉
pm2 delete astock-app >/dev/null 2>&1 || true

if [ $# -gt 0 ]; then
  only="$(IFS=,; echo "$*")"
  pm2 start ecosystem.config.js --only "$only" --update-env
else
  pm2 start ecosystem.config.js --update-env
fi

# pm2 start 会立刻把每个 app 跑一次：astock-daily 一跑就发信，astock-sync 会写库。
# 先把这两个一次性任务压住，等 cron_restart 到点自己拉起（stopped 状态下 cron 依然生效）
pm2 stop astock-daily astock-sync >/dev/null 2>&1 || true

pm2 save
pm2 ls

# 探一下页面：/biga 未登录应当 302 跳到 /login，说明蓝图挂上了
sleep 2
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:5000/biga/ || true)"
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
