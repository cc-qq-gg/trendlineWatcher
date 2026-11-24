// pm2 生态文件：仓库里的常驻服务与定时任务都在这里。
//
//   pm2 start ecosystem.config.js                                   # 全部
//   pm2 start ecosystem.config.js --only trendline-web              # 指定几个
//   pm2 logs astock-daily --lines 50
//   pm2 save && pm2 startup                                         # 开机自启（startup 需 sudo）
//
// 启动方式：uv run --no-sync <python -m 模块 | 脚本>。
// - 用 uv 而不是 .venv/bin/python：环境由 uv.lock 定义，uv sync 一次之后所有入口都认
//   同一个解释器，服务器上不用记 venv 路径；
// - --no-sync：pm2 只管拉起进程，不动环境。否则每天 cron 拉起的瞬间会去 sync，
//   和部署/重启撞在一起抢 .venv，而且会把「不在 lock 里的包」卸掉。增删依赖请显式
//   `uv sync --inexact`（pm2_start.sh 每次启动都会跑一遍，幂等）。
// - UV_BIN：uv 不在 PATH 里（服务器 ~/.local/bin、nvm 环境）时，由 pm2_start.sh 导出
//   绝对路径；手动 `pm2 start` 时也可以 UV_BIN=/usr/local/bin/uv pm2 start ...
//
// 时区：daily 的 14:50 闸门、回填 cutoff 和 cron_restart 都跟本地时钟走。cron_restart
// 用的是 pm2 daemon 启动时的时区，首次启动 pm2 前先 export TZ=Asia/Shanghai（或者把
// 服务器时区设成 Asia/Shanghai），否则 14:50 会按 UTC 触发。
const path = require('path');

const ROOT = __dirname;
const UV = process.env.UV_BIN || 'uv';

// 统一壳：uv run + 模块/脚本参数，日志落 logs/pm2（gitignore 里已忽略 logs/）
function app(name, args, extra = {}) {
  return {
    name,
    script: UV,
    args: `run --no-sync ${args}`,
    cwd: ROOT, // 项目是 package=false，包靠 cwd 进 sys.path；data/ 也是相对路径
    interpreter: 'none',
    env: { TZ: 'Asia/Shanghai', PYTHONUNBUFFERED: '1' },
    out_file: path.join(ROOT, 'logs/pm2', `${name}.out.log`),
    error_file: path.join(ROOT, 'logs/pm2', `${name}.err.log`),
    merge_logs: true,
    time: true, // 每行日志带时间戳
    ...extra,
  };
}

module.exports = {
  apps: [
    // 14:50 取盘中价判信号并发邮件。一次性任务：跑完即退，autorestart 必须关掉，
    // 靠 cron_restart 每交易日 14:50 拉起（1-5 = 周一到周五，daily 内部还会再跳周末）。
    // --cron：pm2 start/restart 会立刻把一次性任务跑一遍，这个开关让它只在 14:45-15:05
    // 窗口内真跑，部署时不会顺手发一封「收盘后」的假 14:50 邮件
    app('astock-daily', 'python -m astock.daily --send --cron', {
      autorestart: false,
      cron_restart: '50 14 * * 1-5',
    }),

    // 15:10 把已定稿的收盘行写进 CSV（不带 --send，不发信）。少了这一步库不会自己长，
    // detect 算的是「历史 + 今日盘中价」，历史缺口会让窗口数对不上交易日。
    // --cron 带上收盘窗口：部署时立刻被拉起也不会去拉数据、更不会被 SIGINT 打断在写盘中
    app('astock-sync', 'python -m astock.daily --write --cron 15:05-15:30', {
      autorestart: false,
      cron_restart: '10 15 * * 1-5',
    }),

    // 趋势线 Web（0.0.0.0:5000）：A 股页面挂在同一个端口下的 /biga（见 TrendlineWebApp
    // 末尾的 init_app），所以这里不再有独立的 astock 端口。进程内还挂着股票监控调度
    // （每日 16:00 检查），stock_monitor_scheduler 不用单独起，起了就是重复检查。
    // 本地调试 A 股页面仍可单独起 8012：`uv run --no-sync python -m astock.app`
    app('trendline-web', 'run_trendline_monitor.py'),

    // BTC 数据同步 + 回测 + 定时告警（默认 UTC 00:05/04:05/.../20:05 六班）
    app('btc-strategy-worker', 'python -m btc_strategy.worker'),

    // StochRSI 背离盯盘：常驻 schedule，出信号走邮件
    app('watcher-stochrsi', 'watcher_stochrsi.py'),
  ],
};
