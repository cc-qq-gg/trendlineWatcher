"""独立的本地预览服务（127.0.0.1:8012），两个视图：指数关系 / 双动量轮动。

启动（工作目录必须是仓库根；项目是 package=false，包靠 cwd 进 sys.path）：

    uv run python -m astock.app

接口：
    GET  /                     页面
    GET  /api/meta             数据覆盖、序列清单、预设、刷新状态
    GET  /api/relations        多条序列同轴（原始点位 / 滚动分位 / ERP）
    GET  /api/momentum         双动量轮动回测
    POST /api/refresh          后台增量采集（立刻返回，前端轮询 meta）
"""

from __future__ import annotations

import gzip
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from . import service

ROOT = Path(__file__).resolve().parent.parent
STATIC = Path(__file__).resolve().parent / "static"
VENDOR = ROOT / "vendor"

app = Flask(__name__, static_folder=None)
app.json.ensure_ascii = False          # Flask 2.3 起 JSON_AS_ASCII 已废弃，中文不再转义
app.json.sort_keys = False


def _args() -> dict:
    """收集出现过的查询参数并在解析阶段就报错，统一由 ValueError 转 400。"""
    out: dict = {}
    for name, cast in (
        ("window", int),
        ("entry", float),
        ("exit_threshold", float),
        ("cost_rate", float),
        ("years", float),
        ("preset", str),
        ("start", str),
        ("codes", str),
    ):
        raw = request.args.get(name)
        if raw is not None and raw != "":
            out[name] = raw if cast is str else cast(raw)
    return out


def _flag(name: str, default: bool) -> bool:
    raw = request.args.get(name)
    if raw is None or raw == "":
        return default
    return raw not in ("0", "false", "False")


def _reply(result: dict):
    response = jsonify(result)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.after_request
def _gzip(response):
    """给 JSON / 文本回压实测 5 倍：回测 637KB→123KB，关系图 491KB→110KB。

    只压这两类、且 >1KB 才压；静态文件是 direct_passthrough，get_data() 会抛，先跳过。
    自己写而不是引 flask-compress，是为了不给这台机器多加一个依赖。
    """
    if response.direct_passthrough or response.status_code != 200:
        return response
    if "Content-Encoding" in response.headers:
        return response
    if "gzip" not in request.headers.get("Accept-Encoding", "").lower():
        return response
    if not response.headers.get("Content-Type", "").startswith(("application/json", "text/")):
        return response
    body = response.get_data()
    if len(body) < 1024:
        return response
    packed = gzip.compress(body, 6)
    response.set_data(packed)
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = str(len(packed))
    response.headers["Vary"] = "Accept-Encoding"
    return response


def _error(exc: ValueError):
    return jsonify({"error": str(exc)}), 400


@app.get("/")
def index():
    return send_from_directory(STATIC, "index.html")


@app.get("/vendor/<path:name>")
def vendor(name: str):
    # 图表库共享仓库根的 vendor，不在本模块内联一份
    return send_from_directory(VENDOR, name)


@app.get("/api/meta")
def api_meta():
    return _reply(service.meta())


@app.get("/api/relations")
def api_relations():
    try:
        args = _args()
        codes = [code for code in args["codes"].split(",") if code] if "codes" in args else None
        result = service.relations(
            codes=codes,
            start=args.get("start"),
            years=int(args.get("years", service.DEFAULT_YEARS)),
        )
    except ValueError as exc:
        return _error(exc)
    return _reply(result)


@app.get("/api/momentum")
def api_momentum():
    try:
        args = _args()
        # 区间优先级：显式 start > years（最近 N 年）> 默认全历史。
        # 三者都落到 momentum.window_start，那边 start_date 会压过 years，所以这里先择一。
        start = args.get("start")
        if start is None and "years" not in args:
            start = service.DEFAULT_START
        result = service.momentum_view(
            preset=args.get("preset", "optimized"),
            window=args.get("window"),
            entry=args.get("entry"),
            exit_threshold=args.get("exit_threshold"),
            cost_rate=args.get("cost_rate"),
            years=args.get("years"),
            start=start,
            detail=_flag("detail", True),
        )
    except ValueError as exc:
        return _error(exc)
    return _reply(result)


@app.post("/api/refresh")
def api_refresh():
    return _reply(service.refresh())


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8012, debug=False)
