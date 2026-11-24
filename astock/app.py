"""A 股页面（指数关系 / 双动量轮动）：独立蓝图，挂在哪、带不带登录由调用方决定。

- 本地：`uv run --no-sync python -m astock.app` → http://127.0.0.1:8012/biga/
  （和线上同一个路径，省得两边对不上）
- 线上：TrendlineWebApp 调 `init_app(app, "/biga", require_login=True)`，与趋势线页面共用
  端口和登录 session，不再单独占一个端口

接口（都挂在前缀下）：
    GET  /biga/               页面
    GET  /biga/vendor/<name>  图表库（相对路径引用，蓝图自带，不依赖宿主 app）
    GET  /biga/api/meta       数据覆盖、序列清单、预设、刷新状态
    GET  /biga/api/relations  多条序列同轴（原始点位 / 滚动分位 / ERP）
    GET  /biga/api/momentum   双动量轮动回测
    POST /biga/api/refresh    后台增量采集（立刻返回，前端轮询 meta）

数据自愈：GET 进来先看库在不在，缺了就后台起一次增量采集（同一进程只起一条），
首次部署不用先手动建库——页面自己会从「无数据」等到有数据。
"""

from __future__ import annotations

import gzip
from pathlib import Path

from flask import (
    Blueprint,
    Flask,
    current_app,
    jsonify,
    redirect,
    request,
    send_from_directory,
    session,
)

from . import service

ROOT = Path(__file__).resolve().parent.parent
STATIC = Path(__file__).resolve().parent / "static"
VENDOR = ROOT / "vendor"
PREFIX = "/biga"

bp = Blueprint("astock", __name__)
_EXT_KEY = "astock_gzip"           # 宿主 app 上标记「压缩钩子已挂」，重复 init 不叠加


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


def _gzip(response):
    """给 JSON / HTML / JS 回压实测 5 倍：回测 637KB→123KB，页面 41KB→6KB。

    只压这两类、且 >1KB 才压；direct_passthrough 的响应 get_data() 会抛，静态文件
    在 _asset() 里已经关掉这个开关。自己写而不是引 flask-compress，是为了不给这台
    机器多加一个依赖。
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


def init_app(target: Flask, url_prefix: str = PREFIX, require_login: bool = False,
             gzip_scope: str | None = None) -> None:
    """把 A 股页面挂到任意 Flask app 上。

    - url_prefix：路由前缀，本地与线上统一 /biga；
    - require_login：True 时页面未登录跳 /login、接口返回 401 JSON（宿主有登录系统才开）；
    - gzip_scope：压缩生效的路径前缀，None 表示只压本页面的路由，"" 表示全站。
    """
    target.register_blueprint(bp, url_prefix=url_prefix)
    target.json.ensure_ascii = False          # Flask 2.3 起 JSON_AS_ASCII 已废弃，中文不再转义
    target.json.sort_keys = False
    if require_login:
        target.config["ASTOCK_REQUIRE_LOGIN"] = True
    if target.extensions.get(_EXT_KEY):
        return
    target.extensions[_EXT_KEY] = True
    scope = (url_prefix if gzip_scope is None else gzip_scope).rstrip("/")

    @target.after_request
    def _maybe_gzip(response):
        if scope and not request.path.startswith(scope):
            return response
        return _gzip(response)


@bp.before_request
def _gate():
    """先验登录（宿主开了才验），再顺手确认库在——未登录的请求不触发采集。"""
    if current_app.config.get("ASTOCK_REQUIRE_LOGIN") and not session.get("authenticated"):
        if "/api/" in request.path:
            return jsonify({"success": False, "message": "需要登录", "redirect": "/login"}), 401
        return redirect("/login")
    if request.method == "GET":
        service.ensure_data()
    return None


def _asset(directory: Path, filename: str):
    """静态文件先走 conditional（重复访问直接 304），再放开 passthrough 让 after_request
    能读到内容做 gzip：页面 41KB→6KB，图表库 196KB→57KB，弱网下差别很明显。"""
    response = send_from_directory(directory, filename, conditional=True)
    response.direct_passthrough = False      # 文件都很小，读进内存压一次没有压力
    return response


@bp.get("/")
def index():
    return _asset(STATIC, "index.html")


@bp.get("/vendor/<path:name>")
def vendor(name: str):
    # 图表库共享仓库根的 vendor，不在本模块内联一份；页面里用相对路径引用，
    # 所以独立跑和挂在 /biga 下都不需要改 HTML
    return _asset(VENDOR, name)


@bp.get("/api/meta")
def api_meta():
    return _reply(service.meta())


@bp.get("/api/relations")
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


@bp.get("/api/momentum")
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


@bp.post("/api/refresh")
def api_refresh():
    return _reply(service.refresh())


def _error(exc: ValueError):
    return jsonify({"error": str(exc)}), 400


# ---------------- 本地独立入口 ----------------

app = Flask(__name__, static_folder=None)
init_app(app, PREFIX, gzip_scope="")        # 本地跑也走 /biga，HTML 里全是相对路径


@app.get("/")
def _root():
    """老地址不 404：`/` 直接送到 /biga/，本地收藏夹不用改。"""
    return redirect(f"{PREFIX}/")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8012, debug=False)
