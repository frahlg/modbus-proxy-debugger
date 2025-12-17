import asyncio
import contextlib
import os
import pathlib
from contextlib import asynccontextmanager
from typing import Any, Dict, Iterable, Optional, Protocol

from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


class LiveFeed:
    """Lightweight broadcaster for websockets."""

    def __init__(self) -> None:
        self._listeners: set[asyncio.Queue[dict[str, Any]]] = set()

    def register(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=500)
        self._listeners.add(q)
        return q

    def unregister(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._listeners.discard(queue)

    def publish(self, message: dict[str, Any]) -> None:
        for queue in list(self._listeners):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(message)
                except asyncio.QueueFull:
                    pass


class ConfigPayload(BaseModel):
    bind: str
    target: str
    map_path: Optional[str] = None
    log_file: Optional[str] = None
    verbose: bool = False
    debug: bool = False
    allow_write: bool = False
    timeout_s: float = Field(default=10.0, alias="timeout")
    max_retries: int = 1
    max_frame_bytes: int = 512
    dedup_window_s: float = Field(default=5.0, alias="dedup_window")
    max_log_dir_mb: int = 200
    max_log_files: int = 50
    http_bind: Optional[str] = None
    csv_enabled: bool = True
    csv_file: Optional[str] = None


class PartialConfig(BaseModel):
    bind: Optional[str] = None
    target: Optional[str] = None
    map_path: Optional[str] = None
    log_file: Optional[str] = None
    verbose: Optional[bool] = None
    debug: Optional[bool] = None
    allow_write: Optional[bool] = None
    timeout_s: Optional[float] = Field(default=None, alias="timeout")
    max_retries: Optional[int] = None
    max_frame_bytes: Optional[int] = None
    dedup_window_s: Optional[float] = Field(default=None, alias="dedup_window")
    max_log_dir_mb: Optional[int] = None
    max_log_files: Optional[int] = None
    http_bind: Optional[str] = None
    csv_enabled: Optional[bool] = None
    csv_file: Optional[str] = None

    class Config:
        extra = "ignore"


class ControllerProtocol(Protocol):
    """Small protocol so create_app can stay decoupled from controller impl."""

    async def start(self, config: Dict[str, Any]) -> Dict[str, Any]:
        ...

    async def stop(self) -> Dict[str, Any]:
        ...

    async def update_config(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def snapshot(self) -> Dict[str, Any]:
        ...


def _list_maps(maps_dir: pathlib.Path) -> list[str]:
    if not maps_dir.exists():
        return []
    return [
        str(p.resolve())
        for p in maps_dir.glob("**/*.y*ml")
        if p.is_file()
    ]


def create_app(
    controller: ControllerProtocol,
    feed: LiveFeed,
    *,
    maps_dir: pathlib.Path,
    web_dir: pathlib.Path,
) -> FastAPI:
    maps_dir.mkdir(parents=True, exist_ok=True)
    web_dir.mkdir(parents=True, exist_ok=True)

    stats_task: Optional[asyncio.Task[None]] = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal stats_task

        async def emit_stats() -> None:
            while True:
                await asyncio.sleep(1)
                feed.publish({"type": "stats", "data": controller.snapshot()})

        stats_task = asyncio.create_task(emit_stats())
        try:
            yield
        finally:
            if stats_task:
                stats_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stats_task

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/web", StaticFiles(directory=web_dir, html=True), name="web")

    @app.get("/web", response_class=FileResponse)
    def serve_index() -> FileResponse:
        index_path = web_dir / "index.html"
        return FileResponse(index_path)

    @app.get("/api/status")
    async def get_status() -> dict[str, Any]:
        data = controller.snapshot()
        data["available_maps"] = _list_maps(maps_dir)
        return data

    @app.post("/api/start")
    async def start_proxy(payload: ConfigPayload) -> dict[str, Any]:
        config_dict = payload.model_dump()
        result = await controller.start(config_dict)
        result["available_maps"] = _list_maps(maps_dir)
        return result

    @app.post("/api/stop")
    async def stop_proxy() -> dict[str, Any]:
        result = await controller.stop()
        result["available_maps"] = _list_maps(maps_dir)
        return result

    @app.post("/api/config")
    async def update_config(payload: PartialConfig) -> dict[str, Any]:
        try:
            result = await controller.update_config(payload.model_dump(exclude_none=True))
            result["available_maps"] = _list_maps(maps_dir)
            return result
        except ValueError as exc:  # Likely missing initial config
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/map/upload")
    async def upload_map(file: UploadFile = File(...), set_current: bool = False) -> dict[str, Any]:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Filename missing")
        dest = (maps_dir / os.path.basename(file.filename)).resolve()
        content = await file.read()
        dest.write_bytes(content)
        updates: dict[str, Any] = {}
        if set_current:
            updates["map_path"] = str(dest)
            await controller.update_config(updates)
        return {"saved_as": str(dest), "available_maps": _list_maps(maps_dir)}

    @app.post("/api/map/select")
    async def select_map(path: str) -> dict[str, Any]:
        candidate = pathlib.Path(path)
        if not candidate.is_absolute():
            candidate = (maps_dir / path).resolve()
        if not candidate.exists():
            raise HTTPException(status_code=404, detail="Map not found")
        result = await controller.update_config({"map_path": str(candidate)})
        result["available_maps"] = _list_maps(maps_dir)
        return result

    @app.get("/api/maps")
    async def list_maps() -> dict[str, Iterable[str]]:
        return {"available_maps": _list_maps(maps_dir)}

    @app.websocket("/api/ws/live")
    async def websocket_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        queue = feed.register()
        await ws.send_json({"type": "stats", "data": controller.snapshot()})
        try:
            while True:
                msg = await queue.get()
                await ws.send_json(msg)
        except WebSocketDisconnect:
            pass
        finally:
            feed.unregister(queue)

    return app
