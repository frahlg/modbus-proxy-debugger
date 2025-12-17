import atexit
import asyncio
import argparse
import json
import logging
import logging.handlers
import os
import struct
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse

try:
    import yaml
except ImportError:
    sys.exit(
        "Error: The 'pyyaml' module is missing.\n"
        "Fix: run `uv sync` (or `uv add pyyaml`) and try again."
    )

# Optional: colored console output
try:
    from colorama import Fore, Style, init

    init(autoreset=True)
except ImportError:

    class _MockColor:
        def __getattr__(self, _name: str) -> str:
            return ""

    Fore = Style = _MockColor()  # type: ignore


logger = logging.getLogger("modbus_proxy")
csv_logger = logging.getLogger("modbus_proxy.csv")


class TimestampedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Rotates logs by size, renaming current file with timestamp."""

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base, ext = os.path.splitext(self.baseFilename)
        new_name = f"{base}_{timestamp}{ext}"

        if os.path.exists(self.baseFilename):
            try:
                os.rename(self.baseFilename, new_name)
            except OSError:
                pass

        if not self.delay:
            self.stream = self._open()


class DedupingHandler(logging.Handler):
    """Coalesces repeated identical formatted log lines.

    Emits the first occurrence immediately; subsequent repeats within `window_s`
    are suppressed. When the line changes or the window elapses, a summary line
    is emitted.
    """

    def __init__(self, inner: logging.Handler, *, window_s: float = 5.0):
        super().__init__(inner.level)
        self.inner = inner
        self.window_s = window_s
        self._last_key: Optional[tuple[int, str]] = None
        self._last_ts: float = 0.0
        self._count: int = 0
        self._first_ts: float = 0.0

    def setFormatter(self, fmt: logging.Formatter) -> None:
        super().setFormatter(fmt)
        self.inner.setFormatter(fmt)

    def _emit_summary_if_needed(self, now: float) -> None:
        if not self._last_key or self._count <= 1:
            return
        levelno, line = self._last_key
        dur = max(0.0, now - self._first_ts)
        summary = f"(repeated {self._count - 1}x over {dur:.1f}s) {line}"
        rec = logging.LogRecord(
            name=logger.name,
            level=levelno,
            pathname="",
            lineno=0,
            msg=summary,
            args=(),
            exc_info=None,
        )
        rec.created = now
        self.inner.handle(rec)
        self._count = 0
        self._last_key = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            now = time.time()
            line = self.format(record)
            key = (record.levelno, line)

            if self._last_key == key:
                # Same line as previous.
                if self._count == 0:
                    self._first_ts = now
                    self._count = 1

                if now - self._last_ts <= self.window_s:
                    self._count += 1
                    self._last_ts = now
                    return

                # Window elapsed: flush summary and start counting again.
                self._emit_summary_if_needed(now)
                self._last_key = key
                self._first_ts = now
                self._last_ts = now
                self._count = 1
                self.inner.emit(record)
                return

            # Line changed: flush any pending summary.
            self._emit_summary_if_needed(now)
            self._last_key = key
            self._first_ts = now
            self._last_ts = now
            self._count = 1
            self.inner.emit(record)
        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        self._emit_summary_if_needed(time.time())
        try:
            self.inner.flush()
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.flush()
        finally:
            try:
                self.inner.close()
            finally:
                super().close()


def setup_logging(
    log_file: Optional[str],
    *,
    debug: bool = False,
    dedup_window_s: float = 5.0,
    enable_csv: bool = True,
    csv_file: Optional[str] = None,
) -> None:
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S"
    )

    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    csv_logger.setLevel(logging.INFO)

    # Avoid duplicate handlers on re-entry.
    logger.handlers.clear()
    csv_logger.handlers.clear()

    # Console handler (dedup by default)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(DedupingHandler(ch, window_s=dedup_window_s))

    # File handler (dedup as well)
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        fh = TimestampedRotatingFileHandler(
            log_file,
            maxBytes=2 * 1024 * 1024,  # smaller chunks for shipping
            backupCount=0,
            encoding="utf-8",
        )
        fh.setFormatter(formatter)
        logger.addHandler(DedupingHandler(fh, window_s=dedup_window_s))

        if enable_csv:
            csv_path = csv_file or (
                log_file.replace(".log", ".csv") if log_file.endswith(".log") else f"{log_file}.csv"
            )
            csv_handler = TimestampedRotatingFileHandler(
                csv_path,
                maxBytes=2 * 1024 * 1024,
                backupCount=0,
                encoding="utf-8",
            )
            csv_handler.setFormatter(logging.Formatter("%(message)s"))
            csv_logger.addHandler(csv_handler)

            try:
                if os.path.exists(csv_path) and os.stat(csv_path).st_size == 0:
                    csv_logger.info(
                        "Timestamp,Client,TransactionID,Function,Address,RegisterName,RawHex,DecimalValue,ScaledValue,Unit"
                    )
            except OSError:
                pass

    # Ensure pending dedup summaries flush on exit.
    atexit.register(lambda: [h.flush() for h in logger.handlers if hasattr(h, "flush")])


def check_log_size(log_dir: str, *, max_size_mb: int = 200, max_files: Optional[int] = 50) -> None:
    """Enforces log directory retention by size and file count (oldest-first)."""
    try:
        files: list[tuple[str, float, int]] = []
        total_size = 0
        for name in os.listdir(log_dir):
            fp = os.path.join(log_dir, name)
            if not os.path.isfile(fp):
                continue
            try:
                st = os.stat(fp)
            except OSError:
                continue
            files.append((fp, st.st_mtime, st.st_size))
            total_size += st.st_size

        files.sort(key=lambda x: x[1])

        if max_files is not None and max_files > 0 and len(files) > max_files:
            to_delete = len(files) - max_files
            for fp, _, _ in files[:to_delete]:
                try:
                    os.remove(fp)
                except OSError:
                    pass
            files = files[to_delete:]
            total_size = sum(s for _, _, s in files)

        limit_bytes = max_size_mb * 1024 * 1024
        if total_size <= limit_bytes:
            return

        deleted = 0
        for fp, _, size in files:
            try:
                os.remove(fp)
                deleted += size
            except OSError:
                pass
            if total_size - deleted <= limit_bytes:
                break

    except Exception as e:
        logger.error(f"Error during log cleanup: {e}")


def hex_dump(data: bytes) -> str:
    return " ".join(f"{b:02X}" for b in data)


class _UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate mapping keys (prevents silent overwrites)."""

    def construct_mapping(self, node, deep=False):  # type: ignore[override]
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError(f"Duplicate key in YAML mapping: {key!r}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.load(f, Loader=_UniqueKeyLoader)
    return data or {}


def _merge_register_maps(
    base: dict[Optional[int], dict[int, dict[str, Any]]],
    other: dict[Optional[int], dict[int, dict[str, Any]]],
    *,
    warn_overrides: bool = True,
) -> dict[Optional[int], dict[int, dict[str, Any]]]:
    out: dict[Optional[int], dict[int, dict[str, Any]]] = {
        None: dict(base.get(None, {})),
        3: dict(base.get(3, {})),
        4: dict(base.get(4, {})),
    }
    for func, mapping in other.items():
        if func not in out:
            out[func] = {}
        for addr, details in mapping.items():
            if warn_overrides and addr in out[func]:
                logger.warning(f"Map override: FC{func or 0:02d} addr {addr} overwritten")
            out[func][addr] = details
    return out


def _normalize_map_config(config: dict[str, Any]) -> dict[str, Any]:
    """Normalizes legacy and new map schemas into a common internal shape."""
    maps_by_func: dict[Optional[int], dict[int, dict[str, Any]]] = {None: {}, 3: {}, 4: {}}

    if "input_registers" in config or "holding_registers" in config:
        for addr, details in (config.get("input_registers") or {}).get("registers", {}).items():
            maps_by_func[4][int(addr)] = details
        for addr, details in (config.get("holding_registers") or {}).get("registers", {}).items():
            maps_by_func[3][int(addr)] = details
    else:
        for addr, details in (config.get("registers") or {}).items():
            maps_by_func[None][int(addr)] = details

    return {
        "name": config.get("name"),
        "byte_order": (config.get("byte_order") or "big").lower(),
        "word_order": (config.get("word_order") or "big").lower(),
        "maps_by_func": maps_by_func,
    }


def _endianness_normalize(raw_bytes: bytes, *, byte_order: str, word_order: str) -> bytes:
    """Reorders raw bytes so struct.unpack('>') works consistently."""
    byte_order = (byte_order or "big").lower()
    word_order = (word_order or "big").lower()

    if len(raw_bytes) == 2:
        return raw_bytes[::-1] if byte_order == "little" else raw_bytes

    if len(raw_bytes) == 4:
        w0, w1 = raw_bytes[:2], raw_bytes[2:4]
        if byte_order == "little":
            w0, w1 = w0[::-1], w1[::-1]
        if word_order in ("little", "swap"):
            w0, w1 = w1, w0
        return w0 + w1

    return raw_bytes


class ModbusMapper:
    """Loads Modbus register maps and decodes register blocks for better logs.

    Goals:
    - Make maps easy to extend and compose (supports `include:`).
    - Support FC03 and FC04 overlapping addresses.
    - Fail fast on duplicate YAML keys (within a file).
    """

    def __init__(self, yaml_path: Optional[str] = None):
        self.device_name: Optional[str] = None
        self.byte_order: str = "big"
        self.word_order: str = "big"
        self.maps_by_func: dict[Optional[int], dict[int, dict[str, Any]]] = {None: {}, 3: {}, 4: {}}

        if yaml_path:
            self.load(yaml_path)

    def load(self, yaml_path: str) -> None:
        if not os.path.exists(yaml_path):
            logger.warning(f"Map file not found: {yaml_path}")
            return

        base_dir = os.path.dirname(os.path.abspath(yaml_path))
        seen: set[str] = set()

        def load_one(path: str) -> dict[str, Any]:
            ap = os.path.abspath(path)
            if ap in seen:
                raise ValueError(f"Map include cycle detected at {ap}")
            seen.add(ap)
            cfg = _load_yaml(ap)

            merged_cfg = dict(cfg)
            includes = cfg.get("include") or []
            if isinstance(includes, str):
                includes = [includes]

            merged_maps: dict[Optional[int], dict[int, dict[str, Any]]] = {None: {}, 3: {}, 4: {}}
            for inc in includes:
                inc_path = inc
                if not os.path.isabs(inc_path):
                    inc_path = os.path.join(os.path.dirname(ap), inc_path)
                inc_cfg = load_one(inc_path)
                inc_norm = _normalize_map_config(inc_cfg)
                merged_maps = _merge_register_maps(merged_maps, inc_norm["maps_by_func"], warn_overrides=True)

            # Finally merge this file on top
            norm = _normalize_map_config(merged_cfg)
            merged_maps = _merge_register_maps(merged_maps, norm["maps_by_func"], warn_overrides=True)

            merged_cfg["_normalized"] = {
                "name": norm["name"],
                "byte_order": norm["byte_order"],
                "word_order": norm["word_order"],
                "maps_by_func": merged_maps,
            }
            return merged_cfg

        cfg = load_one(os.path.join(base_dir, os.path.basename(yaml_path)))
        norm2 = cfg.get("_normalized") or {}

        self.device_name = norm2.get("name")
        self.byte_order = norm2.get("byte_order") or "big"
        self.word_order = norm2.get("word_order") or "big"
        self.maps_by_func = norm2.get("maps_by_func") or {None: {}, 3: {}, 4: {}}

        fc3 = len(self.maps_by_func.get(3, {}))
        fc4 = len(self.maps_by_func.get(4, {}))
        legacy = len(self.maps_by_func.get(None, {}))
        logger.info(
            f"Loaded map {yaml_path} ({self.device_name or 'unknown device'}): "
            f"FC03={fc3}, FC04={fc4}, legacy={legacy}"
        )

    def get_info(self, addr: int, func_code: Optional[int] = None) -> Optional[dict[str, Any]]:
        if func_code in self.maps_by_func and addr in self.maps_by_func[func_code]:
            return self.maps_by_func[func_code].get(addr)
        return self.maps_by_func.get(None, {}).get(addr)

    def parse_block(self, start_addr: int, data_bytes: bytes, *, func_code: Optional[int] = None) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        total_registers = len(data_bytes) // 2

        i = 0
        while i < total_registers:
            addr = start_addr + i
            reg_def = self.get_info(addr, func_code=func_code)

            dtype = (reg_def or {}).get("type", "U16")
            name = (reg_def or {}).get("name", "Unknown")
            scale = (reg_def or {}).get("scale", 1)
            unit = (reg_def or {}).get("unit", "")

            try:
                if dtype in ("U32", "S32", "F32"):
                    if i + 1 >= total_registers:
                        i += 1
                        continue
                    raw_bytes = data_bytes[i * 2 : i * 2 + 4]
                    consumed = 2
                else:
                    raw_bytes = data_bytes[i * 2 : i * 2 + 2]
                    consumed = 1

                raw_norm = _endianness_normalize(raw_bytes, byte_order=self.byte_order, word_order=self.word_order)

                if dtype == "U32":
                    val_raw = struct.unpack(">I", raw_norm)[0]
                elif dtype == "S32":
                    val_raw = struct.unpack(">i", raw_norm)[0]
                elif dtype == "S16":
                    val_raw = struct.unpack(">h", raw_norm)[0]
                elif dtype == "F32":
                    val_raw = struct.unpack(">f", raw_norm)[0]
                else:
                    val_raw = struct.unpack(">H", raw_norm)[0]

                val_scaled = val_raw * scale
                val_str = f"{val_scaled:.2f}" if isinstance(val_scaled, float) else str(val_scaled)

                if reg_def:
                    txt = f"{Fore.CYAN}{name}{Style.RESET_ALL}: {val_str}{unit}"
                else:
                    txt = f"Reg {addr}: {val_raw}"

                parsed.append(
                    {
                        "text": txt,
                        "csv": {
                            "addr": addr,
                            "name": name,
                            "hex": hex_dump(raw_bytes),
                            "dec": val_raw,
                            "scaled": val_scaled,
                            "unit": unit,
                        },
                    }
                )
                i += consumed
            except Exception as e:
                logger.debug(f"Error parsing addr {addr}: {e}")
                i += 1

        return parsed


# Global mapper (initialized in main())
mapper: ModbusMapper = ModbusMapper()


@dataclass
class Stats:
    start_ts: float = time.time()
    active_clients: int = 0
    total_clients: int = 0
    requests: int = 0
    responses: int = 0
    upstream_errors: int = 0
    blocked_writes: int = 0
    total_latency_ms: float = 0.0
    max_latency_ms: float = 0.0

    def observe_latency(self, ms: float) -> None:
        self.total_latency_ms += ms
        self.max_latency_ms = max(self.max_latency_ms, ms)


stats = Stats()


@dataclass
class HttpContext:
    upstream: "SharedUpstream"
    map_path: Optional[str]
    upload_path: str
    max_frame_bytes: int


def build_exception_packet(req: bytes, exception_code: int) -> Optional[bytes]:
    """Constructs a Modbus Exception response for a given request frame."""
    try:
        tid = req[0:2]
        unit_id = req[6]
        func_code = req[7]
        header = tid + b"\x00\x00" + b"\x00\x03"  # unit + func + code
        pdu = bytes([unit_id, func_code | 0x80, exception_code])
        return header + pdu
    except Exception:
        return None


def decode_packet(data: bytes, *, direction: str, context: Optional[dict[str, Any]] = None, verbose: bool = False):
    if not data:
        return "Empty Packet", None
    if len(data) < 8:
        return f"Short: {hex_dump(data)}", None

    trans_id = struct.unpack(">H", data[0:2])[0]
    func_code = data[7]
    pdu = data[8:]
    base = f"TID:{trans_id:04X}"

    if func_code & 0x80:
        exc = pdu[0] if len(pdu) >= 1 else None
        if exc is None:
            return f"{Fore.RED}{base} EXCEPTION{Style.RESET_ALL}", None
        return f"{Fore.RED}{base} EXC:{exc:02X}{Style.RESET_ALL}", None

    if direction == "req":
        if func_code in (3, 4) and len(pdu) >= 4:
            start, count = struct.unpack(">HH", pdu[0:4])
            return (
                f"{base} {Fore.BLUE}READ{Style.RESET_ALL} FC{func_code:02d} @ {start} (Qty {count})",
                {"addr": start, "count": count, "func": func_code},
            )
        if func_code == 6 and len(pdu) >= 4:
            addr, val = struct.unpack(">HH", pdu[0:4])
            msg = f"{base} {Fore.YELLOW}WRITE{Style.RESET_ALL} FC06 @ {addr} = 0x{val:04X}"
            if verbose:
                msg += f" (raw={hex_dump(pdu)})"
            return msg, {"addr": addr, "count": 1, "func": func_code}
        if func_code == 16 and len(pdu) >= 4:
            addr, qty = struct.unpack(">HH", pdu[0:4])
            msg = f"{base} {Fore.YELLOW}WRITE{Style.RESET_ALL} FC16 @ {addr} (Qty {qty})"
            if verbose:
                msg += f" (raw={hex_dump(pdu)})"
            return msg, {"addr": addr, "count": qty, "func": func_code}

        if verbose:
            return f"{base} Func:{func_code:02X} Raw:{hex_dump(pdu)}", None
        return f"{base} Func:{func_code:02X}", None

    # response
    if func_code in (3, 4) and context and len(pdu) >= 1:
        data_part = pdu[1:]
        parsed = mapper.parse_block(context["addr"], data_part, func_code=context.get("func"))
        if not parsed:
            return f"{base} {Fore.GREEN}DATA{Style.RESET_ALL}: (no parsed values)", []
        display = " | ".join(p["text"] for p in parsed)
        return f"{base} {Fore.GREEN}DATA{Style.RESET_ALL}: [{display}]", [p["csv"] for p in parsed]

    if func_code in (6, 16) and context:
        return f"{base} {Fore.GREEN}ACK{Style.RESET_ALL} FC{func_code:02d} @ {context.get('addr')}", None

    if verbose:
        return f"{base} Func:{func_code:02X} Raw:{hex_dump(pdu)}", None
    return f"{base} Func:{func_code:02X}", None


async def read_frame(reader: asyncio.StreamReader, *, max_frame_bytes: int = 512) -> Optional[bytes]:
    """Reads a Modbus TCP frame (MBAP header + body)."""
    try:
        header = await reader.readexactly(6)
        length = struct.unpack(">H", header[4:6])[0]
        if length <= 0 or length > max_frame_bytes:
            return None
        body = await reader.readexactly(length)
        return header + body
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
        return None
    except Exception as e:
        logger.debug(f"Frame read error: {e}")
        return None


class SharedUpstream:
    def __init__(self, host: str, port: int, *, timeout_s: float, max_retries: int, max_frame_bytes: int):
        self.host = host
        self.port = port
        self.timeout_s = timeout_s
        self.max_retries = max(0, max_retries)
        self.max_frame_bytes = max_frame_bytes
        self.lock = asyncio.Lock()
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def exchange(self, req: bytes) -> bytes:
        async with self.lock:
            for attempt in range(1 + self.max_retries):
                try:
                    if not self.writer or self.writer.is_closing() or not self.reader:
                        self.reader, self.writer = await asyncio.wait_for(
                            asyncio.open_connection(self.host, self.port), timeout=self.timeout_s
                        )
                        logger.info(f"Connected to upstream {self.host}:{self.port}")

                    self.writer.write(req)
                    await self.writer.drain()

                    res = await asyncio.wait_for(
                        read_frame(self.reader, max_frame_bytes=self.max_frame_bytes), timeout=self.timeout_s
                    )
                    if not res:
                        raise asyncio.TimeoutError("no response")
                    return res
                except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
                    logger.debug(f"Upstream failed attempt {attempt + 1}/{1 + self.max_retries}: {e}")
                    if self.writer:
                        try:
                            self.writer.close()
                            await self.writer.wait_closed()
                        except Exception:
                            pass
                    self.reader = None
                    self.writer = None
                    if attempt >= self.max_retries:
                        raise
                    await asyncio.sleep(0.2)

        raise RuntimeError("unreachable")

    async def check_connection(self) -> bool:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout_s
            )
            w.close()
            await w.wait_closed()
            return True
        except Exception:
            return False

    async def close(self) -> None:
        async with self.lock:
            if self.writer:
                try:
                    self.writer.close()
                    await self.writer.wait_closed()
                except Exception:
                    pass
            self.reader = None
            self.writer = None


def _http_response(status: int, body: str, content_type: str = "text/plain; charset=utf-8") -> bytes:
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed", 500: "Error"}.get(
        status, "OK"
    )
    body_bytes = body.encode("utf-8")
    headers = [
        f"HTTP/1.1 {status} {reason}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body_bytes)}",
        "Connection: close",
        "",
        "",
    ]
    return ("\r\n".join(headers)).encode("utf-8") + body_bytes


def _json_response(status: int, payload: dict[str, Any]) -> bytes:
    return _http_response(status, json.dumps(payload), content_type="application/json")


def _build_read_request(unit: int, func_code: int, start: int, qty: int, *, tid: Optional[int] = None) -> bytes:
    tx_id = tid if tid is not None else int(time.time() * 1000) & 0xFFFF
    pdu = bytes([func_code]) + struct.pack(">HH", start, qty)
    mbap = struct.pack(">HHH", tx_id, 0, len(pdu) + 1)
    return mbap + bytes([unit]) + pdu


async def _poll_block(
    *,
    context: HttpContext,
    func_code: int,
    start: int,
    quantity: int,
    unit: int,
) -> dict[str, Any]:
    req = _build_read_request(unit, func_code, start, quantity)
    res = await context.upstream.exchange(req)

    if len(res) < 9:
        raise ValueError("malformed response from upstream")

    res_tid = struct.unpack(">H", res[0:2])[0]
    res_func = res[7]
    if res_func & 0x80:
        code = res[8]
        raise ValueError(f"upstream exception code {code}")

    byte_count = res[8]
    data = res[9 : 9 + byte_count]
    parsed = mapper.parse_block(start, data, func_code=func_code)

    return {
        "transaction_id": res_tid,
        "function": res_func,
        "start": start,
        "quantity": quantity,
        "unit": unit,
        "byte_count": byte_count,
        "raw_hex": hex_dump(res),
        "parsed": [
            {
                "address": p["csv"]["addr"],
                "name": p["csv"]["name"],
                "hex": p["csv"]["hex"],
                "dec": p["csv"]["dec"],
                "scaled": p["csv"]["scaled"],
                "unit": p["csv"]["unit"],
                "text": p["text"],
            }
            for p in parsed
        ],
    }


def _mapper_snapshot() -> dict[str, Any]:
    def _format_func(func_code: Optional[int]) -> list[dict[str, Any]]:
        regs: list[dict[str, Any]] = []
        for addr in sorted(mapper.maps_by_func.get(func_code, {})):
            details = mapper.maps_by_func.get(func_code, {}).get(addr) or {}
            regs.append(
                {
                    "address": addr,
                    "name": details.get("name", ""),
                    "type": details.get("type", "U16"),
                    "scale": details.get("scale", 1),
                    "unit": details.get("unit", ""),
                    "function": func_code,
                }
            )
        return regs

    return {
        "device_name": mapper.device_name,
        "byte_order": mapper.byte_order,
        "word_order": mapper.word_order,
        "maps": {
            "legacy": _format_func(None),
            "fc03": _format_func(3),
            "fc04": _format_func(4),
        },
    }


INDEX_HTML = """
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>Modbus Explorer</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; background: #0b1221; color: #e2e8f0; }
    h1 { color: #7bdcb5; }
    h2 { margin-top: 28px; color: #9cdcfe; }
    table { border-collapse: collapse; width: 100%; margin-top: 8px; }
    th, td { border: 1px solid #2d3748; padding: 8px; }
    th { background: #1f2937; }
    tr:nth-child(even) { background: #111827; }
    button { background: #2563eb; color: white; border: none; padding: 6px 10px; cursor: pointer; border-radius: 4px; }
    button:hover { background: #1d4ed8; }
    .meta { display: flex; gap: 12px; flex-wrap: wrap; }
    .meta div { background: #111827; padding: 8px 12px; border-radius: 6px; border: 1px solid #1f2937; }
    #poll-output { white-space: pre-wrap; background: #0f172a; padding: 12px; border-radius: 6px; border: 1px solid #1f2937; margin-top: 12px; }
    #upload-area { margin-top: 20px; display: grid; gap: 8px; max-width: 640px; }
    textarea { width: 100%; min-height: 160px; background: #0f172a; color: #e2e8f0; border: 1px solid #1f2937; border-radius: 6px; padding: 8px; }
    input[type=\"file\"] { color: #e2e8f0; }
  </style>
</head>
<body>
  <h1>Modbus Explorer</h1>
  <div class=\"meta\" id=\"meta\"></div>
  <div id=\"map-container\"></div>
  <div id=\"poll-output\">Select a register to poll.</div>
  <div id=\"upload-area\">
    <h2>Upload/Replace Map</h2>
    <input type=\"file\" id=\"map-file\" accept=\".yaml,.yml,text/yaml\" />
    <textarea id=\"map-text\" placeholder=\"Or paste YAML here...\"></textarea>
    <button id=\"upload-btn\">Upload map</button>
    <div id=\"upload-status\"></div>
  </div>
  <script>
    const pollOutput = document.getElementById('poll-output');
    const mapContainer = document.getElementById('map-container');
    const meta = document.getElementById('meta');

    function quantityForType(type) {
      return ['U32', 'S32', 'F32'].includes(type) ? 2 : 1;
    }

    function renderMeta(info) {
      meta.innerHTML = '';
      const fields = [
        ['Device', info.device_name || '—'],
        ['Byte order', info.byte_order],
        ['Word order', info.word_order],
        ['Legacy entries', info.maps.legacy.length],
        ['FC03 entries', info.maps.fc03.length],
        ['FC04 entries', info.maps.fc04.length],
      ];
      fields.forEach(([label, value]) => {
        const div = document.createElement('div');
        div.textContent = `${label}: ${value}`;
        meta.appendChild(div);
      });
    }

    function makeTable(title, rows) {
      const container = document.createElement('div');
      const h2 = document.createElement('h2');
      h2.textContent = title;
      container.appendChild(h2);
      const table = document.createElement('table');
      table.innerHTML = '<tr><th>Address</th><th>Name</th><th>Type</th><th>Scale</th><th>Unit</th><th>Action</th></tr>';
      rows.forEach(row => {
        const tr = document.createElement('tr');
        const cells = [row.address, row.name || '—', row.type || 'U16', row.scale ?? 1, row.unit || ''];
        cells.forEach(val => {
          const td = document.createElement('td');
          td.textContent = val;
          tr.appendChild(td);
        });
        const action = document.createElement('td');
        const btn = document.createElement('button');
        btn.textContent = 'Poll now';
        btn.onclick = () => {
          const funcCode = row.function === null ? 4 : row.function;
          pollRegister(row.address, funcCode, row.type || 'U16');
        };
        action.appendChild(btn);
        tr.appendChild(action);
        table.appendChild(tr);
      });
      container.appendChild(table);
      return container;
    }

    async function pollRegister(address, funcCode, type) {
      pollOutput.textContent = 'Polling...';
      try {
        const res = await fetch('/api/poll', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ function: funcCode, start: address, quantity: quantityForType(type) }),
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data.error || 'poll failed');
        }
        const lines = [
          `Transaction: ${data.transaction_id} | Func: ${data.function} | Start: ${data.start} | Qty: ${data.quantity} | Unit: ${data.unit}`,
          `Raw: ${data.raw_hex}`,
        ];
        if (Array.isArray(data.parsed)) {
          data.parsed.forEach(p => lines.push(p.text));
        }
        pollOutput.textContent = lines.join('\n');
      } catch (err) {
        pollOutput.textContent = 'Error: ' + err.message;
      }
    }

    async function loadMap() {
      const res = await fetch('/api/map');
      const data = await res.json();
      renderMeta(data);
      mapContainer.innerHTML = '';
      mapContainer.appendChild(makeTable('Legacy (FC04 fallback)', data.maps.legacy));
      mapContainer.appendChild(makeTable('FC03 Holding Registers', data.maps.fc03));
      mapContainer.appendChild(makeTable('FC04 Input Registers', data.maps.fc04));
    }

    async function uploadMap() {
      const fileInput = document.getElementById('map-file');
      const textArea = document.getElementById('map-text');
      const status = document.getElementById('upload-status');
      let payload = textArea.value.trim();
      if (!payload && fileInput.files.length) {
        payload = await fileInput.files[0].text();
      }
      if (!payload) {
        status.textContent = 'Please select a file or paste YAML first.';
        return;
      }
      status.textContent = 'Uploading...';
      const res = await fetch('/api/map/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ yaml: payload }),
      });
      const data = await res.json();
      if (!res.ok) {
        status.textContent = 'Upload failed: ' + (data.error || 'unknown error');
        return;
      }
      status.textContent = data.message || 'Map updated';
      textArea.value = '';
      fileInput.value = '';
      await loadMap();
    }

    document.getElementById('upload-btn').onclick = uploadMap;
    loadMap();
  </script>
</body>
</html>
"""


async def handle_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, context: HttpContext) -> None:
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        if not data:
            writer.close()
            return

        if b"\r\n\r\n" in data:
            head, body = data.split(b"\r\n\r\n", 1)
        else:
            head, body = data, b""

        header_lines = head.decode("utf-8", errors="replace").split("\r\n")
        request_line = header_lines[0] if header_lines else ""
        parts = request_line.split()
        if len(parts) < 2:
            writer.write(_http_response(400, "bad request\n"))
            await writer.drain()
            writer.close()
            return
        method, raw_path = parts[0], parts[1]
        headers: dict[str, str] = {}
        for line in header_lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        content_length = int(headers.get("content-length", "0") or 0)
        while len(body) < content_length:
            chunk = await asyncio.wait_for(reader.read(content_length - len(body)), timeout=2.0)
            if not chunk:
                break
            body += chunk

        parsed_url = urlparse(raw_path)
        path = parsed_url.path

        uptime_s = max(0.0, time.time() - stats.start_ts)
        avg_latency = (stats.total_latency_ms / stats.responses) if stats.responses else 0.0

        if method == "GET" and path in ("/health", "/healthz"):
            writer.write(_http_response(200, "ok\n"))
        elif method == "GET" and path in ("/metrics", "/stats"):
            body_text = (
                f"uptime_seconds {uptime_s:.0f}\n"
                f"active_clients {stats.active_clients}\n"
                f"total_clients {stats.total_clients}\n"
                f"requests_total {stats.requests}\n"
                f"responses_total {stats.responses}\n"
                f"upstream_errors_total {stats.upstream_errors}\n"
                f"blocked_writes_total {stats.blocked_writes}\n"
                f"avg_latency_ms {avg_latency:.2f}\n"
                f"max_latency_ms {stats.max_latency_ms:.2f}\n"
            )
            writer.write(_http_response(200, body_text))
        elif method == "GET" and path == "/api/map":
            writer.write(_json_response(200, _mapper_snapshot()))
        elif method == "POST" and path == "/api/poll":
            try:
                payload = json.loads(body.decode("utf-8")) if body else {}
                func_code = int(payload.get("function"))
                start = int(payload.get("start"))
                qty = int(payload.get("quantity") or 1)
                unit = int(payload.get("unit") or 1)
                if func_code not in (3, 4):
                    raise ValueError("function must be 3 or 4")
                if qty <= 0:
                    raise ValueError("quantity must be positive")
                result = await _poll_block(context=context, func_code=func_code, start=start, quantity=qty, unit=unit)
                writer.write(_json_response(200, result))
            except Exception as e:
                writer.write(_json_response(400, {"error": str(e)}))
        elif method == "POST" and path == "/api/map/upload":
            try:
                if headers.get("content-type", "").startswith("application/json"):
                    payload = json.loads(body.decode("utf-8")) if body else {}
                    yaml_text = payload.get("yaml") or ""
                else:
                    yaml_text = body.decode("utf-8") if body else ""
                if not yaml_text.strip():
                    raise ValueError("no YAML provided")
                tmp_path = f"{context.upload_path}.tmp"
                os.makedirs(os.path.dirname(context.upload_path) or ".", exist_ok=True)
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write(yaml_text)

                # Validate with duplicate-key protection before swapping in globally.
                try:
                    _ = ModbusMapper(tmp_path)
                except Exception:
                    os.remove(tmp_path)
                    raise

                os.replace(tmp_path, context.upload_path)
                mapper.load(context.upload_path)
                context.map_path = context.upload_path
                writer.write(_json_response(200, {"message": "Map uploaded and reloaded."}))
            except Exception as e:
                writer.write(_json_response(400, {"error": str(e)}))
        elif method == "GET" and path == "/":
            writer.write(_http_response(200, INDEX_HTML, content_type="text/html; charset=utf-8"))
        else:
            writer.write(_http_response(404, "not found\n"))

        await writer.drain()
    except Exception:
        try:
            writer.write(_http_response(500, "error\n"))
            await writer.drain()
        except Exception:
            pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    upstream: SharedUpstream,
    log_dir: Optional[str],
    allow_write: bool,
    verbose: bool,
    max_log_dir_mb: int,
    max_log_files: int,
    max_frame_bytes: int,
    csv_enabled: bool,
) -> None:
    peer = writer.get_extra_info("peername")
    client_ip = peer[0] if peer else "unknown"
    logger.info(f"Client connected: {client_ip}")

    stats.active_clients += 1
    stats.total_clients += 1

    last_log_check = 0.0

    try:
        while True:
            now = time.time()
            if log_dir and (now - last_log_check) > 60:
                last_log_check = now
                check_log_size(log_dir, max_size_mb=max_log_dir_mb, max_files=max_log_files)

            req = await read_frame(reader, max_frame_bytes=max_frame_bytes)
            if not req:
                break

            req_str, ctx = decode_packet(req, direction="req", verbose=verbose)
            logger.info(f"[{client_ip}] --> {req_str}")
            stats.requests += 1

            # Safety default: block writes unless explicitly allowed.
            if not allow_write and len(req) >= 8 and req[7] in (6, 16):
                stats.blocked_writes += 1
                exc = build_exception_packet(req, 0x01)  # Illegal Function
                if exc:
                    writer.write(exc)
                    await writer.drain()
                    logger.warning(f"[{client_ip}] <-- BLOCKED write (read-only mode)")
                    continue
                break

            t0 = time.time()
            try:
                res = await upstream.exchange(req)
            except Exception as e:
                stats.upstream_errors += 1
                logger.warning(f"[{client_ip}] Upstream error: {e}")
                res = b""
            latency_ms = (time.time() - t0) * 1000.0

            if not res:
                exc = build_exception_packet(req, 0x0B)  # Gateway Target Device Failed to Respond
                if exc:
                    writer.write(exc)
                    await writer.drain()
                    logger.warning(f"[{client_ip}] <-- EXCEPTION (Gateway Timeout)")
                    continue
                break

            res_str, csv_rows = decode_packet(res, direction="res", context=ctx, verbose=verbose)
            logger.info(f"[{client_ip}] <-- {res_str} ({latency_ms:.1f}ms)")

            stats.responses += 1
            stats.observe_latency(latency_ms)

            if csv_enabled and csv_rows:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                tid = struct.unpack(">H", req[0:2])[0]
                func = ctx.get("func") if isinstance(ctx, dict) else None
                func_str = f"{int(func):02d}" if func is not None else "??"
                for row in csv_rows:
                    csv_logger.info(
                        f"{ts},{client_ip},{tid:04X},{func_str},{row['addr']},{row['name']},"
                        f"{row['hex']},{row['dec']},{row['scaled']},{row['unit']}"
                    )

            writer.write(res)
            await writer.drain()

    except Exception as e:
        logger.error(f"[{client_ip}] Handler error: {e}")
    finally:
        stats.active_clients = max(0, stats.active_clients - 1)
        try:
            writer.close()
        except Exception:
            pass
        logger.info(f"Client disconnected: {client_ip}")


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Modbus TCP proxy debugger")
    parser.add_argument("--bind", "-b", default=os.environ.get("MODBUS_PROXY_BIND", "0.0.0.0:5020"))
    parser.add_argument("--target", "-t", default=os.environ.get("MODBUS_PROXY_TARGET"))
    parser.add_argument("--map", "-m", default=os.environ.get("MODBUS_PROXY_MAP"), help="Path to YAML register map")
    parser.add_argument("--log", "-l", default=os.environ.get("MODBUS_PROXY_LOG", "logs/modbus.log"))

    parser.add_argument("--verbose", action="store_true", help="Verbose logging (includes raw bytes for unknown packets)")
    parser.add_argument("--debug", action="store_true", help="Enable debug-level internal logs")

    parser.add_argument("--timeout", type=float, default=float(os.environ.get("MODBUS_PROXY_TIMEOUT", "10.0")))
    parser.add_argument("--max-retries", type=int, default=int(os.environ.get("MODBUS_PROXY_MAX_RETRIES", "1")))
    parser.add_argument(
        "--max-frame-bytes", type=int, default=int(os.environ.get("MODBUS_PROXY_MAX_FRAME_BYTES", "512"))
    )

    parser.add_argument(
        "--allow-write",
        action="store_true",
        help="Allow FC06/FC16 to reach upstream (default: block writes)",
    )

    parser.add_argument("--dedup-window", type=float, default=float(os.environ.get("MODBUS_PROXY_DEDUP_WINDOW", "5")))
    parser.add_argument("--max-log-dir-mb", type=int, default=int(os.environ.get("MODBUS_PROXY_MAX_LOG_DIR_MB", "200")))
    parser.add_argument("--max-log-files", type=int, default=int(os.environ.get("MODBUS_PROXY_MAX_LOG_FILES", "50")))

    parser.add_argument(
        "--http",
        default=os.environ.get("MODBUS_PROXY_HTTP"),
        help="Optional HTTP bind for /health and /metrics (e.g. 127.0.0.1:8080)",
    )

    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Disable CSV export (reduces disk usage)",
    )
    parser.add_argument(
        "--csv-file",
        default=os.environ.get("MODBUS_PROXY_CSV_FILE"),
        help="Optional explicit CSV path (defaults to <log>.csv)",
    )

    return parser


async def start_proxy(
    *,
    bind: str,
    target: str,
    map_path: Optional[str],
    log_file: Optional[str],
    verbose: bool,
    debug: bool,
    allow_write: bool,
    timeout_s: float,
    max_retries: int,
    max_frame_bytes: int,
    dedup_window_s: float,
    max_log_dir_mb: int,
    max_log_files: int,
    http_bind: Optional[str],
    csv_enabled: bool,
    csv_file: Optional[str],
) -> tuple[asyncio.AbstractServer, Optional[asyncio.AbstractServer]]:
    setup_logging(
        log_file,
        debug=debug,
        dedup_window_s=dedup_window_s,
        enable_csv=csv_enabled,
        csv_file=csv_file,
    )

    # map is for logging experience only; proxy works without it.
    global mapper
    mapper = ModbusMapper(map_path)

    host, port_str = target.split(":")
    upstream = SharedUpstream(
        host,
        int(port_str),
        timeout_s=timeout_s,
        max_retries=max_retries,
        max_frame_bytes=max_frame_bytes,
    )

    if not await upstream.check_connection():
        logger.warning(f"Could not connect to upstream {target}. Proxy will still start, but requests may fail.")

    b_host, b_port_str = bind.split(":")
    log_dir = os.path.dirname(log_file) if log_file else None

    upload_path = map_path or os.path.join("maps", "uploaded_map.yaml")
    http_context = HttpContext(
        upstream=upstream,
        map_path=map_path,
        upload_path=upload_path,
        max_frame_bytes=max_frame_bytes,
    )

    server = await asyncio.start_server(
        lambda r, w: handle_client(
            r,
            w,
            upstream=upstream,
            log_dir=log_dir,
            allow_write=allow_write,
            verbose=verbose,
            max_log_dir_mb=max_log_dir_mb,
            max_log_files=max_log_files,
            max_frame_bytes=max_frame_bytes,
            csv_enabled=csv_enabled,
        ),
        b_host,
        int(b_port_str),
    )

    async def _close_upstream_when_server_stops() -> None:
        await server.wait_closed()
        await upstream.close()

    asyncio.create_task(_close_upstream_when_server_stops())

    http_server = None
    if http_bind:
        h_host, h_port_str = http_bind.split(":")
        http_server = await asyncio.start_server(
            lambda r, w: handle_http(r, w, context=http_context), h_host, int(h_port_str)
        )

    logger.info(f"Proxying {bind} -> {target}")
    if map_path:
        logger.info(f"Using map: {map_path}")
    if not allow_write:
        logger.warning("Write blocking enabled (read-only proxy). Use --allow-write to permit FC06/FC16.")
    if http_bind:
        logger.info(
            f"HTTP monitoring on {http_bind} (GET /health, /metrics, / for explorer, /api/map, /api/poll, /api/map/upload)"
        )

    return server, http_server


async def main_async(argv: Optional[list[str]] = None) -> None:
    parser = create_argument_parser()
    args = parser.parse_args(argv)

    if not args.target:
        parser.error("--target is required (or set MODBUS_PROXY_TARGET)")

    server, http_server = await start_proxy(
        bind=args.bind,
        target=args.target,
        map_path=args.map,
        log_file=args.log,
        verbose=bool(args.verbose),
        debug=bool(args.debug),
        allow_write=bool(args.allow_write),
        timeout_s=float(args.timeout),
        max_retries=int(args.max_retries),
        max_frame_bytes=int(args.max_frame_bytes),
        dedup_window_s=float(args.dedup_window),
        max_log_dir_mb=int(args.max_log_dir_mb),
        max_log_files=int(args.max_log_files),
        http_bind=args.http,
        csv_enabled=not bool(args.no_csv),
        csv_file=args.csv_file,
    )

    async with server:
        if http_server:
            async with http_server:
                await asyncio.gather(server.serve_forever(), http_server.serve_forever())
        else:
            await server.serve_forever()


def run() -> None:
    """Entry-point for console script."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
