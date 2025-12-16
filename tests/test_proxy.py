import asyncio
import struct
import sys
from pathlib import Path

import pytest


def build_mbap(tid: int, unit: int, pdu: bytes) -> bytes:
    # length = unit(1) + pdu
    length = 1 + len(pdu)
    return struct.pack(">HHH", tid, 0, length) + bytes([unit]) + pdu


def req_read(tid: int, unit: int, func: int, start: int, qty: int) -> bytes:
    return build_mbap(tid, unit, bytes([func]) + struct.pack(">HH", start, qty))


def req_write_single(tid: int, unit: int, addr: int, value: int) -> bytes:
    return build_mbap(tid, unit, b"\x06" + struct.pack(">HH", addr, value))


def res_exception(req: bytes, code: int) -> bytes:
    tid = req[0:2]
    proto = b"\x00\x00"
    length = b"\x00\x03"
    unit = req[6:7]
    func = bytes([req[7] | 0x80])
    return tid + proto + length + unit + func + bytes([code])


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readexactly(6)
    length = struct.unpack(">H", header[4:6])[0]
    body = await reader.readexactly(length)
    return header + body


async def upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            frame = await read_frame(reader)
            unit = frame[6]
            func = frame[7]
            pdu = frame[8:]
            tid = struct.unpack(">H", frame[0:2])[0]

            if func in (3, 4):
                start, qty = struct.unpack(">HH", pdu[0:4])
                regs = []
                for i in range(qty):
                    regs.append(struct.pack(">H", (start + i) & 0xFFFF))
                data = b"".join(regs)
                resp_pdu = bytes([func, len(data)]) + data
                writer.write(build_mbap(tid, unit, resp_pdu))
                await writer.drain()
            elif func == 6:
                # Echo request as response (unit+func+addr+value)
                resp_pdu = bytes([func]) + pdu[0:4]
                writer.write(build_mbap(tid, unit, resp_pdu))
                await writer.drain()
            elif func == 16:
                # Echo start+qty
                resp_pdu = bytes([func]) + pdu[0:4]
                writer.write(build_mbap(tid, unit, resp_pdu))
                await writer.drain()
            else:
                writer.write(res_exception(frame, 0x01))
                await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_proxy_forwards_read_without_hardware(tmp_path):
    # Start fake upstream Modbus server
    upstream = await asyncio.start_server(upstream_handler, "127.0.0.1", 0)
    upstream_host, upstream_port = upstream.sockets[0].getsockname()[:2]

    # Start proxy
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import main  # noqa: E402

    log_file = str(tmp_path / "modbus.log")
    proxy, _http = await main.start_proxy(
        bind="127.0.0.1:0",
        target=f"{upstream_host}:{upstream_port}",
        map_path=None,
        log_file=log_file,
        verbose=False,
        debug=False,
        allow_write=False,
        timeout_s=2.0,
        max_retries=0,
        max_frame_bytes=512,
        dedup_window_s=0.5,
        max_log_dir_mb=50,
        max_log_files=20,
        http_bind=None,
        csv_enabled=False,
        csv_file=None,
    )
    proxy_host, proxy_port = proxy.sockets[0].getsockname()[:2]

    async with upstream, proxy:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(proxy_host, proxy_port), timeout=2.0)
        req = req_read(tid=0x1234, unit=1, func=4, start=100, qty=2)
        writer.write(req)
        await writer.drain()

        resp = await asyncio.wait_for(read_frame(reader), timeout=2.0)
        # Expect FC04 response with bytecount 4 and register values 100, 101
        assert resp[0:2] == b"\x12\x34"
        assert resp[7] == 4
        assert resp[8] == 4
        assert resp[9:13] == struct.pack(">HH", 100, 101)

        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_proxy_blocks_writes_by_default(tmp_path):
    upstream = await asyncio.start_server(upstream_handler, "127.0.0.1", 0)
    upstream_host, upstream_port = upstream.sockets[0].getsockname()[:2]

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import main  # noqa: E402

    log_file = str(tmp_path / "modbus.log")
    proxy, _http = await main.start_proxy(
        bind="127.0.0.1:0",
        target=f"{upstream_host}:{upstream_port}",
        map_path=None,
        log_file=log_file,
        verbose=False,
        debug=False,
        allow_write=False,
        timeout_s=2.0,
        max_retries=0,
        max_frame_bytes=512,
        dedup_window_s=0.5,
        max_log_dir_mb=50,
        max_log_files=20,
        http_bind=None,
        csv_enabled=False,
        csv_file=None,
    )
    proxy_host, proxy_port = proxy.sockets[0].getsockname()[:2]

    async with upstream, proxy:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(proxy_host, proxy_port), timeout=2.0)
        req = req_write_single(tid=0xBEEF, unit=1, addr=10, value=0x0001)
        writer.write(req)
        await writer.drain()

        resp = await asyncio.wait_for(read_frame(reader), timeout=2.0)
        # Should be exception illegal function (0x01)
        assert resp[0:2] == b"\xBE\xEF"
        assert resp[7] == (6 | 0x80)
        assert resp[8] == 0x01

        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_http_ui_renders(tmp_path):
    upstream = await asyncio.start_server(upstream_handler, "127.0.0.1", 0)
    upstream_host, upstream_port = upstream.sockets[0].getsockname()[:2]

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import main  # noqa: E402

    log_file = str(tmp_path / "modbus.log")
    proxy, http_server = await main.start_proxy(
        bind="127.0.0.1:0",
        target=f"{upstream_host}:{upstream_port}",
        map_path=None,
        log_file=log_file,
        verbose=False,
        debug=False,
        allow_write=False,
        timeout_s=2.0,
        max_retries=0,
        max_frame_bytes=512,
        dedup_window_s=0.2,
        max_log_dir_mb=50,
        max_log_files=20,
        http_bind="127.0.0.1:0",
        csv_enabled=False,
        csv_file=None,
    )
    assert http_server is not None
    http_host, http_port = http_server.sockets[0].getsockname()[:2]

    async with upstream, proxy, http_server:
        r, w = await asyncio.wait_for(asyncio.open_connection(http_host, http_port), timeout=2.0)
        w.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await w.drain()
        data = await asyncio.wait_for(r.read(65536), timeout=2.0)
        w.close()
        await w.wait_closed()

        assert b"200 OK" in data
        assert b"Modbus Proxy Debugger" in data
        assert b"Recent logs" in data

