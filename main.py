import asyncio
import argparse
import logging
import logging.handlers
import sys
import struct
import os
import time
import csv
from datetime import datetime
try:
    import yaml  # Requires pip install pyyaml
except ImportError:
    sys.exit("Error: The 'pyyaml' module is missing. Please install it (e.g., 'pip install pyyaml').")


# Optional: Color output
try:
    from colorama import Fore, Style, init
    init(autoreset=True)
except ImportError:
    class MockColor:
        def __getattr__(self, name): return ""
    Fore = Style = MockColor()

# Configure logging
logger = logging.getLogger("modbus_proxy")
csv_logger = logging.getLogger("csv_data")

def setup_logging(log_file=None, debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(message)s', datefmt='%H:%M:%S')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        # Rotate when file reaches 5MB, keep 10 backups
        fh = logging.handlers.RotatingFileHandler(
            log_file, 
            maxBytes=5*1024*1024, 
            backupCount=10, 
            encoding='utf-8'
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # CSV setup
    csv_file = log_file.replace(".log", ".csv") if log_file else "modbus_data.csv"
    # CSV also needs rotation to prevent massive files
    csv_handler = logging.handlers.RotatingFileHandler(
        csv_file,
        maxBytes=5*1024*1024,
        backupCount=10,
        encoding='utf-8'
    )
    csv_handler.setFormatter(logging.Formatter('%(message)s'))
    csv_logger.setLevel(logging.INFO)
    csv_logger.addHandler(csv_handler)
    # Only write header if file is new/empty
    if os.stat(csv_file).st_size == 0:
        csv_logger.info("Timestamp,Client,TransactionID,Function,Address,RegisterName,RawHex,DecimalValue,ScaledValue,Unit")

def check_log_size(log_dir, max_size_mb=200):
    """Checks total size of log directory and removes oldest files if over limit."""
    try:
        total_size = 0
        files = []
        for f in os.listdir(log_dir):
            fp = os.path.join(log_dir, f)
            if os.path.isfile(fp):
                size = os.path.getsize(fp)
                total_size += size
                files.append((fp, os.path.getmtime(fp), size))
        
        total_mb = total_size / (1024*1024)
        if total_mb > max_size_mb:
            logger.warning(f"Log directory size {total_mb:.2f}MB exceeds limit {max_size_mb}MB. Cleaning up...")
            # Sort by time (oldest first)
            files.sort(key=lambda x: x[1])
            
            deleted_size = 0
            for fp, _, size in files:
                try:
                    os.remove(fp)
                    deleted_size += size
                    if (total_size - deleted_size) / (1024*1024) <= max_size_mb:
                        break
                except OSError as e:
                    logger.error(f"Error deleting {fp}: {e}")
            
            logger.info(f"Cleanup complete. Removed {deleted_size/(1024*1024):.2f}MB")
            
    except Exception as e:
        logger.error(f"Error during log cleanup: {e}")

def hex_dump(data):
    return ' '.join(f'{b:02X}' for b in data)

class ModbusMapper:
    """Handles loading YAML maps and parsing raw bytes into human values."""
    def __init__(self, yaml_path=None):
        self.map = {}
        if yaml_path and os.path.exists(yaml_path):
            try:
                with open(yaml_path, 'r') as f:
                    config = yaml.safe_load(f)
                    # Convert keys to int and normalize structure
                    for addr, details in config.get('registers', {}).items():
                        self.map[int(addr)] = details
                logger.info(f"Loaded register map from {yaml_path} with {len(self.map)} definitions.")
            except Exception as e:
                logger.error(f"Failed to load YAML map: {e}")

    def get_info(self, addr):
        return self.map.get(addr)

    def parse_block(self, start_addr, data_bytes):
        """
        Parses a block of registers. Handles U16, U32, etc.
        Returns a list of parsed dictionaries.
        """
        parsed_results = []
        
        # Total 16-bit registers available in this packet
        total_registers = len(data_bytes) // 2 
        
        i = 0
        while i < total_registers:
            current_addr = start_addr + i
            reg_def = self.map.get(current_addr)
            
            # Default to U16 if not defined
            dtype = reg_def.get('type', 'U16') if reg_def else 'U16'
            name = reg_def.get('name', 'Unknown') if reg_def else 'Unknown'
            scale = reg_def.get('scale', 1) if reg_def else 1
            unit = reg_def.get('unit', '') if reg_def else ''
            
            # Extract raw bytes based on type length
            try:
                if dtype in ['U32', 'S32', 'F32']:
                    # 32-bit types require 2 registers (4 bytes)
                    if i + 1 >= total_registers:
                        # Not enough data for 32-bit, skip
                        i += 1
                        continue
                        
                    raw_bytes = data_bytes[i*2 : (i*2)+4]
                    consumed_registers = 2
                else:
                    # 16-bit types (U16, S16)
                    raw_bytes = data_bytes[i*2 : (i*2)+2]
                    consumed_registers = 1
                
                # Unpack
                val_raw = 0
                if dtype == 'U32':
                    val_raw = struct.unpack('>I', raw_bytes)[0]
                elif dtype == 'S32':
                    val_raw = struct.unpack('>i', raw_bytes)[0]
                elif dtype == 'S16':
                    val_raw = struct.unpack('>h', raw_bytes)[0]
                elif dtype == 'F32':
                    val_raw = struct.unpack('>f', raw_bytes)[0]
                else: # U16
                    val_raw = struct.unpack('>H', raw_bytes)[0]

                # Scale
                val_scaled = val_raw * scale
                
                # Format string
                if isinstance(val_scaled, float):
                    val_str = f"{val_scaled:.2f}"
                else:
                    val_str = str(val_scaled)

                # Only add to results if we have a definition OR if it's strictly unknown
                # We skip the second register of a U32 pair to avoid garbage logs
                if reg_def:
                    txt = f"{Fore.CYAN}{name}{Style.RESET_ALL}: {val_str}{unit}"
                else:
                    txt = f"Reg {current_addr}: {val_raw}"

                parsed_results.append({
                    'text': txt,
                    'csv': {
                        'addr': current_addr,
                        'name': name,
                        'hex': hex_dump(raw_bytes),
                        'dec': val_raw,
                        'scaled': val_scaled,
                        'unit': unit
                    }
                })

                i += consumed_registers

            except Exception as e:
                logger.error(f"Error parsing addr {current_addr}: {e}")
                i += 1
                
        return parsed_results

# Initialize Global Mapper
mapper = None

def decode_packet(data, direction="req", context=None):
    if not data: return "Empty Packet", None
    if len(data) < 8: return f"Short: {hex_dump(data)}", None

    trans_id = struct.unpack('>H', data[0:2])[0]
    func_code = data[7]
    pdu = data[8:]
    base_info = f"TID:{trans_id:04X}"

    if func_code & 0x80:
        return f"{Fore.RED}{base_info} EXCEPTION{Style.RESET_ALL}", None

    if direction == "req":
        if func_code in [3, 4]:
            start, count = struct.unpack('>HH', pdu[0:4])
            return f"{base_info} {Fore.BLUE}READ{Style.RESET_ALL} @ {start} (Qty {count})", {'addr': start, 'count': count}
        elif func_code in [6, 16]:
            start, val = struct.unpack('>HH', pdu[0:4])
            return f"{base_info} {Fore.YELLOW}WRITE{Style.RESET_ALL} @ {start}", None

    elif direction == "res":
        if func_code in [3, 4] and context:
            byte_count = pdu[0]
            data_part = pdu[1:]
            
            # Use the Mapper to parse the block
            parsed_items = mapper.parse_block(context['addr'], data_part)
            
            display_str = " | ".join([p['text'] for p in parsed_items])
            csv_rows = [p['csv'] for p in parsed_items]
            
            return f"{base_info} {Fore.GREEN}DATA{Style.RESET_ALL}: [{display_str}]", csv_rows

    return f"{base_info} Func:{func_code:02X} Raw:{hex_dump(pdu)}", None

async def read_frame(reader):
    try:
        header = await reader.readexactly(6)
        length = struct.unpack('>H', header[4:6])[0]
        if length > 512: return None
        body = await reader.readexactly(length)
        return header + body
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
        return None
    except Exception as e:
        logger.debug(f"Frame read error: {e}")
        return None

class SharedUpstream:
    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.lock = asyncio.Lock()
        self.writer = None
    
    async def exchange(self, req):
        async with self.lock:
            try:
                if not self.writer or self.writer.is_closing():
                    self.reader, self.writer = await asyncio.wait_for(
                        asyncio.open_connection(self.host, self.port), self.timeout)
                self.writer.write(req)
                await self.writer.drain()
                return await asyncio.wait_for(read_frame(self.reader), self.timeout)
            except Exception as e:
                # If we lose connection during exchange, clear the writer so next request reconnects
                logger.warning(f"Upstream connection failed during exchange: {e}")
                self.writer = None
                raise e

    async def check_connection(self):
        """Proactively checks if upstream is reachable."""
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), self.timeout)
            w.close()
            await w.wait_closed()
            return True
        except Exception:
            return False

async def handle_client(reader, writer, upstream, log_dir):
    client_ip = writer.get_extra_info('peername')[0]
    logger.info(f"Client connected: {client_ip}")
    try:
        while True:
            # Check log size periodically (simple probability check to avoid excessive IO)
            if log_dir and time.time() % 300 < 1: # Check roughly every 5 mins
                check_log_size(log_dir)

            req = await read_frame(reader)
            if not req:
                logger.info(f"[{client_ip}] Client sent FIN/EOF (closed connection).")
                break
            
            req_str, ctx = decode_packet(req, "req")
            logger.info(f"[{client_ip}] --> {req_str}")
            
            res = await upstream.exchange(req)
            
            if not res:
                logger.warning(f"[{client_ip}] Upstream disconnected while waiting for response. Closing client.")
                break

            res_str, csv_data = decode_packet(res, "res", ctx)
            logger.info(f"[{client_ip}] <-- {res_str}")
            
            if csv_data:
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                tid = struct.unpack('>H', req[0:2])[0]
                for row in csv_data:
                    csv_logger.info(f"{ts},{client_ip},{tid:04X},03,{row['addr']},{row['name']},{row['hex']},{row['dec']},{row['scaled']},{row['unit']}")

            writer.write(res)
            await writer.drain()
    except OSError as e:
        logger.error(f"[{client_ip}] Network error (client or upstream): {e}")
    except Exception as e:
        logger.error(f"[{client_ip}] Handler error: {e}")
    finally:
        writer.close()
        logger.info(f"Client disconnected: {client_ip}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", "-b", default="0.0.0.0:5020")
    parser.add_argument("--target", "-t", required=True)
    parser.add_argument("--map", "-m", help="Path to YAML register map")
    parser.add_argument("--log", "-l", default="logs/modbus.log")
    args = parser.parse_args()

    setup_logging(args.log)
    log_dir = os.path.dirname(args.log) if args.log else None
    
    # Initialize the Global Mapper with the provided argument
    global mapper
    mapper = ModbusMapper(args.map)

    host, port = args.target.split(":")
    upstream = SharedUpstream(host, int(port), 5.0)
    
    # Check upstream connection at startup
    if not await upstream.check_connection():
        logger.warning(f"Could not connect to upstream {args.target}. Proxy will still start, but requests may fail.")
    else:
        logger.info(f"Successfully verified connection to upstream {args.target}")

    b_host, b_port = args.bind.split(":")

    server = await asyncio.start_server(lambda r, w: handle_client(r, w, upstream, log_dir), b_host, int(b_port))
    logger.info(f"Proxying {args.bind} -> {args.target}")
    if args.map: logger.info(f"Using Map: {args.map}")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
