# Modbus Proxy Logger

A high-performance Modbus TCP proxy designed for deep inspection, logging, and analysis.
This tool sits between your Modbus Client (Home Assistant, Sourceful Zap, Scada, PLC) and your Modbus TCP Server (Inverter, Meter).

It provides:
1.  **Multiplexing**: Allow multiple clients to talk to a single-connection Modbus device.
2.  **Deep Logging**: See every byte, every register, and every value.
3.  **Data Decoding**: Uses YAML maps to turn `04 D2` into `123.4 V`.
4.  **CSV Export**: Automatically builds a dataset of your device's behavior.

## Features

- **Project Management**: Built with `uv` for fast, reliable Python dependency management.
- **Transparent Proxy**: Works with any Modbus TCP traffic.
- **Robust Logging**: 
    - Concise by default with de-duplication of repeated lines.
    - Rolling logs (2MB chunks) with directory retention controls.
    - Optional CSV export for Excel/Pandas analysis.
- **Hardware Agnostic**: Just swap the YAML file to support Sungrow, Huawei, Victron, etc.

## Installation

1.  **Install uv**:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2.  **Sync Dependencies**:
    ```bash
    uv sync
    ```

## Usage

### Quick Start (Raw Mode)
Connect to `192.168.1.50` and listen on port `5020`:
```bash
uv run main.py --target 192.168.1.50:502
```

### Full Mode (With Decoding)
Use a map to decode values:
```bash
uv run main.py --target 192.168.1.50:502 --map maps/sungrow_hybrid.yaml
```

### Safe Default (Read-only proxy)
Writes (FC06/FC16) are blocked unless you explicitly allow them:
```bash
uv run main.py --target 192.168.1.50:502
```
To allow writes:
```bash
uv run main.py --target 192.168.1.50:502 --allow-write
```

### Monitoring endpoints
Expose a tiny HTTP server for health and stats:
```bash
uv run main.py --target 192.168.1.50:502 --http 127.0.0.1:8080
```
Then query `GET /health` or `GET /metrics`.

## Register Maps

Maps are located in the `maps/` directory.
You can create your own by following the format in `maps/sungrow_hybrid.yaml`.

**Format:**
```yaml
name: "My Device"
byte_order: "big"
word_order: "big" # for 32-bit: \"big\" or \"swap\"\n+
input_registers: # FC04
  registers:
    13000:
      name: "Battery Voltage"
      unit: "V"
      scale: 0.1
      type: "U16"

holding_registers: # FC03
  registers:
    13049:
      name: "EMS Mode Selection"
      type: "U16"
```

You can also compose maps using `include:`:
```yaml
include:
  - base.yaml
  - overrides.yaml
```

## Logging Policy

- **Log File**: Defaults to `logs/modbus.log`.
- **Rotation**: Files rotate every 2MB.
- **De-duplication**: repeated identical lines are summarized automatically.
- **Retention**: controlled by `--max-log-files` and `--max-log-dir-mb`.
- **CSV**: disable with `--no-csv` if you only need logs.
