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
    - Rolling logs (5MB limit).
    - Auto-cleanup (Limits folder to 200MB).
    - CSV export for Excel/Pandas analysis.
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

## Register Maps

Maps are located in the `maps/` directory.
You can create your own by following the format in `maps/sungrow_hybrid.yaml`.

**Format:**
```yaml
registers:
  13000:
    name: "Battery Voltage"
    unit: "V"
    scale: 0.1
    type: "U16"
```

## Logging Policy

- **Log File**: Defaults to `logs/modbus.log`.
- **Rotation**: Files rotate every 5MB.
- **Retention**: Keeps last 10 files.
- **Cleanup**: Automatically deletes old logs if total folder size > 200MB.
