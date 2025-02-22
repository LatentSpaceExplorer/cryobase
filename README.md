# Cryobase

Cryobase is a Python-based ETL pipeline that extracts blockchain data using [cryo](https://github.com/paradigmxyz/cryo), transforms it, and loads it into ClickHouse for analysis. It supports multiple EVM chains and includes derived data analysis for tokens, AMMs, and accounts.

## Features

- **Multi-Chain Support**: Process data from multiple EVM chains simultaneously
- **Raw Data Collection**: Extract blocks, transactions, logs, and native transfers using cryo
- **Derived Analytics**:
  - Token Analysis (ERC20, ERC721, ERC1155)
  - AMM Analysis (Uniswap V2/V3 pools and swaps)
  - Account Analysis (Active EOA summaries, hot contracts)
- **Materialized Views**: Automatically maintains derived tables using ClickHouse materialized views

## Prerequisites

- Python 3.10+
- ClickHouse server
- Access to an EVM chain RPC endpoint
- Installed [cryo](https://github.com/paradigmxyz/cryo) CLI tool
- 64GB of RAM
- 1TB of storage

## Installation

1. Clone the repository 
2. Install dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

3. Configure your chains in `config.toml`, see `config.example.toml` for reference.

4. Run the pipeline:

```bash
uv run cryobase.py
```

## Supported Cryo Datasets

- `blocks`
- `transactions`
- `logs`
- `native_transfers`


