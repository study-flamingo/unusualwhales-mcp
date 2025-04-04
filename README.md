# MCP Server for Unusual Whales API

[![MCP Server](https://img.shields.io/badge/MCP-Server-blue)](https://github.com/modelcontextprotocol/mcp)

This project implements a Model Context Protocol (MCP) server that acts as a wrapper around the [Unusual Whales REST API](https://unusualwhales.com/docs). It allows AI agents or other MCP clients to easily access various financial data endpoints provided by Unusual Whales.

## Features

*   Provides MCP tools to access common Unusual Whales API endpoints.
*   Returns data structured as [Polars](https://pola.rs/) DataFrames for efficient processing.
*   Handles API authentication and basic error handling.

## Requirements

*   Python 3.11+
*   An active Unusual Whales API subscription and token.
*   Poetry or `uv` for dependency management (optional but recommended).

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd mcp-server-unusualwhales
    ```
2.  **Install dependencies:**
    Using `uv` (recommended):
    ```bash
    uv sync
    ```
    Or using `pip` with `poetry`:
    ```bash
    poetry install --no-root --no-dev
    # or if you don't have poetry:
    # pip install -r requirements.txt # (You might need to generate this first: poetry export -f requirements.txt --output requirements.txt --without-hashes)
    ```

## Configuration

The server requires your Unusual Whales API token to authenticate requests. Set the following environment variable:

```bash
export UNUSUAL_WHALES_API_TOKEN="your_api_token_here"
```

On Windows PowerShell:
```powershell
$env:UNUSUAL_WHALES_API_TOKEN = "your_api_token_here"
```
Or add it to your `.env` file if you prefer.

## Usage

Run the MCP server using the `mcp` CLI:

```bash
mcp run src.server:mcp
```

This will start the server, making its tools available to connected MCP clients.

To test the server with the MCP Inspector:
```bash
npx @modelcontextprotocol/inspector mcp run src.server:mcp
```
Then open `http://localhost:5173` in your browser.

## Available Tools

The server exposes the following tools:

*   **`get_flow_alerts`**: Fetches options flow alerts with extensive filtering capabilities (e.g., by premium, DTE, ticker, side, OTM/ITM).
*   **`get_ticker_info`**: Retrieves general information about a specific stock ticker.
*   **`get_stock_state`**: Gets the latest OHLCV (Open, High, Low, Close, Volume) data for a ticker.
*   **`get_institution_holdings`**: Fetches the holdings reported by a specific institution (e.g., VANGUARD GROUP INC).
*   **`get_insider_transactions`**: Retrieves reported insider trading activity with filtering options.
*   **`get_congress_trades`**: Fetches trades reported by members of the US Congress.
*   **`get_news_headlines`**: Retrieves recent news headlines, filterable by ticker.

All tools return data as Polars DataFrames. Refer to the function docstrings in `src/server.py` for detailed parameter descriptions.
