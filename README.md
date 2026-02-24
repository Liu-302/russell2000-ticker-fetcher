# Russell 2000 Index Ticker Fetcher

A small Python tool that dynamically fetches the current Russell 2000 index constituents (stock tickers) from live-updated US data sources, with **no hard-coded ticker list**.

## Features

- Dynamically retrieves the latest Russell 2000 stock tickers (e.g. `AAPL`, `TSLA`)
- No hard-coded tickers: all data is fetched from the web at runtime
- Dual data source strategy:
  - Primary: iShares IWM ETF holdings (tracking tool, reliable data)
  - Fallback: Stoxray Russell 2000 constituents page (third-party source, high coverage)
- Simple CLI and Python API

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start (CLI)

```bash
# Fetch and print Russell 2000 tickers (primary: iShares IWM, fallback: Stoxray)
python russell2000_fetcher.py

# Fetch and also save to a file
python russell2000_fetcher.py -o russell2000_tickers.txt

# Test all configured data sources and show a comparison summary
python russell2000_fetcher.py --test-all
```

### Selecting a Specific Data Source

```bash
# Force iShares IWM (tracking ETF, reliable data)
python russell2000_fetcher.py --source ishares_iwm

# Force Stoxray (third-party fallback, high coverage)
python russell2000_fetcher.py --source stoxray
```

## Python API

```python
from russell2000_fetcher import fetch_russell2000_tickers, test_all_sources

# Use best data source (primary iShares IWM, fallback Stoxray)
tickers = fetch_russell2000_tickers()
print(f"Total: {len(tickers)} constituents")

# Use a specific data source
tickers_ishares = fetch_russell2000_tickers(source="ishares_iwm")
tickers_stoxray = fetch_russell2000_tickers(source="stoxray")

# Test all data sources and get counts
results = test_all_sources()
for source_name, (count, tickers_list) in results.items():
    print(f"{source_name}: {count} tickers")
```

## Data Sources

- **FTSE Russell** – Official index provider for the Russell 2000 index.  
  Official, but typically requires subscription/authorization for full constituent data (not used directly here).

- **iShares IWM ETF (Primary)** – ETF issued by BlackRock that tracks the Russell 2000 index.  
  This project downloads and parses the public holdings CSV to approximate the index constituents.

- **Stoxray (Fallback)** – Third-party financial data website with a Russell 2000 constituents page.  
  Used as a fallback when iShares CSV cannot be retrieved; coverage is slightly above 2000 tickers.
