#!/usr/bin/env python3
"""
Russell 2000 Index Ticker Fetcher

This script dynamically retrieves the current list of Russell 2000 Index constituents
(stock tickers) from live-updated sources, with no hard-coded tickers.

In actual testing, we tried multiple free data sources (Investing.com, ETF Database,
Finviz, Yahoo Finance(IWM), iShares IWM, MarketWatch, ETF.com, etc.). Test command:

    python russell2000_fetcher.py --test-all

Latest test results summary (current network environment, February 2026):

- **iShares IWM**: **1940** tickers (Tracking tool, reliable data)
- **Stoxray**: **2124** tickers (Third-party data source, high coverage, used as fallback)
- Investing.com / ETF Database: 23-24 tickers each
- Other data sources: 0 tickers (401/403, rate limiting, or only minimal data)

Considering constraints of "free, no login, no payment, using only requests+BeautifulSoup",
the current version adopts a **dual data source strategy**:
- **Primary data source**: iShares IWM (Tracking tool, reliable data, ~1940 tickers)
- **Fallback data source**: Stoxray (Third-party data source, high coverage, ~2124 tickers,
  automatic fallback when primary fails)

**Important Notes**:
- **Official data source**: FTSE Russell is the official index provider for Russell 2000 Index,
  but requires subscription/authorization; free testing typically cannot obtain complete data
- **Tracking tool**: iShares IWM is an ETF issued by BlackRock that tracks the Russell 2000 Index;
  provides reliable and easily accessible data
- By default, `fetch_russell2000_tickers()` prioritizes iShares IWM, automatically falling back
  to Stoxray on failure
- Test data source code has been moved to `test_sources.py` and is automatically called
  when using `--test-all`
- In actual usage, iShares IWM is prioritized; test data sources are not called

Usage:
    python russell2000_fetcher.py                 # Prioritize iShares IWM, fallback to Stoxray
    python russell2000_fetcher.py -o tickers.txt  # Also save to file
    python russell2000_fetcher.py --test-all      # Test all data sources (including test_sources.py)
"""

import argparse
import re
import sys
import time
from typing import Dict, List, Optional, Tuple


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# -----------------------------------------------------------------------------
# Primary data source: iShares IWM ETF (Tracking tool, reliable data)
# Fallback data source: Stoxray (Third-party data source)
# Note: FTSE Russell is the official index provider but requires subscription/authorization
# -----------------------------------------------------------------------------

STOXRAY_BASE = "https://stoxray.com/markets/russell-2000"


def fetch_from_ishares_iwm() -> Optional[List[str]]:
    """
    Fetch complete holdings CSV from iShares IWM ETF holdings page (primary data source).
    
    Note: iShares IWM is an ETF issued by BlackRock that tracks the Russell 2000 Index.
    It is not the official data source for the index, but provides reliable and easily
    accessible data. The official data source is FTSE Russell, but requires subscription/authorization.
    
    Accesses iShares website to download the complete holdings CSV file for IWM ETF,
    retrieving the Russell 2000 Index constituent list.
    
    Returns:
        List of stock tickers (uppercase strings), or None if fetch fails.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        import csv
        import io
        
        # iShares IWM may have multiple product IDs; try multiple
        product_ids = ["239710", "239714", "239707"]  # 239710 verified as working
        
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,text/csv",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.ishares.com/",
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        tickers = []
        
        # Try accessing homepage first to get cookies
        try:
            session.get("https://www.ishares.com/", timeout=30)
            time.sleep(1)
        except Exception:
            pass
        
        # Try each product ID
        for product_id in product_ids:
            try:
                # 1. Access main page first
                main_url = f"https://www.ishares.com/us/products/{product_id}/ishares-russell-2000-etf"
                resp = session.get(main_url, headers=headers, timeout=60)
                if resp.status_code != 200:
                    continue
                
                soup = BeautifulSoup(resp.text, "lxml")
                
                # 2. Find CSV download links
                csv_links = []
                
                # Find all possible CSV download links
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    text = a.get_text(strip=True).lower()
                    # Find links containing "csv", "download", "holdings", "export"
                    if any(keyword in href.lower() or keyword in text for keyword in 
                           ["csv", "download", "holdings", "export", ".csv"]):
                        if href.startswith("http"):
                            csv_links.append(href)
                        elif href.startswith("/"):
                            csv_links.append("https://www.ishares.com" + href)
                        else:
                            csv_links.append(f"https://www.ishares.com/{href}")
                
                # 3. Try accessing holdings page
                holdings_url = f"https://www.ishares.com/us/products/{product_id}/ishares-russell-2000-etf/holdings"
                try:
                    holdings_resp = session.get(holdings_url, headers=headers, timeout=60)
                    if holdings_resp.status_code == 200:
                        holdings_soup = BeautifulSoup(holdings_resp.text, "lxml")
                        # Also search for CSV links in holdings page
                        for a in holdings_soup.find_all("a", href=True):
                            href = a.get("href", "")
                            text = a.get_text(strip=True).lower()
                            if any(keyword in href.lower() or keyword in text for keyword in 
                                   ["csv", "download", "holdings", "export", ".csv"]):
                                if href.startswith("http"):
                                    csv_links.append(href)
                                elif href.startswith("/"):
                                    csv_links.append("https://www.ishares.com" + href)
                                else:
                                    csv_links.append(f"https://www.ishares.com/{href}")
                except Exception:
                    pass
                
                # 4. Try common CSV API path patterns (ensure IWM, not IWV)
                common_csv_patterns = [
                    f"https://www.ishares.com/us/products/{product_id}/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund",
                    f"https://www.ishares.com/us/products/{product_id}/ishares-russell-2000-etf/holdings.csv",
                    f"https://www.ishares.com/us/products/{product_id}/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings",
                ]
                csv_links.extend(common_csv_patterns)
                
                # 5. Try downloading and parsing CSV (ensure IWM, not IWV)
                for csv_url in csv_links:
                    try:
                        # Skip IWV (Russell 3000) links, only process IWM (Russell 2000)
                        if "IWV" in csv_url.upper() or "russell-3000" in csv_url.lower():
                            continue
                        
                        csv_headers = headers.copy()
                        csv_headers["Accept"] = "text/csv,text/plain,*/*"
                        csv_resp = session.get(csv_url, headers=csv_headers, timeout=60)
                        
                        if csv_resp.status_code == 200 and csv_resp.text:
                            # Check response content to ensure it's not IWV
                            if "IWV" in csv_resp.text[:500].upper() or "russell 3000" in csv_resp.text[:500].lower():
                                continue
                            # Try parsing CSV
                            csv_text = csv_resp.text
                            
                            # Use csv module to parse
                            try:
                                csv_reader = csv.reader(io.StringIO(csv_text))
                                header_found = False
                                ticker_col_idx = None
                                
                                for row_idx, row in enumerate(csv_reader):
                                    if row_idx == 0:
                                        # Find ticker/symbol column
                                        for col_idx, col in enumerate(row):
                                            col_lower = col.lower().strip()
                                            if any(keyword in col_lower for keyword in 
                                                   ["ticker", "symbol", "security", "holding"]):
                                                ticker_col_idx = col_idx
                                                header_found = True
                                                break
                                        if not header_found:
                                            # If header not found, try first column
                                            ticker_col_idx = 0
                                        continue
                                    
                                    # Extract ticker
                                    if ticker_col_idx is not None and len(row) > ticker_col_idx:
                                        ticker = row[ticker_col_idx].strip().strip('"').strip("'")
                                        if ticker and re.match(r"^[A-Za-z]{1,5}$", ticker.upper()):
                                            tickers.append(ticker.upper())
                            except Exception as csv_error:
                                # CSV parsing failed, try simple text parsing
                                lines = csv_text.split("\n")
                                for line in lines[1:]:  # Skip header row
                                    # Try comma-separated
                                    parts = line.split(",")
                                    for part in parts:
                                        part = part.strip().strip('"').strip("'")
                                        if part and re.match(r"^[A-Za-z]{1,5}$", part.upper()):
                                            tickers.append(part.upper())
                            
                            # If successfully retrieved many tickers, consider CSV found
                            if len(tickers) > 100:
                                break
                    except Exception as e:
                        continue
                
                # If enough tickers found, stop trying other product IDs
                if len(tickers) > 100:
                    break
                    
            except Exception as e:
                print(f"[iShares IWM] Failed to fetch product {product_id}: {e}", file=sys.stderr)
                continue
        
        session.close()
        
        # Deduplicate and return
        unique_tickers = list(dict.fromkeys(tickers))
        return unique_tickers if unique_tickers else None
        
    except Exception as e:
        print(f"[iShares IWM] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_stoxray(max_pages: int = 45) -> Optional[List[str]]:
    """
    Fetch Russell 2000 constituents (stock tickers) from Stoxray.

    Stoxray page structure:
        Company | Ticker (e.g., "VBIVXNAS") | Last | Cur | M.cap. | Industry

    Ticker column is typically "ticker + exchange", e.g., "VBIVXNAS", "AUPHXNAS".
    Here we extract the stock ticker part.

    Args:
        max_pages: Maximum number of pages to fetch (default 45 pages, ~2200 tickers)

    Returns:
        List of stock tickers (uppercase strings), or None if fetch fails.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        import urllib3

        # Suppress SSL warnings to avoid noise in some environments
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }

        tickers: List[str] = []

        session = requests.Session()
        session.headers.update(headers)

        for page in range(max_pages):
            url = f"{STOXRAY_BASE}?page={page}" if page > 0 else STOXRAY_BASE

            # Simple retry for stability
            max_retries = 3
            last_error: Optional[Exception] = None
            for retry in range(max_retries):
                try:
                    resp = session.get(
                        url,
                        timeout=60,
                        verify=True,
                        stream=False,
                    )
                    resp.raise_for_status()
                    break
                except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                    last_error = e
                    if retry < max_retries - 1:
                        wait_time = (retry + 1) * 2  # 2s, 4s, 6s
                        print(
                            f"[Stoxray] Page {page} SSL/connection error, "
                            f"retrying in {wait_time}s...",
                            file=sys.stderr,
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        print(
                            f"[Stoxray] Page {page} failed after retries: {e}",
                            file=sys.stderr,
                        )
                        return None

            soup = BeautifulSoup(resp.text, "lxml")
            page_tickers = 0

            for row in soup.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue

                # Second column is typically Ticker column, e.g., "VBIVXNAS"
                ticker_cell = cells[1].get_text(strip=True).upper().replace(" ", "")
                if not ticker_cell:
                    continue

                # Remove exchange suffixes: XNAS / XNYS / ARCA / AMEX etc.
                for suffix in ("XNAS", "XNYS", "ARCA", "AMEX"):
                    if ticker_cell.endswith(suffix):
                        ticker_cell = ticker_cell[:-len(suffix)]
                        break

                if ticker_cell and re.match(r"^[A-Z]{1,5}$", ticker_cell):
                    tickers.append(ticker_cell)
                    page_tickers += 1

            # If this page has significantly fewer tickers, consider it the last page
            if page > 0 and page_tickers < 30:
                break

            # Slight delay to avoid putting pressure on the website
            delay = 0.2 + (page * 0.01)  # 0.2s ~ 0.65s
            time.sleep(delay)

        session.close()

        if not tickers:
            return None

        # Deduplicate while preserving order
        unique = list(dict.fromkeys(tickers))
        return unique

    except Exception as e:
        print(f"[Stoxray] Scraping failed: {e}", file=sys.stderr)
        return None


# -----------------------------------------------------------------------------
# Main interface: Prioritize iShares IWM (tracking tool, reliable data),
# fallback to Stoxray on failure
# Test data sources moved to test_sources.py, called when using --test-all
# -----------------------------------------------------------------------------

MIN_EXPECTED_TICKERS = 500  # Russell 2000 theoretically ~2000 tickers; set safe lower limit

DATA_SOURCES: Dict[str, Tuple[str, callable]] = {
    "ishares_iwm": ("iShares IWM", fetch_from_ishares_iwm),
    "stoxray": ("Stoxray", fetch_from_stoxray),
}


def fetch_russell2000_tickers(source: Optional[str] = None) -> List[str]:
    """
    Retrieve current Russell 2000 Index constituent list (stock tickers).

    **Note**: By default, prioritizes iShares IWM (tracking tool, reliable data),
    automatically falling back to Stoxray (third-party data source) on failure.
    Test data sources moved to test_sources.py, called when using `--test-all`.
    
    Note: FTSE Russell is the official index provider for Russell 2000 Index,
    but requires subscription/authorization; free testing typically cannot obtain complete data.

    Args:
        source: Optional data source name. If None, automatically selects by priority:
                - Primary: iShares IWM (tracking tool, reliable data)
                - Fallback: Stoxray (third-party data source)
                Can also manually specify 'ishares_iwm' or 'stoxray'.

    Returns:
        List of stock tickers (e.g., ['AAPL', 'TSLA', ...]).
    """
    if source is None:
        # Default priority: try iShares IWM first (tracking tool, reliable data),
        # fallback to Stoxray on failure
        primary_source = "ishares_iwm"
        fallback_source = "stoxray"
        
        # Try primary data source
        name, func = DATA_SOURCES[primary_source]
        print(f"[Trying {name}...]", file=sys.stderr)
        tickers = func()
        
        if tickers and len(tickers) >= MIN_EXPECTED_TICKERS:
            print(f"[OK] Retrieved {len(tickers)} tickers from {name}", file=sys.stderr)
            return tickers
        else:
            # Primary data source failed, fallback to alternative
            print(f"[{name} failed or insufficient data, falling back to {DATA_SOURCES[fallback_source][0]}...]", file=sys.stderr)
            name, func = DATA_SOURCES[fallback_source]
            tickers = func()
            if not tickers:
                raise RuntimeError(f"Failed to retrieve tickers from both {DATA_SOURCES[primary_source][0]} and {name}")
            print(f"[OK] Retrieved {len(tickers)} tickers from {name} (fallback)", file=sys.stderr)
            return tickers
    else:
        source_key = source.lower()
        if source_key not in DATA_SOURCES:
            raise ValueError(
                f"Unknown source: {source}. Available: {', '.join(DATA_SOURCES.keys())}"
            )

        name, func = DATA_SOURCES[source_key]
        tickers = func()
        if not tickers:
            raise RuntimeError(f"Failed to retrieve tickers from {name}")

        print(f"[OK] Retrieved {len(tickers)} tickers from {name}", file=sys.stderr)
        return tickers


def test_all_sources() -> dict:
    """
    Test all data sources and return results.

    This function calls test data sources from test_sources.py, as well as
    primary sources from the main program. Used to demonstrate complete
    testing process and comparison results.

    Returns:
        Dictionary of {source_key: (count, tickers)}.
    """
    results: dict = {}

    print("\n" + "=" * 60, file=sys.stderr)
    print("Testing all data sources...", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)

    # Test primary data sources first (iShares IWM and Stoxray)
    for source_key, (name, func) in DATA_SOURCES.items():
        print(f"Testing {name}...", end=" ", flush=True, file=sys.stderr)
        try:
            tickers = func()
            if tickers:
                count = len(tickers)
                results[source_key] = (count, tickers)
                print(f"[OK] {count} tickers", file=sys.stderr)
            else:
                results[source_key] = (0, [])
                print("[FAILED] (no data)", file=sys.stderr)
        except Exception as e:
            results[source_key] = (0, [])
            print(f"[FAILED] Error: {str(e)[:80]}", file=sys.stderr)

    # Then test other data sources (from test_sources.py, skip those already in primary sources)
    try:
        import test_sources
        for source_key, (name, func) in test_sources.TEST_SOURCES.items():
            # Skip those already in primary sources (avoid duplicate testing)
            if source_key in DATA_SOURCES:
                continue
            print(f"Testing {name}...", end=" ", flush=True, file=sys.stderr)
            try:
                tickers = func()
                if tickers:
                    count = len(tickers)
                    results[source_key] = (count, tickers)
                    print(f"[OK] {count} tickers", file=sys.stderr)
                else:
                    results[source_key] = (0, [])
                    print("[FAILED] (no data)", file=sys.stderr)
            except Exception as e:
                results[source_key] = (0, [])
                print(f"[FAILED] Error: {str(e)[:80]}", file=sys.stderr)
    except ImportError:
        print("\n[Warning] test_sources.py not found, skipping test sources.", file=sys.stderr)

    print("\n" + "=" * 60, file=sys.stderr)
    print("Comparison Summary:", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Merge all data source names for display
    all_sources = dict(DATA_SOURCES)  # Include primary sources
    try:
        import test_sources
        all_sources.update(test_sources.TEST_SOURCES)
    except ImportError:
        pass
    
    for source_key, (count, _) in sorted(
        results.items(), key=lambda x: x[1][0], reverse=True
    ):
        name = all_sources.get(source_key, (source_key, None))[0]
        print(f"  {name:30s}: {count:4d} tickers", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)

    return results


# -----------------------------------------------------------------------------
# CLI entry point
# -----------------------------------------------------------------------------

def main() -> None:
    """CLI entry point: Fetch and print Russell 2000 constituent list."""
    parser = argparse.ArgumentParser(
        description="Fetch Russell 2000 Index stock tickers (default: iShares IWM, fallback to Stoxray, no hard-coded list).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Prioritize iShares IWM to fetch and print constituents (auto-fallback to Stoxray on failure)
  python russell2000_fetcher.py

  # Print and save to file
  python russell2000_fetcher.py -o russell2000_tickers.txt

  # Test all data sources (including test sources)
  python russell2000_fetcher.py --test-all

  # Use specific data source
  python russell2000_fetcher.py --source ishares_iwm  # Force iShares IWM
  python russell2000_fetcher.py --source stoxray      # Force Stoxray
        """,
    )

    parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="Save tickers to file (also prints to console)",
    )
    parser.add_argument(
        "--source",
        choices=list(DATA_SOURCES.keys()),
        help="Use specific data source (default: auto-select 'ishares_iwm' with 'stoxray' fallback)",
    )
    parser.add_argument(
        "--test-all",
        action="store_true",
        help="Test built-in data source(s) and show comparison (no tickers printed)",
    )

    args = parser.parse_args()

    # Test mode: test all data sources
    if args.test_all:
        results = test_all_sources()
        if args.output:
            # Merge all data source names for display
            all_sources = dict(DATA_SOURCES)
            try:
                import test_sources
                all_sources.update(test_sources.TEST_SOURCES)
            except ImportError:
                pass
            
            with open(args.output, "w", encoding="utf-8") as f:
                f.write("Data Source Comparison\n")
                f.write("=" * 60 + "\n\n")
                f.write("Note: Default usage prioritizes iShares IWM with Stoxray fallback. Other sources are for testing only.\n\n")
                for source_key, (count, tickers) in sorted(
                    results.items(), key=lambda x: x[1][0], reverse=True
                ):
                    name = all_sources.get(source_key, (source_key, None))[0]
                    f.write(f"{name}: {count} tickers\n")
                    if tickers:
                        f.write(
                            f"  Sample: {', '.join(sorted(tickers)[:10])}...\n"
                        )
                    f.write("\n")
            print(f"\n[Comparison saved to {args.output}]", file=sys.stderr)
        return

    # Normal mode: fetch and output constituents
    tickers = fetch_russell2000_tickers(source=args.source)
    sorted_tickers = sorted(tickers)

    for t in sorted_tickers:
        print(t)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            for t in sorted_tickers:
                f.write(t + "\n")
        print(f"\n[Saved {len(tickers)} tickers to {args.output}]", file=sys.stderr)


if __name__ == "__main__":
    main()
