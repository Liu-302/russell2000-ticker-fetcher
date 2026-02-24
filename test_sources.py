#!/usr/bin/env python3
"""
Test Data Sources Module

This module contains all data source functions intended for testing only.
These data sources perform poorly in actual usage, but code is retained
to demonstrate the complete testing process and comparison results.

In actual usage, the main program uses dual data source strategy
(iShares IWM and Stoxray, see russell2000_fetcher.py).
"""

import re
import sys
import time
from typing import Dict, List, Optional, Tuple

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def fetch_from_investing() -> Optional[List[str]]:
    """Test only: Investing.com (usually returns 0 tickers)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}
        resp = requests.get("https://www.investing.com/indices/smallcap-2000-components", headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                for cell in cells[:2]:
                    links = cell.find_all("a", href=True)
                    for a in links:
                        text = a.get_text(strip=True)
                        if text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[Investing.com] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_yfinance() -> Optional[List[str]]:
    """Test only: Yahoo Finance IWM ETF (usually rate-limited, returns 0 tickers)"""
    try:
        import yfinance as yf
        for attempt in range(3):
            try:
                if attempt > 0:
                    delay = 2 ** attempt
                    print(f"[yfinance] Retry attempt {attempt + 1}/3 after {delay}s...", file=sys.stderr)
                    time.sleep(delay)
                iwm = yf.Ticker("IWM")
                funds = iwm.funds_data
                if funds:
                    equity = getattr(funds, "equity_holdings", None) or getattr(funds, "top_holdings", None)
                    if equity and hasattr(equity, "columns"):
                        for col in ["Symbol", "Holding", "symbol", "holding"]:
                            if col in equity.columns:
                                tickers = equity[col].dropna().astype(str).str.strip().tolist()
                                valid = [t.upper() for t in tickers if t and re.match(r"^[A-Z]{1,5}$", t.upper())]
                                return list(dict.fromkeys(valid)) if valid else None
                return None
            except Exception as e:
                if "rate limit" in str(e).lower() or "429" in str(e):
                    if attempt < 2:
                        continue
                print(f"[yfinance] Attempt {attempt + 1} failed: {e}", file=sys.stderr)
                if attempt == 2:
                    return None
        return None
    except Exception as e:
        print(f"[yfinance] Fallback triggered: {e}", file=sys.stderr)
        return None


def fetch_from_ishares_iwm() -> Optional[List[str]]:
    """
    Test only: iShares IWM ETF holdings page (attempts to fetch complete holdings CSV)
    
    Note: iShares IWM is an ETF issued by BlackRock that tracks the Russell 2000 Index.
    It is not the official data source for the index, but provides reliable and easily
    accessible data. The official data source is FTSE Russell, but requires subscription/authorization.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        import csv
        import io
        
        # iShares IWM may have multiple product IDs; try multiple
        product_ids = ["239714", "239710", "239707"]  # 239714 verified as working
        
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
                                print(f"[iShares IWM] Found CSV with {len(tickers)} tickers from {csv_url}", file=sys.stderr)
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


def fetch_from_quotelinks() -> Optional[List[str]]:
    """Test only: QuoteLinks (usually returns 0 tickers)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}
        resp = requests.get("https://www.quotelinks.com/russell2000.html", headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 10:
                continue
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if cells:
                    sym = cells[0].get_text(strip=True)
                    if sym and re.match(r"^[A-Za-z]{1,5}$", sym):
                        tickers.append(sym.upper())
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[QuoteLinks] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_barchart() -> Optional[List[str]]:
    """Test only: Barchart (usually returns 0 tickers)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Referer": "https://www.barchart.com/"}
        resp = requests.get("https://www.barchart.com/stocks/indices/russell/russell2000", headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        href = a.get("href", "")
                        text = a.get_text(strip=True)
                        if "/quotes/" in href or "/stocks/" in href:
                            parts = href.split("/")
                            for part in parts:
                                if part and re.match(r"^[A-Za-z]{1,5}$", part.upper()):
                                    tickers.append(part.upper())
                        elif text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[Barchart] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_marketvolume() -> Optional[List[str]]:
    """Test only: MarketVolume (usually returns 0 tickers, SSL errors)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*"}
        for retry in range(2):
            try:
                resp = requests.get("https://www.marketvolume.com/indexes_exchanges/r2000_components.asp", headers=headers, timeout=60)
                resp.raise_for_status()
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if retry < 1:
                    print(f"[MarketVolume] Timeout, retrying...", file=sys.stderr)
                    time.sleep(3)
                    continue
                raise
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        text = a.get_text(strip=True)
                        if text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[MarketVolume] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_ftserussell() -> Optional[List[str]]:
    """
    Test only: FTSE Russell official data source (official index provider for Russell 2000 Index)
    
    Note: FTSE Russell is the official index provider for Russell 2000 Index, but typically
    requires subscription/authorization to access complete data. This function attempts
    to fetch data from public pages, but usually cannot obtain the complete list.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        
        # Try multiple possible URLs
        urls = [
            "https://www.ftserussell.com/analytics/factsheets/home/constituentsweights",
            "https://www.ftserussell.com/products/indices/russell-2000",
            "https://www.ftserussell.com/products/indices/russell-2000/constituents",
            "https://www.ftserussell.com/products/indices/russell-2000/constituents-weights",
            "https://research.ftserussell.com/Analytics/Factsheets/Home/ConstituentsWeights",
        ]
        
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.ftserussell.com/",
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        # Access homepage first to get cookies
        try:
            session.get("https://www.ftserussell.com/", timeout=30)
            time.sleep(1)
        except Exception:
            pass
        
        tickers = []
        
        for url in urls:
            try:
                resp = session.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "lxml")
                    
                    # Find stock tickers in tables
                    for table in soup.find_all("table"):
                        for row in table.find_all("tr"):
                            cells = row.find_all(["td", "th"])
                            for cell in cells:
                                # Find stock tickers in links
                                for a in cell.find_all("a", href=True):
                                    text = a.get_text(strip=True)
                                    if text and re.match(r"^[A-Za-z]{1,5}$", text.upper()):
                                        tickers.append(text.upper())
                                
                                # Directly find stock tickers in text
                                text = cell.get_text(strip=True)
                                if text and re.match(r"^[A-Za-z]{1,5}$", text.upper()):
                                    tickers.append(text.upper())
                    
                    # If some tickers found, continue trying other URLs
                    if tickers:
                        continue
            except Exception:
                continue
        
        session.close()
        
        unique_tickers = list(dict.fromkeys(tickers))
        return unique_tickers if unique_tickers else None
        
    except Exception as e:
        print(f"[FTSE Russell] Attempt failed (official data source, may require subscription/authorization): {e}", file=sys.stderr)
        return None


def fetch_from_etfdb() -> Optional[List[str]]:
    """Test only: ETF Database (usually returns only 23 tickers, JS dynamic loading)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Referer": "https://etfdb.com/"}
        resp = requests.get("https://etfdb.com/etf/IWM/#holdings", headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        href = a.get("href", "")
                        text = a.get_text(strip=True)
                        if "/stock/" in href or "/equity/" in href:
                            parts = href.split("/")
                            for part in parts:
                                if part and re.match(r"^[A-Za-z]{1,5}$", part.upper()):
                                    tickers.append(part.upper())
                        elif text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[ETF Database] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_zacks() -> Optional[List[str]]:
    """Test only: Zacks (usually returns 0 tickers)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Referer": "https://www.zacks.com/"}
        resp = requests.get("https://www.zacks.com/funds/etf/IWM/holding", headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        href = a.get("href", "")
                        text = a.get_text(strip=True)
                        if "/stock/" in href or "/quote/" in href:
                            parts = href.split("/")
                            for part in parts:
                                if part and re.match(r"^[A-Za-z]{1,5}$", part.upper()):
                                    tickers.append(part.upper())
                        elif text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[Zacks] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_marketwatch() -> Optional[List[str]]:
    """Test only: MarketWatch (usually returns 401 Forbidden, requires login)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.marketwatch.com/",
        }
        session = requests.Session()
        session.headers.update(headers)
        try:
            session.get("https://www.marketwatch.com/", timeout=30)
            time.sleep(1)
        except Exception:
            pass
        resp = session.get("https://www.marketwatch.com/investing/index/rut", headers=headers, timeout=60)
        if resp.status_code in [401, 403]:
            alt_url = "https://www.marketwatch.com/investing/index/rut/components"
            resp = session.get(alt_url, headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        href = a.get("href", "")
                        text = a.get_text(strip=True)
                        if "/quote/" in href or "/investing/stock/" in href:
                            parts = href.split("/")
                            for part in parts:
                                if part and re.match(r"^[A-Za-z]{1,5}$", part.upper()):
                                    tickers.append(part.upper())
                        elif text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        session.close()
        return list(dict.fromkeys(tickers)) if tickers else None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [401, 403]:
            print(f"[MarketWatch] Access denied (may require login): {e}", file=sys.stderr)
        else:
            print(f"[MarketWatch] Scraping failed: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[MarketWatch] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_finviz() -> Optional[List[str]]:
    """Test only: Finviz (usually returns only partial tickers, JS pagination)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Referer": "https://finviz.com/"}
        resp = requests.get("https://finviz.com/screener.ashx?v=111&f=idx_rut2000", headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        href = a.get("href", "")
                        if "/quote.ashx" in href and "t=" in href:
                            ticker = href.split("t=")[1].split("&")[0].upper()
                            if ticker and re.match(r"^[A-Z]{1,5}$", ticker):
                                tickers.append(ticker)
        return list(dict.fromkeys(tickers)) if tickers else None
    except Exception as e:
        print(f"[Finviz] Scraping failed: {e}", file=sys.stderr)
        return None


def fetch_from_etfcom() -> Optional[List[str]]:
    """Test only: ETF.com (usually returns 403 Forbidden)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.etf.com/",
        }
        session = requests.Session()
        session.headers.update(headers)
        try:
            session.get("https://www.etf.com/", timeout=30)
            time.sleep(1)
        except Exception:
            pass
        resp = session.get("https://www.etf.com/IWM", headers=headers, timeout=60)
        if resp.status_code == 403:
            alt_url = "https://www.etf.com/IWM/holdings"
            resp = session.get(alt_url, headers=headers, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tickers = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    for a in cell.find_all("a", href=True):
                        href = a.get("href", "")
                        text = a.get_text(strip=True)
                        if "/holdings/" in href or "/stock/" in href:
                            parts = href.split("/")
                            for part in parts:
                                if part and re.match(r"^[A-Za-z]{1,5}$", part.upper()):
                                    tickers.append(part.upper())
                        elif text and re.match(r"^[A-Za-z]{1,5}$", text):
                            tickers.append(text.upper())
        session.close()
        return list(dict.fromkeys(tickers)) if tickers else None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"[ETF.com] Access denied (may require login): {e}", file=sys.stderr)
        else:
            print(f"[ETF.com] Scraping failed: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[ETF.com] Scraping failed: {e}", file=sys.stderr)
        return None


# All test data sources dictionary
TEST_SOURCES: Dict[str, Tuple[str, callable]] = {
    "investing": ("Investing.com", fetch_from_investing),
    "yfinance": ("Yahoo Finance (IWM ETF)", fetch_from_yfinance),
    "ishares_iwm": ("iShares IWM", fetch_from_ishares_iwm),
    "quotelinks": ("QuoteLinks", fetch_from_quotelinks),
    "barchart": ("Barchart", fetch_from_barchart),
    "marketvolume": ("MarketVolume", fetch_from_marketvolume),
    "ftserussell": ("FTSE Russell (Official)", fetch_from_ftserussell),
    "etfdb": ("ETF Database", fetch_from_etfdb),
    "zacks": ("Zacks", fetch_from_zacks),
    "marketwatch": ("MarketWatch", fetch_from_marketwatch),
    "finviz": ("Finviz", fetch_from_finviz),
    "etfcom": ("ETF.com", fetch_from_etfcom),
}
