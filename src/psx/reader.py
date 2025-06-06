"""High level interface for downloading PSX data."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from urllib.parse import urljoin
import json
import os
import threading
from typing import Union

import pandas as pd
from tqdm import tqdm
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta

import requests

from . import network, parsing


class DataReader:
    """Thread-safe downloader for PSX historical data and announcements."""

    headers = parsing.HEADERS

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.__history = "https://dps.psx.com.pk/historical"
        self.__symbols = "https://dps.psx.com.pk/symbols"
        self.__local = threading.local()

    # ------------------------------------------------------------------
    @property
    def session(self) -> requests.Session:
        """Get or create a requests session for the current thread."""
        if not hasattr(self.__local, "session"):
            self.__local.session = requests.Session()
        return self.__local.session

    # ------------------------------------------------------------------
    def tickers(self) -> pd.DataFrame:
        """Return the PSX master symbol list as a DataFrame."""
        return pd.read_json(self.__symbols)

    def stocks(self, tickers: Union[str, list, None], start: date, end: date) -> pd.DataFrame:
        """Download OHLCV data for one or more symbols."""
        if tickers is None:
            universe = self.tickers()
            tickers = universe.loc[~universe["isDebt"], "symbol"].tolist()
        tickers = [tickers] if isinstance(tickers, str) else tickers
        dates = parsing.daterange(start, end)

        frames = []
        for ticker in tickers:
            frame = self.get_psx_data(ticker, dates)
            frames.append(frame[start:end])
            if self.verbose:
                print(f"{ticker}: final frame {frame.shape[0]} rows\n{frame.head()}\n")

        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, keys=tickers, names=["Ticker", "Date"])

    # ------------------------------------------------------------------
    def reports(
        self,
        symbol: str,
        tab_name: str = "Financial Results",
        years: int = 5,
        save_dir: str = ".",
    ) -> list:
        """Scrape company announcements from the PSX site.

        If ``tab_name`` is ``"Financial Reports"`` the function scrapes the
        ``Financial Reports`` table which only contains PDF links.  For all
        other tabs the function falls back to the standard Announcements
        section.
        """

        cutoff = date.today() - relativedelta(years=years)
        base = f"https://dps.psx.com.pk/company/{symbol}"
        results = []
        next_url = base

        while next_url:
            if tab_name == "Financial Reports":
                soup = self._get_page_dynamic(next_url)
                tab = soup.find(id="reports") or soup
                for row in tab.select("tbody tr"):
                    cells = row.find_all("td")
                    if len(cells) < 3:
                        continue

                    when = date_parser.parse(cells[2].get_text(strip=True)).date()
                    if when < cutoff:
                        next_url = None
                        break

                    link = cells[0].find("a")
                    if not link:
                        continue

                    try:
                        content = self._extract_pdf(urljoin(base, link["href"]))
                        source = "PDF"
                    except Exception:
                        content, source = "", ""

                    results.append(
                        {
                            "title": link.get_text(strip=True),
                            "date": when.isoformat(),
                            "source": source,
                            "content": content,
                        }
                    )

                # financial reports do not provide proper pagination links
                next_url = None

            else:
                soup = self._get_page(next_url)

                tab = soup.find(id=tab_name.replace(" ", "")) or soup

                for row in tab.select("tr"):
                    title_el = row.find(class_="title")
                    date_el = row.find(class_="date")
                    if not title_el or not date_el:
                        continue

                    when = date_parser.parse(date_el.get_text(strip=True)).date()
                    if when < cutoff:
                        next_url = None
                        break

                    pdf_link = row.find("a", class_="pdf")
                    view_link = row.find("a", class_="view")
                    content, source = "", ""

                    if pdf_link is not None:
                        try:
                            content = self._extract_pdf(urljoin(base, pdf_link["href"]))
                            source = "PDF"
                        except Exception:
                            if view_link is not None:
                                content = self._extract_view(urljoin(base, view_link["href"]))
                                source = "View"
                    elif view_link is not None:
                        content = self._extract_view(urljoin(base, view_link["href"]))
                        source = "View"

                    results.append(
                        {
                            "title": title_el.get_text(strip=True),
                            "date": when.isoformat(),
                            "source": source,
                            "content": content,
                        }
                    )

                next_link = tab.find("a", class_="next")
                next_url = urljoin(base, next_link["href"]) if next_link else None

        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, f"{symbol}_{tab_name.replace(' ', '_')}_reports.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)
        return results

    # ------------------------------------------------------------------
    def get_psx_data(self, symbol: str, dates: list) -> pd.DataFrame:
        """Download and combine monthly trading data for ``symbol``."""
        data, futures = [], []
        desc = f"Downloading {symbol}'s Data"
        with tqdm(total=len(dates), desc=desc) as bar, ThreadPoolExecutor(max_workers=6) as pool:
            for d in dates:
                futures.append(pool.submit(self._download_single_month, symbol=symbol, dt=d))
            for fut in as_completed(futures):
                data.append(fut.result())
                bar.update(1)

        data = [df for df in data if isinstance(df, pd.DataFrame)]
        frame = parsing.preprocess(data)
        if self.verbose:
            print(
                f"{symbol}: fetched {frame.shape[0]} rows "
                f"({len([d for d in data if not d.empty])} months non-empty)"
            )
        return frame

    # ------------------------------------------------------------------
    def _download_single_month(self, symbol: str, dt: date) -> pd.DataFrame:
        """Internal helper to fetch a single month."""
        post = {"month": dt.month, "year": dt.year, "symbol": symbol}
        response = self.session.post(self.__history, data=post, timeout=30)
        response.raise_for_status()
        soup = network.parser(response.text, "html.parser")
        return parsing.html_to_frame(soup)

    # ------------------------------------------------------------------
    # Wrappers for backwards compatibility and tests
    def _get_page(self, url: str):
        return network.get_page(self.session, url)

    def _get_page_dynamic(self, url: str):
        return network.get_page_dynamic(url)

    def _extract_pdf(self, url: str) -> str:
        return network.extract_pdf(self.session, url)

    def _extract_view(self, url: str) -> str:
        return network.extract_view(self.session, url)

    def _html_to_frame(self, soup):
        return parsing.html_to_frame(soup)

    def _preprocess(self, monthly_frames):
        return parsing.preprocess(monthly_frames)

    @staticmethod
    def daterange(start: date, end: date):
        return parsing.daterange(start, end)
