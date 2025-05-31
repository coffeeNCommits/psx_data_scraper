"""
patched_datareader.py
---------------------
Fork of the original psx-data-reader class, updated May-2025.

• Works with the new PSX HTML table whose first column is now 'Date'
  (but still accepts the old 'TIME' column if PSX flips back).
• Prints row counts and sample data frames so you can eyeball progress
  without setting extra breakpoints.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, date
from typing import Union

from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup as parser
from tqdm import tqdm

import threading
import numpy as np
import pandas as pd
import requests

# ────────────────────────────────────────────────────────────── #

class DataReader:
    """
    One instance → thread-safe psx scraper / CSV parser.
    """

    # **NEW** header list spells the columns the way we want them
    headers = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.__history = "https://dps.psx.com.pk/historical"
        self.__symbols = "https://dps.psx.com.pk/symbols"
        self.__local = threading.local()

    # ——————————————————————————————————————————— session per thread ——— #

    @property
    def session(self):
        if not hasattr(self.__local, "session"):
            self.__local.session = requests.Session()
        return self.__local.session

    # ——————————————————————————————— public convenience wrappers ——— #

    def tickers(self) -> pd.DataFrame:
        """Return the PSX master symbol list as DataFrame."""
        return pd.read_json(self.__symbols)

    def stocks(self,
               tickers: Union[str, list],
               start: date,
               end: date) -> pd.DataFrame:
        """
        Main public API – returns an OHLCV DataFrame for one or more tickers.
        """
        tickers = [tickers] if isinstance(tickers, str) else tickers
        dates   = self.daterange(start, end)

        frames = []
        for ticker in tickers:
            frame = self.get_psx_data(ticker, dates)
            frames.append(frame[start:end])  # slice after full concat
            if self.verbose:
                print(f"{ticker}: final frame {frame.shape[0]} rows\n{frame.head()}\n")

        if len(frames) == 1:
            return frames[0]

        return pd.concat(frames, keys=tickers, names=["Ticker", "Date"])

    # ——————————————————————————————— low-level fetch helpers ——— #

    def get_psx_data(self, symbol: str, dates: list) -> pd.DataFrame:
        """Pull one month at a time in parallel, concat, clean."""
        data, futures = [], []

        desc = f"Downloading {symbol}'s Data"
        with tqdm(total=len(dates), desc=desc) as bar, \
             ThreadPoolExecutor(max_workers=6) as pool:

            for d in dates:
                futures.append(pool.submit(self._download_single_month,
                                            symbol=symbol, dt=d))

            for fut in as_completed(futures):
                data.append(fut.result())
                bar.update(1)

        data = [df for df in data if isinstance(df, pd.DataFrame)]
        frame = self._preprocess(data)

        if self.verbose:
            print(f"{symbol}: fetched {frame.shape[0]} rows "
                  f"({len([d for d in data if not d.empty])} months non-empty)")
        return frame

    def _download_single_month(self, symbol: str, dt: date) -> pd.DataFrame:
        post = {"month": dt.month, "year": dt.year, "symbol": symbol}
        with self.session.post(self.__history, data=post, timeout=30) as r:
            soup = parser(r.text, "html.parser")
            return self._html_to_frame(soup)

    # —————————————————————————————— HTML → DataFrame utilities ——— #

    def _html_to_frame(self, soup) -> pd.DataFrame:
        rows = soup.select("tr")
        if not rows:                 # empty result for delisted / no data
            return pd.DataFrame(columns=self.headers).set_index("Date")

        stocks = defaultdict(list)
        for row in rows:
            cols = [td.get_text(strip=True) for td in row.select("td")]
            for key, val in zip(self.headers, cols):
                stocks[key].append(val)

        # Detect which column (Date or TIME) actually arrived
        date_key = next((k for k in ("Date", "TIME") if k in stocks), "Date")
        stocks[date_key] = [
            datetime.strptime(x, "%b %d, %Y") for x in stocks[date_key]
        ]

        df = pd.DataFrame(stocks, columns=self.headers)
        df.rename(columns={date_key: "Date"}, inplace=True)
        df.set_index("Date", inplace=True)
        return df

    # —————————————————————————————— misc helpers ——— #

    @staticmethod
    def daterange(start: date, end: date) -> list:
        """Return list of the first day of every month in [start, end]."""
        months = (end.year - start.year) * 12 + (end.month - start.month)
        anchors = [datetime(start.year, start.month, 1)]
        for i in range(months):
            anchors.append(anchors[-1] + relativedelta(months=1))
        return anchors or [start]

    @staticmethod
    def _preprocess(monthly_frames: list) -> pd.DataFrame:
        if not monthly_frames:
            return pd.DataFrame(columns=DataReader.headers).set_index("Date")

        df = pd.concat(monthly_frames).sort_index()
        df.rename(columns=str.title, inplace=True)      # OPEN→Open etc.
        df.Volume = df.Volume.str.replace(",", "")      # "1,234" → "1234"

        # numeric coercion
        for col in ("Open", "High", "Low", "Close", "Volume"):
            df[col] = df[col].str.replace(",", "").astype(np.float64)
        return df

# ───────────────────────────────────────────── runner / demo ——— #

if __name__ == "__main__":
    dr = DataReader(verbose=True)
    demo = dr.stocks("OGDC", date(2024, 1, 1), date(2024, 12, 31))
