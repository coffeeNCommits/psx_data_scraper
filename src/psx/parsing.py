from collections import defaultdict
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

# Standard column order for all scraped tables
HEADERS = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']


def daterange(start: date, end: date) -> list:
    """Return the first day of each month between ``start`` and ``end``."""
    if end < start:
        raise ValueError("end date must not be earlier than start date")

    months = (end.year - start.year) * 12 + (end.month - start.month)
    anchors = [datetime(start.year, start.month, 1)]
    for _ in range(months):
        anchors.append(anchors[-1] + relativedelta(months=1))
    return anchors or [start]


def html_to_frame(soup) -> pd.DataFrame:
    """Convert the PSX table HTML into a DataFrame."""
    rows = soup.select("tr")
    if not rows:
        return pd.DataFrame(columns=HEADERS).set_index("Date")

    stocks = defaultdict(list)
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.select("td")]
        for key, val in zip(HEADERS, cols):
            stocks[key].append(val)

    # The column may be labelled TIME on some pages
    date_key = next((k for k in ("Date", "TIME") if k in stocks), "Date")
    stocks[date_key] = [
        datetime.strptime(x, "%b %d, %Y") for x in stocks[date_key]
    ]

    df = pd.DataFrame(stocks, columns=HEADERS)
    df.rename(columns={date_key: "Date"}, inplace=True)
    df.set_index("Date", inplace=True)
    return df


def preprocess(monthly_frames: list) -> pd.DataFrame:
    """Merge monthly frames and coerce numeric columns."""
    if not monthly_frames:
        return pd.DataFrame(columns=HEADERS).set_index("Date")

    df = pd.concat(monthly_frames).sort_index()
    df.rename(columns=str.title, inplace=True)

    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in df.columns:
            series = df[col]
            if series.dtype == object:
                series = series.str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(series, errors="coerce").astype(np.float64)
    return df
