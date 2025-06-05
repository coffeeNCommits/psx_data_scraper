import pandas as pd
import numpy as np
from datetime import date, datetime
from bs4 import BeautifulSoup
import pytest
import requests

from psx.web import DataReader
import psx


def test_daterange():
    result = DataReader.daterange(date(2024, 1, 1), date(2024, 3, 1))
    expected = [
        datetime(2024, 1, 1),
        datetime(2024, 2, 1),
        datetime(2024, 3, 1),
    ]
    assert result == expected

def test_daterange_invalid():
    with pytest.raises(ValueError):
        DataReader.daterange(date(2024, 3, 1), date(2024, 1, 1))

def make_sample_html():
    return """
    <table>
    <tr><td>Jan 01, 2024</td><td>1</td><td>2</td><td>1</td><td>2</td><td>1,000</td></tr>
    <tr><td>Jan 02, 2024</td><td>3</td><td>4</td><td>3</td><td>4</td><td>2,000</td></tr>
    </table>
    """


def test_html_to_frame():
    soup = BeautifulSoup(make_sample_html(), "html.parser")
    dr = DataReader(verbose=False)
    df = dr._html_to_frame(soup)
    assert list(df.columns) == DataReader.headers[1:]
    assert len(df) == 2
    assert df.index[0] == datetime(2024, 1, 1)


def test_preprocess():
    soup = BeautifulSoup(make_sample_html(), "html.parser")
    dr = DataReader(verbose=False)
    df = dr._html_to_frame(soup)
    processed = dr._preprocess([df])
    assert processed.Volume.iloc[0] == 1000.0
    assert processed.Open.dtype == np.float64


def test_download_single_month(monkeypatch):
    html = make_sample_html()

    class DummyResponse:
        def __init__(self, text):
            self.text = text
        def raise_for_status(self):
            pass

    def fake_post(url, data, timeout):
        return DummyResponse(html)

    dr = DataReader(verbose=False)
    session = dr.session
    monkeypatch.setattr(session, "post", fake_post)

    df = dr._download_single_month("OGDC", date(2024, 1, 1))
    assert df.index[0] == datetime(2024, 1, 1)


def test_download_single_month_http_error(monkeypatch):
    class DummyResponse:
        text = ""
        def raise_for_status(self):
            raise requests.HTTPError("boom")

    def fake_post(url, data, timeout):
        return DummyResponse()

    dr = DataReader(verbose=False)
    session = dr.session
    monkeypatch.setattr(session, "post", fake_post)

    with pytest.raises(requests.HTTPError):
        dr._download_single_month("OGDC", date(2024, 1, 1))


def test_psx_exports():
    assert isinstance(psx.stocks.__self__, DataReader)
    assert psx.stocks.__self__.verbose is False
    assert callable(psx.tickers)

def test_stocks_all(monkeypatch):
    dr = DataReader(verbose=False)
    tickers_df = pd.DataFrame([
        {"symbol": "OGDC", "isDebt": False},
        {"symbol": "BOND", "isDebt": True},
        {"symbol": "LUCK", "isDebt": False},
    ])

    monkeypatch.setattr(dr, "tickers", lambda: tickers_df)

    def fake_get_psx_data(symbol, dates):
        idx = [datetime(2024, 1, 1)]
        data = {c: [1] for c in DataReader.headers[1:]}
        return pd.DataFrame(data, index=idx)

    monkeypatch.setattr(dr, "get_psx_data", fake_get_psx_data)

    df = dr.stocks(None, date(2024, 1, 1), date(2024, 1, 1))
    assert set(df.index.get_level_values("Ticker")) == {"OGDC", "LUCK"}
    assert len(df) == 2
