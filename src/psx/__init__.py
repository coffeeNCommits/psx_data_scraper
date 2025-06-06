from .reader import DataReader

__version__ = "1.0.0"

_data_reader = DataReader(verbose=False)

stocks = _data_reader.stocks

tickers = _data_reader.tickers

reports = _data_reader.reports