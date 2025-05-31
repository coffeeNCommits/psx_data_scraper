# 📈 PSX Data Scraper

[![Made with Python](https://img.shields.io/badge/Made%20with-Python-blue?style=for-the-badge)](https://python.org)
[![Built with Love](https://img.shields.io/badge/Built%20with-%F0%9F%92%96-love-red?style=for-the-badge)](https://github.com/coffeeNCommits)

🔔 This is a **maintained** and improved fork of the original `psx-data-reader`.  
This version includes bug fixes and additional improvements for long-term usability.

---

## Overview

`psx_data_scraper` is a Python package that scrapes historical OHLCV (Open, High, Low, Close, Volume) data for all stocks listed on the **Pakistan Stock Exchange (PSX)**.

### ✅ Key Features

- Scrapes all available historical data till the current date
- Supports downloading data for **multiple companies in one call**
- Returns clean and structured **Pandas DataFrames**
- Built-in **multi-threading** for fast concurrent data fetching
- Chunked requests for efficiency and server-friendliness
- Fully open-source and ready for extension

---

## 📦 Installation

To use this scraper, clone the repository:

```bash
git clone https://github.com/coffeeNCommits/psx_data_scraper.git
cd psx_data_scraper
pip install -r requirements.txt

🚀 Usage

from psx_scraper.get_psx_data import DataReader
from datetime import date

reader = DataReader()

# Single stock example
df = reader.stocks("OGDC", start=date(2022, 1, 1), end=date(2023, 1, 1))
print(df.head())

# Multiple stocks
df_multi = reader.stocks(["OGDC", "LUCK"], start=date(2022, 1, 1), end=date(2023, 1, 1))
print(df_multi.head())

To get a list of current PSX tickers:

tickers = reader.tickers()
print(tickers.head())

```

📊 Example Output

Sample structure of returned DataFrame:

Date	Open	High	Low	Close	Volume	Symbol
2022-01-03	123.0	125.5	121.0	124.0	4,500,000	OGDC

🧑‍💻 Author
Maintainer: coffeeNCommits
Originally based on work by MuhammadAmir5670, but this version has been independently updated and maintained.

📌 License
This project is licensed under the MIT License — see the LICENSE file for details.

Feel free to contribute or raise issues. Happy scraping! 🚀
