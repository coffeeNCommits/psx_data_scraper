import io
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

import requests
from bs4 import BeautifulSoup as parser
from pdfminer.high_level import extract_text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_page(session: requests.Session, url: str):
    """Download HTML and return a parsed soup."""
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return parser(response.text, "html.parser")


def extract_pdf(
    session: requests.Session,
    url: str,
    *,
    parse_timeout: int = 15,
    maxpages: int = 20,
) -> str:
    """Download a PDF and extract its text.

    A separate worker thread is used to guard against PDFs that hang
    ``pdfminer`` during parsing.  If parsing exceeds ``parse_timeout`` seconds
    a ``RuntimeError`` is raised.
    """

    response = session.get(url, timeout=30)
    response.raise_for_status()
    buf = io.BytesIO(response.content)

    def parse():
        return extract_text(buf, maxpages=maxpages)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(parse)
        try:
            return future.result(timeout=parse_timeout)
        except FuturesTimeout:
            future.cancel()
            raise RuntimeError(f"PDF parse timed out for {url}")


def extract_view(session: requests.Session, url: str) -> str:
    """Fetch an HTML page and return its text content."""
    soup = get_page(session, url)
    return soup.get_text(" ", strip=True)


def get_page_dynamic(url: str):
    """Use Selenium to load a dynamic page and return a parsed soup."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    with webdriver.Chrome(options=options) as driver:
        driver.get(url)
        html = driver.page_source
    return parser(html, "html.parser")
