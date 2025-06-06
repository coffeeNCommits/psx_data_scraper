import json
import requests
import pytest

from psx.web import DataReader


def network_available() -> bool:
    try:
        requests.head("https://dps.psx.com.pk", timeout=5).raise_for_status()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not network_available(), reason="network unreachable")
def test_reports_live(tmp_path):
    dr = DataReader(verbose=False)
    results = dr.reports("OGDC", years=1, save_dir=tmp_path)
    assert results, "no announcements fetched"
    out_file = tmp_path / "OGDC_Financial_Results_reports.json"
    assert out_file.exists()
    data = json.loads(out_file.read_text())
    assert len(data) == len(results)


def test_financial_reports_parsing(tmp_path):
    html = """
    <div id='reports'>
      <table>
        <tbody class='tbl__body'>
          <tr>
            <td><a href='file.pdf'>Quarterly</a></td>
            <td>2024-12-31</td>
            <td>2025-02-28</td>
          </tr>
        </tbody>
      </table>
    </div>
    """

    dr = DataReader(verbose=False)

    def fake_get_page(url):
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    def fake_extract_pdf(url):
        assert url.endswith("file.pdf")
        return "PDF TEXT"

    dr._get_page = fake_get_page
    dr._extract_pdf = fake_extract_pdf

    results = dr.reports("OGDC", tab_name="Financial Reports", years=1, save_dir=tmp_path)

    expected = [{
        "title": "Quarterly",
        "date": "2025-02-28",
        "source": "PDF",
        "content": "PDF TEXT",
    }]

    assert results == expected

    out_file = tmp_path / "OGDC_Financial_Reports_reports.json"
    assert json.loads(out_file.read_text()) == results
