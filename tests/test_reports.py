import json
from datetime import date
from bs4 import BeautifulSoup
from psx.web import DataReader


def test_reports(monkeypatch, tmp_path):
    dr = DataReader(verbose=False)

    html1 = """
    <div id="FinancialResults">
      <table>
        <tr><td class="title">Q1 2024</td><td class="date">2024-02-15</td><td><a class="pdf" href="r1.pdf">PDF</a></td></tr>
        <tr><td class="title">Q4 2023</td><td class="date">2023-11-01</td><td><a class="view" href="v1.html">View</a></td></tr>
      </table>
      <a class="next" href="page2.html">Next</a>
    </div>
    """
    html2 = """
    <div id="FinancialResults">
      <table>
        <tr><td class="title">Q3 2019</td><td class="date">2019-10-01</td><td><a class="pdf" href="old.pdf">PDF</a></td></tr>
      </table>
    </div>
    """

    pages = [BeautifulSoup(html1, "html.parser"), BeautifulSoup(html2, "html.parser")]

    def fake_get_page(url):
        return pages.pop(0)

    monkeypatch.setattr(dr, "_get_page", fake_get_page)
    monkeypatch.setattr(dr, "_extract_pdf", lambda url: "pdf text")
    monkeypatch.setattr(dr, "_extract_view", lambda url: "view text")

    results = dr.reports("OGDC", save_dir=tmp_path)

    assert len(results) == 2
    assert results[0]["source"] == "PDF"

    out_file = tmp_path / "OGDC_Financial_Results_reports.json"
    assert out_file.exists()
    data = json.loads(out_file.read_text())
    assert len(data) == 2
