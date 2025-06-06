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