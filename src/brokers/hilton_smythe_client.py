# src/brokers/hilton_smythe_client.py

import requests
from urllib.parse import quote_plus

BASE_URL = "https://hiltonsmythe.com"

class HiltonSmytheClient:
    def __init__(self, timeout=(5, 20)):
        self.session = requests.Session()
        self.timeout = timeout
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

    def build_sector_url(self, sector: str, page: int) -> str:
        sector_q = quote_plus(sector)
        return (
            f"{BASE_URL}/businesses-for-sale/"
            f"?business-sector={sector_q}&page={page}"
        )

    def fetch(self, url: str) -> str:
        print(f"[HS FETCH] {url}")
        resp = self.session.get(
            url,
            timeout=self.timeout,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text