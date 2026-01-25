# src/brokers/daltons_client.py

import requests

BASE_URL = "https://www.daltonsbusiness.com"

class DaltonsClient:
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

    def list_page(self, page: int) -> str:
        url = f"{BASE_URL}/listing-businesses-for-sale/page/{page}/"
        return self.fetch(url)

    def detail_page(self, url: str) -> str:
        return self.fetch(url)

    def fetch(self, url: str) -> str:
        print(f"[DALTONS FETCH] {url}")
        resp = self.session.get(
            url,
            timeout=self.timeout,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text