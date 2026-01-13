# src/brokers/transworld_uk/transworld_uk_client.py

import requests
from urllib.parse import urljoin


class TransworldUKClient:
    BASE_URL = "https://tworldba.co.uk"
    SEARCH_URL = f"{BASE_URL}/buy-a-business/business-listing-search/"

    def __init__(self, timeout: int = 10):
        self.session = requests.Session()
        self.timeout = timeout

    def fetch_page(self, url: str) -> str | None:
        try:
            r = self.session.get(url, timeout=self.timeout)
            r.raise_for_status()
            return r.text
        except Exception:
            return None

    def fetch_index_page(self, url: str) -> str | None:
        return self.fetch_page(url)

    def fetch_detail_page(self, url: str) -> str | None:
        return self.fetch_page(url)

    def resolve_url(self, href: str) -> str:
        return urljoin(self.BASE_URL, href)