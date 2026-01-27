# src/brokers/bsr_client.py

import requests

BASE_URL = "https://www.business-sale.com"


class BusinessSaleReportClient:
    """
    Thin HTTP client for Business Sale Report (BSR).

    Notes:
    - Public index pages only
    - No authentication
    - Detail pages may be gated (handled upstream)
    """

    def __init__(self, timeout=(5, 20)):
        self.session = requests.Session()
        self.timeout = timeout

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        })

    # --------------------------------------------------
    # Index pages
    # --------------------------------------------------

    def list_page(self, page: int = 1) -> str:
        """
        Fetch paginated companies-for-sale index.

        Observed patterns:
        - /companies-for-sale?page=1
        - Some pages may redirect or clamp page numbers
        """

        if page <= 1:
            url = f"{BASE_URL}/companies-for-sale"
        else:
            url = f"{BASE_URL}/companies-for-sale?page={page}"

        return self.fetch(url)

    # --------------------------------------------------
    # Detail pages (best-effort, often gated)
    # --------------------------------------------------

    def detail_page(self, url: str) -> str:
        return self.fetch(url)

    # --------------------------------------------------
    # Core fetch
    # --------------------------------------------------

    def fetch(self, url: str) -> str:
        print(f"[BSR FETCH] {url}")

        resp = self.session.get(
            url,
            timeout=self.timeout,
            allow_redirects=True,
        )

        resp.raise_for_status()
        return resp.text