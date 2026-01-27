import requests

BASE_URL = "https://businessesforsalefree.com"

class B4SFreeClient:
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
        # country=253 → UK
        url = (
            f"{BASE_URL}/firmListings.php"
            f"?country=253&page={page}"
        )
        return self.fetch(url)

    def detail_page(self, url: str) -> str:
        return self.fetch(url)

    def fetch(self, url: str) -> str:
        print(f"[B4SFREE FETCH] {url}")
        resp = self.session.get(
            url,
            timeout=self.timeout,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text