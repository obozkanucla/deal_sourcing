from bs4 import BeautifulSoup


def extract_clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # TODO: narrow to main content container
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())