import random
import curl_cffi.requests as requests
from utils.logger import get_logger
from config import SCRAPE_INTERVAL_MAX, SCRAPE_INTERVAL_MIN


class BaseScraper:
    def __init__(self):
        self.urls: list = self._get_URLs()
        self.log = get_logger(__name__)

    @property
    def interval(self) -> float:
        return random.uniform(SCRAPE_INTERVAL_MIN, SCRAPE_INTERVAL_MAX)

    @property
    def impersonate(self) -> requests.BrowserTypeLiteral:
        return random.choice(["chrome", "safari", "firefox"])

    def _get_URLs(self) -> list:
        with open("urls.txt", "r") as f:
            self.URLs = f.read().splitlines()
        return self.URLs
