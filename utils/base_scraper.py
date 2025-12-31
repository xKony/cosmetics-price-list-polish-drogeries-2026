import random
import curl_cffi.requests as requests
from typing import Optional, Any, Set
from utils.logger import get_logger
from config import SCRAPE_INTERVAL_MAX, SCRAPE_INTERVAL_MIN


class BaseScraper:
    def __init__(self):
        self.urls: list = self._get_URLs()
        self.interval: int = self._get_scraping_interval() 
        self.log = get_logger(__name__)
        self.impersonate: Optional[requests.BrowserTypeLiteral] = (
            self._get_impersonation()
        )

    def _get_impersonation(self) -> requests.BrowserTypeLiteral:
        return random.choice(["chrome", "safari", "firefox"])

    def _get_scraping_interval(
        self, min=SCRAPE_INTERVAL_MIN, max=SCRAPE_INTERVAL_MAX
    ) -> int:
        return random.randint(min, max)

    def _get_URLs(self) -> list:
        with open("urls.txt", "r") as f:
            self.URLs = f.read().splitlines()
        return self.URLs
