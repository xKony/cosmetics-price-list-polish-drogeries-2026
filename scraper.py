from config import HEADLESS_BROWSER
from utils.base_browser import BaseBrowser
from utils.logger import get_logger

log = get_logger(__name__)


class Scraper(BaseBrowser):
    def __init__(self):
        super().__init__()
