import os
import time
from typing import Set
from bs4 import BeautifulSoup
from utils.base_scraper import BaseScraper
from config import NOTINO_URL
import curl_cffi.requests as requests


class NotinoScraper(BaseScraper):
    def __init__(self):
        # Initialize parent class
        super().__init__()
        # Configuration specific to Notino
        self.base_url = NOTINO_URL
        self.output_dir = "scrapers/urls/"
        self.output_file = "notino_products.txt"
        self.product_links: Set[str] = set()

    def scrape(self):
        """
        Main execution method to loop through pages, extract links,
        and save them to the file.
        """
        self.log.info(f"Starting scraping for: {self.base_url}")

        # Ensure output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.log.info(f"Created directory: {self.output_dir}")

        # Pagination Loop (1 to 500)
        for page in range(1, 501):
            # Construct URL based on the pattern provided
            # Page 1: ?f=1-1-2-3645, Page N: ?f={N}-1-2-3645
            query_param = f"?f={page}-9-2-3645"
            target_url = f"{self.base_url}{query_param}"

            self.log.info(f"Scraping Page {page}: {target_url}")

            try:
                # Perform Request
                response = requests.get(
                    target_url, impersonate=self.impersonate, timeout=30
                )

                # Check for 404 or other non-200 statuses
                if response.status_code == 404:
                    self.log.warning(f"Page {page} returned 404. Stopping scraper.")
                    break
                elif response.status_code != 200:
                    self.log.error(
                        f"Failed to fetch page {page}. Status: {response.status_code}"
                    )
                    continue

                # Parse HTML
                soup = BeautifulSoup(response.content, "html.parser")

                # Container extraction
                # Note: 'id' or 'class' selection depends on the exact HTML structure of Notino.
                # Assuming 'productListWrapper' is an ID or Class.
                # BeautifulSoup's select or find can handle this.
                product_container = soup.find(id="productListWrapper") or soup.find(
                    class_="productListWrapper"
                )

                if not product_container:
                    self.log.warning(
                        f"No product container found on page {page}. Stopping scraper."
                    )
                    break

                # Extract Links
                # Finding all anchor tags within the container
                items = product_container.find_all("a", href=True)

                if not items:
                    self.log.warning(
                        f"No products found in container on page {page}. Stopping scraper."
                    )
                    break

                page_new_links = 0
                for item in items:
                    link = str(item["href"])
                    # Normalize link (handle relative paths if necessary)
                    if link.startswith("/"):
                        link = f"https://www.notino.pl{link}"
                    # specific filter to ensure we are getting product links, not navigation
                    # (Optional refinement based on Notino's typical url structure)
                    if link not in self.product_links:
                        self.product_links.add(link)
                        page_new_links += 1

                self.log.info(
                    f"Page {page}: Found {page_new_links} new unique products. Total unique: {len(self.product_links)}"
                )

                # Save check: Save incrementally or wait until end?
                # Requirement implies saving at the end, but let's save periodically or just hold in memory as requested.

            except Exception as e:
                self.log.error(f"Error scraping page {page}: {e}")
                # Don't break on a single page error, try next
                continue

            # Polite scraping: Sleep based on BaseScraper interval
            time.sleep(self.interval)

        # Output Results
        self._save_results()

    def _save_results(self):
        """Helper method to write collected links to file."""
        output_path = os.path.join(self.output_dir, self.output_file)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                for link in sorted(self.product_links):
                    f.write(f"{link}\n")

            self.log.info(
                f"Successfully saved {len(self.product_links)} unique links to {output_path}"
            )
        except IOError as e:
            self.log.error(f"Failed to save results to file: {e}")
