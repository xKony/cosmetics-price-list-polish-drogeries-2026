import os
import time
import re
from typing import Set, Optional, Tuple, List
from bs4 import BeautifulSoup
from utils.base_scraper import BaseScraper
from config import NOTINO_URL, MAX_PRODUCTS
from database.database import (
    PriceDatabase,
)  # Assuming database.py is in the python path
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

        # Initialize Database
        # Ensure database directory exists if needed
        if not os.path.exists("database"):
            os.makedirs("database")
        self.db = PriceDatabase(db_name="database/prices.db")

    def _clean_price(self, price_str: str) -> float:
        """
        Converts '343,00 zł' or '343,00' -> 343.0
        """
        if not price_str:
            return 0.0
        # Remove anything that isn't a digit or a comma
        clean = re.sub(r"[^\d,]", "", price_str)
        # Replace decimal comma with dot
        clean = clean.replace(",", ".")
        try:
            return float(clean)
        except ValueError:
            self.log.error(f"Failed to convert price: {price_str}")
            return 0.0

    def _parse_volume(self, text: str) -> Tuple[float, str]:
        """
        Extracts volume and unit from strings like '50 ml', '3,5 g'.
        Returns (volume_float, unit_string).
        """
        if not text:
            return 0.0, "N/A"

        # Regex to find number (handling comma decimals) and unit
        match = re.search(r"(\d+(?:,\d+)?)\s*([a-zA-Z]+)", text)
        if match:
            vol_str = match.group(1).replace(",", ".")
            unit = match.group(2)
            try:
                return float(vol_str), unit
            except ValueError:
                return 0.0, unit
        return 0.0, "N/A"

    def scrape_product_links(self):
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
            if len(self.product_links) > MAX_PRODUCTS:
                break
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

    def scrape_products(self):
        """
        Reads URLs from file and extracts detailed product data.
        """
        input_path = os.path.join(self.output_dir, self.output_file)

        if not os.path.exists(input_path):
            self.log.error(f"Input file not found: {input_path}")
            return

        with open(input_path, "r", encoding="utf-8") as f:
            urls = f.read().splitlines()

        self.log.info(f"Loaded {len(urls)} URLs for detailed scraping.")

        for url in urls:
            try:
                self.log.info(f"Processing product: {url}")
                response = requests.get(url, impersonate=self.impersonate, timeout=30)

                if response.status_code != 200:
                    self.log.error(
                        f"Failed to fetch {url} (Status: {response.status_code})"
                    )
                    continue

                soup = BeautifulSoup(response.content, "html.parser")
                self._parse_and_save_product(soup, url)

            except Exception as e:
                self.log.error(f"Critical error processing {url}: {e}")

            # Politeness
            time.sleep(self.interval)

    def _parse_and_save_product(self, soup: BeautifulSoup, url: str):
        """
        Parses the product page soup, determines if it's single or multi variant,
        and saves data to DB.
        """
        # 1. Extract Common Data (Brand/Name)
        h1 = soup.find("h1")
        full_name = h1.get_text(strip=False) if h1 else "Unknown Product"
        print(full_name)
        # Simple heuristic: Split H1 for Brand, or use meta tags if available
        # Requirement says "Extract Brand/Name from <h1>"
        brand = "Notino Selection"  # Default
        name = full_name

        # Try to find specific brand meta/tag if possible, else rely on H1
        brand_meta = soup.find("meta", attrs={"itemprop": "brand"})
        if brand_meta and brand_meta.get("content"):
            brand = brand_meta["content"]

        # 2. Extract Omnibus Min Price (Common)
        min_price = 0.0
        specs = soup.find("div", {"data-testid": "product-specifications"})
        if specs:
            # Look for text "Cena minimalna"
            # We assume the text node contains the price
            omnibus_text = specs.find(string=re.compile("Cena minimalna"))
            if omnibus_text:
                min_price = self._clean_price(omnibus_text)

        # 3. Detect Scenarios
        variants_container = soup.find(id="pdVariantsTile")

        if variants_container:
            self._handle_multi_variant(variants_container, brand, name, min_price)
        else:
            self._handle_single_variant(soup, brand, name, min_price, url)

    def _handle_multi_variant(self, container, brand, name, min_price):
        """Scenario A: Parse multiple variants from the list."""
        try:
            items = container.find_all("li")
            for item in items:
                # Price
                price_span = item.find("span", {"data-testid": "price-variant"})
                if not price_span:
                    continue
                price = self._clean_price(price_span.get_text(strip=True))

                # Volume / Label
                vol_div = item.find(class_="pd-variant-label")
                vol_text = vol_div.get_text(strip=True) if vol_div else ""
                volume, unit = self._parse_volume(vol_text)

                # Unique ID (from anchor)
                link = item.find("a")
                variant_id = link.get("id") if link else "unknown_var"
                # Fallback if ID is missing, use href hash
                if not variant_id and link and "href" in link.attrs:
                    variant_id = link["href"].split("#")[-1]

                self._save_to_db(
                    ean=variant_id,  # Using HTML ID as EAN proxy
                    brand=brand,
                    name=f"{name} ({vol_text})",
                    category="Face",
                    unit=unit,
                    volume=volume,
                    price=price,
                    min_price=min_price,
                )
        except Exception as e:
            self.log.error(f"Error parsing multi-variants: {e}")

    def _handle_single_variant(self, soup, brand, name, min_price, url):
        """Scenario B: Parse single selected variant."""
        try:
            wrapper = soup.find(id="pdSelectedVariant")
            if not wrapper:
                self.log.warning("Could not find single variant wrapper.")
                return

            # Price
            price_div = soup.find("div", {"data-testid": "pd-price-wrapper"})
            price_span = (
                price_div.find("span", attrs={"content": True}) if price_div else None
            )

            raw_price_val = 0.0
            if price_span:
                # Usually 'content' attribute holds the clean number, or use text
                raw_price_val = self._clean_price(price_span.get_text(strip=True))

            # Volume
            # Dynamic class, so we search for text pattern inside the wrapper
            # Look for any child div that matches volume regex
            vol_text = ""
            # Helper to find text matching volume pattern
            target = wrapper.find(string=re.compile(r"\d+\s*[a-zA-Z]+"))
            if target:
                vol_text = target.strip()

            volume, unit = self._parse_volume(vol_text)

            # Generate a pseudo-ID from URL if no specific ID found
            product_id_proxy = url.split("/")[-1]

            self._save_to_db(
                ean=product_id_proxy,
                brand=brand,
                name=name,
                category="Face",
                unit=unit,
                volume=volume,
                price=raw_price_val,
                min_price=min_price,
            )

        except Exception as e:
            self.log.error(f"Error parsing single variant: {e}")

    def _save_to_db(self, ean, brand, name, category, unit, volume, price, min_price):
        """
        Helper to interface with PriceDatabase.
        """
        try:
            # 1. Add Product to Dictionary
            prod_db_id = self.db.add_product(
                ean=ean,
                brand=brand,
                name=name,
                category=category,
                unit=unit,
                volume=volume,
            )

            # 2. Log Price
            # We map the scraped current price to effective_price
            # We map the Omnibus/Min price to raw_price (or just use same if no diff)
            self.db.log_price(
                product_id=prod_db_id,
                shop="Notino",
                raw_price=min_price if min_price > 0 else price,
                effective_price=price,
                desc="Standard",
                is_promo=False,  # Logic for promo detection can be added later
            )
            self.log.info(f"Saved: {name} | {price} PLN")

        except Exception as e:
            self.log.error(f"Database error for {name}: {e}")

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
