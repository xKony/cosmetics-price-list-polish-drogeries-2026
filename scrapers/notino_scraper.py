import os
import time
import re
import curl_cffi.requests as requests
from typing import Set, Tuple, Optional, Match
from bs4 import BeautifulSoup
from utils.base_scraper import BaseScraper
from config import NOTINO_URL, MAX_PRODUCTS
from database.database import PriceDatabase


class NotinoScraper(BaseScraper):
    def __init__(self):
        # Initialize parent class
        super().__init__()
        # Configuration specific to Notino
        self.base_url: str = NOTINO_URL
        self.output_dir: str = "scrapers/urls/"
        self.output_file: str = "notino_products.txt"
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
        self.log.debug("Cena minimalna before cleaning: " + price_str)
        # Remove anything that isn't a digit or a comma
        clean: str = re.sub(r"[^\d,]", "", price_str)
        # Replace decimal comma with dot
        clean: str = clean.replace(",", ".")
        try:
            self.log.debug(f"Clean cena minimalna: {clean}")
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
        match: Optional[Match[str]] = re.search(r"(\d+(?:,\d+)?)\s*([a-zA-Z]+)", text)
        if match:
            vol_str: str = match.group(1).replace(",", ".")
            unit: str = match.group(2)
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
            query_param: str = f"?f={page}-9-2-3645"
            target_url: str = f"{self.base_url}{query_param}"

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

                page_new_links: int = 0
                for item in items:
                    link: str = str(item["href"])
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
        # 1. Extract Brand/Name from <h1>
        # Structure: <h1 data-testid="pd-header-title">...<a>Brand</a><span>Name</span>...</h1>
        h1 = soup.find("h1", {"data-testid": "pd-header-title"})
        if h1:
            brand_link = h1.find("a")
            brand: str = brand_link.get_text(strip=True) if brand_link else "Notino Selection"
            # Name is in the first span after the brand link
            name_span = (
                brand_link.find_next_sibling("span") if brand_link else h1.find("span")
            )
            name: str = name_span.get_text(strip=True) if name_span else "Unknown Product"
        else:
            # Fallback to general h1 or meta
            h1_fallback = soup.find("h1")
            name = h1_fallback.get_text(strip=True) if h1_fallback else "Unknown Product"
            brand = "Notino Selection"
            brand_meta = soup.find("meta", attrs={"itemprop": "brand"})
            if brand_meta and brand_meta.get("content"):
                brand = brand_meta["content"]

        # 2. Extract Rating
        ratings: float = 0.0
        rating_link = soup.find("a", href="#pdReviewsScroll")
        if rating_link and rating_link.get("title"):
            try:
                match = re.search(r"(\d+(?:[.,]\d+)?)", rating_link["title"])
                if match:
                    ratings = float(match.group(1).replace(",", "."))
            except (ValueError, IndexError):
                pass
        else:
            rating_meta = soup.find("meta", attrs={"itemprop": "ratingValue"})
            if rating_meta and rating_meta.get("content"):
                try:
                    ratings = float(rating_meta["content"])
                except ValueError:
                    pass

        # 3. Handle last_30d_price (Omnibus)
        last_30d_price: float = 0.0
        # Case A: "Cena minimalna" in specifications
        specs = soup.find("div", {"data-testid": "product-specifications"})
        if specs:
            min_price_text = specs.find(string=re.compile("Cena minimalna", re.I))
            if min_price_text:
                match = re.search(r"Cena minimalna\s*(\d+(?:[.,]\d+)?)", min_price_text)
                if match:
                    last_30d_price = self._clean_price(match.group(1))

        # Case B: "Ostatnia najniższa cena" in discount/voucher block
        lowest_msg = soup.find(string=re.compile("Ostatnia najniższa cena", re.I))
        if lowest_msg:
            # Usually followed by a span with class 'lwyce7r'
            price_span = lowest_msg.parent.find("span", class_="lwyce7r")
            if price_span:
                last_30d_price = self._clean_price(price_span.get_text(strip=True))

        # 4. Detect Scenarios
        variants_container = soup.find(id="pdVariantsTile")

        if variants_container:
            self._handle_multi_variant(
                variants_container, brand, name, last_30d_price, ratings
            )
        else:
            self._handle_single_variant(
                soup, brand, name, last_30d_price, url, ratings
            )

    def _handle_multi_variant(self, container, brand, name, last_30d_price, ratings):
        """Scenario A: Parse multiple variants from the list."""
        try:
            items = container.find_all("li")
            for item in items:
                # Price
                price_span = item.find("span", {"data-testid": "price-variant"})
                if not price_span:
                    continue
                price: float = self._clean_price(price_span.get_text(strip=True))

                # Volume / Label
                vol_div = item.find(class_="pd-variant-label")
                vol_text: str = vol_div.get_text(strip=True) if vol_div else ""
                volume, unit = self._parse_volume(vol_text)

                # Unique ID (from anchor)
                link = item.find("a")
                variant_id: str = link.get("id") if link else "unknown_var"
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
                    last_30d_price=last_30d_price,
                    ratings=ratings,
                    desc="Standard",
                )
        except Exception as e:
            self.log.error(f"Error parsing multi-variants: {e}")

    def _handle_single_variant(self, soup, brand, name, last_30d_price, url, ratings):
        """Scenario B: Parse single selected variant."""
        try:
            promo_desc: str = "Standard"
            raw_price: float = 0.0

            # 1. Volume extraction - Priority: aria-live block
            volume: float = 0.0
            unit: str = "N/A"
            aria_live_div = soup.find("div", {"aria-live": "assertive"})
            if aria_live_div:
                # Based on example: first span inside nested div
                vol_span = aria_live_div.find("span")
                if vol_span:
                    volume, unit = self._parse_volume(vol_span.get_text(strip=True))

            # Fallback for volume
            if volume == 0:
                wrapper = soup.find(id="pdSelectedVariant")
                if wrapper:
                    target = wrapper.find(string=re.compile(r"\d+\s*[a-zA-Z]+"))
                    if target:
                        volume, unit = self._parse_volume(target.strip())

            # 2. Price extraction - Priority: Voucher block
            # Logic: If 'z kodem' exists, that price is raw_price.
            voucher_indicator = soup.find(string=re.compile("z kodem", re.I))
            if voucher_indicator:
                # Look for price in the voucher block (usually nearby span with content or pd-price-wrapper)
                # Looking up the tree or in siblings for the price wrapper
                parent_block = voucher_indicator.find_parent(class_="tc9g2yy") or voucher_indicator.find_parent("div")
                if parent_block:
                    price_wrapper = parent_block.find("span", {"data-testid": "pd-price-wrapper"})
                    if price_wrapper:
                        raw_price = self._clean_price(price_wrapper.get_text(strip=True))
                        # Try to find the code itself
                        code_span = parent_block.find("span", class_="c1tsg8xv")
                        code_name = code_span.get_text(strip=True) if code_span else "voucher"
                        promo_desc = f"z kodem {code_name}"

            # Fallback to standard price wrapper
            if raw_price == 0:
                # Check for "aria-live" price block
                if aria_live_div:
                    price_span = aria_live_div.find("span", {"data-testid": "pd-price"})
                    if price_span:
                        raw_price = self._clean_price(price_span.get_text(strip=True))

                # Fallback to standard pd-price-wrapper
                if raw_price == 0:
                    price_div = soup.find("div", {"data-testid": "pd-price-wrapper"})
                    if price_div:
                        raw_price = self._clean_price(price_div.get_text(strip=True))

            # Check for "Promocja ograniczona czasowo" if no voucher
            if promo_desc == "Standard":
                if soup.find(string=re.compile("Promocja ograniczona czasowo", re.I)):
                    promo_desc = "Promocja ograniczona czasowo"

            # 3. Save
            product_id_proxy: str = url.split("/")[-1]
            self._save_to_db(
                ean=product_id_proxy,
                brand=brand,
                name=name,
                category="Face",
                unit=unit,
                volume=volume,
                price=raw_price,
                last_30d_price=last_30d_price,
                ratings=ratings,
                desc=promo_desc,
            )

        except Exception as e:
            self.log.error(f"Error parsing single variant: {e}")

    def _save_to_db(
        self,
        ean: str,
        brand: str,
        name: str,
        category: str,
        unit: str,
        volume: float,
        price: float,
        last_30d_price: float,
        ratings: float,
        desc: str = "Standard",
    ):
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
            # raw_price = current price on site (after voucher if applicable)
            # last_30d_price = the Omnibus minimum price if found, else fallback to current
            self.db.log_price(
                product_id=prod_db_id,
                shop="Notino",
                raw_price=price,
                last_30d_price=last_30d_price if last_30d_price > 0 else price,
                ratings=ratings,
                desc=desc,
                is_promo=desc != "Standard",
            )
            self.log.info(f"Saved: {name} | {price} PLN (Min: {last_30d_price})")

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
