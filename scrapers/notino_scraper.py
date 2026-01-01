import os
import time
import re
import curl_cffi.requests as requests
import itertools
import random
from typing import Set, Tuple, Optional, Match
from bs4 import BeautifulSoup
from utils.base_scraper import BaseScraper
from config import NOTINO_URL, MAX_PRODUCTS
from database.price_database import PriceDatabase


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
        # Remove anything that isn't a digit or a comma
        clean: str = re.sub(r"[^\d,]", "", price_str)
        # Replace decimal comma with dot
        clean: str = clean.replace(",", ".")
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
        Uses a queue to handle variants dynamically.
        Randomizes order and retries failed URLs up to 3 times.
        """
        input_path = os.path.join(self.output_dir, self.output_file)

        if not os.path.exists(input_path):
            self.log.error(f"Input file not found: {input_path}")
            return

        with open(input_path, "r", encoding="utf-8") as f:
            initial_urls = f.read().splitlines()

        # Randomize initial order
        random.shuffle(initial_urls)

        # Queue for processing (BFS)
        queue = list(initial_urls)
        # Track visited URLs to avoid duplicates and loops
        visited = set(initial_urls)
        
        # Track failures and attempts
        failed_urls = []
        attempts = {url: 0 for url in initial_urls}

        self.log.info(f"Loaded {len(initial_urls)} URLs. Starting processing with variant discovery.")

        while queue or failed_urls:
            # If queue is empty but we have failures, try to requeue valid retries
            if not queue and failed_urls:
                self.log.info(f"Main queue empty. Retrying {len(failed_urls)} failed URLs...")
                requeue_list = []
                for f_url in failed_urls:
                    # We only requeue if we haven't hit the limit yet
                    # Note: attempts are incremented on failure
                    curr_attempts = attempts.get(f_url, 0)
                    if curr_attempts < 3:
                        requeue_list.append(f_url)
                
                failed_urls = []
                if not requeue_list:
                    self.log.info("No more URLs to retry (max attempts reached for all failures).")
                    break
                
                queue = requeue_list
                self.log.info(f"Requeued {len(queue)} URLs for retry.")

            if not queue:
                break

            url = queue.pop(0)
            
            # Increment attempt counter for this processing start? 
            # Or only on failure?
            # Let's count *attempts made*.
            attempts[url] = attempts.get(url, 0) + 1
            
            try:
                self.log.info(f"Processing product: {url} (Attempt {attempts[url]})")
                response = requests.get(url, impersonate=self.impersonate, timeout=30)

                if response.status_code != 200:
                    self.log.error(
                        f"Failed to fetch {url} (Status: {response.status_code})"
                    )
                    # Treat non-200 as failure
                    if attempts[url] < 3:
                        failed_urls.append(url)
                    continue

                soup = BeautifulSoup(response.content, "html.parser")
                
                # Parse product and get potential variant links
                new_variants = self._parse_and_save_product(soup, url)
                
                # Add new variants to queue
                for variant_url in new_variants:
                    if variant_url not in visited:
                        visited.add(variant_url)
                        queue.append(variant_url)
                        # Initialize attempt count for new variant
                        attempts[variant_url] = 0
                        self.log.info(f"Added variant to queue: {variant_url}")

            except Exception as e:
                self.log.error(f"Critical error processing {url}: {e}")
                if attempts[url] < 3:
                    failed_urls.append(url)

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

        # 3. Handle last_30d_price (Omnibus) and Product Code (Kod)
        last_30d_price: float = 0.0
        product_code: Optional[str] = None
        
        # specs block contains both Omnibus and Product Code
        specs = soup.find("div", {"data-testid": "product-specifications"})
        if specs:
            # Omnibus
            # Search in the full text of specs to handle split elements
            specs_text = specs.get_text(" ", strip=True) 
            match = re.search(r"Cena minimalna\s*(\d+(?:[.,]\d+)?)", specs_text, re.I)
            if match:
                last_30d_price = self._clean_price(match.group(1))
            
            # Product Code (Kod)
            kod_text = specs.find(string=re.compile("Kod:", re.I))
            if kod_text:
                # Find the parent or sibling that contains the actual code string
                # Structure: <span class="cy9ivtp"><span class="c6v4u6o">Kod: </span>AAC04355</span>
                parent_span = kod_text.find_parent("span", class_="cy9ivtp")
                if parent_span:
                    product_code = parent_span.get_text(strip=True).replace("Kod:", "").strip()

        # Case B fallback for Omnibus: "Ostatnia najniższa cena" in discount/voucher block
        if last_30d_price == 0:
            lowest_msg = soup.find(string=re.compile("Ostatnia najniższa cena", re.I))
            if lowest_msg:
                price_span = lowest_msg.parent.find("span", class_="lwyce7r")
                if price_span:
                    last_30d_price = self._clean_price(price_span.get_text(strip=True))

        # 4. Save Current Product Data
        # Always treat the current page as a single variant record
        self._handle_single_variant(
            soup, brand, name, last_30d_price, url, ratings, product_code
        )

        # 5. Discover Variants
        # Extract links for other variants to visit
        variant_links = []
        try:
            # Selector: a[data-testid^="pd-variant-"]
            variant_elements = soup.find_all("a", attrs={"data-testid": re.compile(r"^pd-variant-")})
            for el in variant_elements:
                href = el.get("href")
                if href:
                    if href.startswith("/"):
                        href = f"https://www.notino.pl{href}"
                    variant_links.append(href)
        except Exception as e:
            self.log.error(f"Error extracting variants: {e}")

        return variant_links



    def _handle_single_variant(self, soup, brand, name, last_30d_price, url, ratings, product_code=None):
        """Scenario B: Parse single selected variant."""
        try:
            promo_desc: Optional[str] = None
            raw_price: float = 0.0

            # 1. Volume extraction
            volume: float = 0.0
            unit: str = "N/A"
            aria_live_div = soup.find("div", {"aria-live": "assertive"})
            if aria_live_div:
                # Iterate through all spans to find one that matches volume pattern
                for span in aria_live_div.find_all("span"):
                    v, u = self._parse_volume(span.get_text(strip=True))
                    if v > 0:
                        volume, unit = v, u
                        break

            if volume == 0:
                wrapper = soup.find(id="pdSelectedVariant")
                if wrapper:
                    # Look for string with a volume-like pattern (number followed by unit)
                    target = wrapper.find(string=re.compile(r"\d+\s*[a-zA-Z]+"))
                    if target:
                        volume, unit = self._parse_volume(target.strip())

            # 2. Price extraction
            pd_price_wrapper = soup.find(attrs={"data-testid": "pd-price-wrapper"})
            # Try to find specific voucher/promo price block
            # Look for 'z kodem' only within relevant price containers to avoid footer false positives
            voucher_indicator = None
            if pd_price_wrapper:
                # Check siblings or parents of price wrapper for voucher info
                # Usually the price wrapper is inside a block that contains the 'z kodem' text
                container = pd_price_wrapper.find_parent("div")
                if container:
                    voucher_indicator = container.find(string=re.compile("z kodem", re.I))
            
            if not voucher_indicator:
                # Fallback: check global soup but verify context (must accept a code or be near price)
                # We limit strictness to avoid "Zapisz się newsletter z kodem" in footers
                candidates = soup.find_all(string=re.compile("z kodem", re.I))
                for cand in candidates:
                    # Check if this candidate is near a price wrapper
                    parent = cand.find_parent("div")
                    if parent and parent.find("span", {"data-testid": "pd-price-wrapper"}):
                        voucher_indicator = cand
                        break

            if voucher_indicator:
                parent_block = voucher_indicator.find_parent(class_="tc9g2yy") or voucher_indicator.find_parent("div")
                if parent_block:
                    price_wrapper = parent_block.find("span", {"data-testid": "pd-price-wrapper"})
                    if price_wrapper:
                        # Prefer 'content' attribute if available for clean numeric value
                        price_val_el = price_wrapper.find("span", {"content": True})
                        if price_val_el:
                            raw_price = self._clean_price(price_val_el["content"])
                        else:
                            raw_price = self._clean_price(price_wrapper.get_text(strip=True))
                        
                        code_span = parent_block.find("span", class_="c1tsg8xv")
                        code_name = code_span.get_text(strip=True) if code_span else None
                        if code_name:
                            promo_desc = f"z kodem {code_name}"

            # Priority 2: Use id="pd-price"
            if raw_price == 0:
                pd_price_el = soup.find(id="pd-price")
                if pd_price_el:
                    price_span = pd_price_el.find("span", {"data-testid": "pd-price"})
                    if price_span:
                        raw_price = self._clean_price(price_span.get("content") or price_span.get_text(strip=True))
                    else:
                        raw_price = self._clean_price(pd_price_el.get_text(strip=True))

            # Priority 3: data-testid="pd-price" anywhere
            if raw_price == 0:
                price_el = soup.find(attrs={"data-testid": "pd-price"})
                if price_el:
                    raw_price = self._clean_price(price_el.get("content") or price_el.get_text(strip=True))

             # Priority 4: Fallback checks
            if raw_price == 0:
                if aria_live_div:
                    price_span = aria_live_div.find("span", {"data-testid": "pd-price"})
                    if price_span:
                        raw_price = self._clean_price(price_span.get_text(strip=True))

                if raw_price == 0 and pd_price_wrapper:
                     raw_price = self._clean_price(pd_price_wrapper.get_text(strip=True))

            # Promo Description Logic
            if not promo_desc:
                # 1. Check strict scoped elements first (like pdSelectedVariant) - implemented above?
                # Actually, let's rely on the strategy of checking relatives of the price wrapper.
                
                # Strategy: Go up from price_wrapper to find the container that holds both price and promo text.
                # The user provided example shows they are close siblings/cousins.
                if pd_price_wrapper:
                    curr = pd_price_wrapper
                    # Traverse up to 4 levels to find a common parent container
                    for _ in range(4):
                        curr = curr.find_parent("div")
                        if not curr:
                            break
                        
                        # Check strictly in this container's text 
                        # We use a regex search on the clean text of this container
                        if curr.find(string=re.compile("Promocja ograniczona czasowo", re.I)):
                            promo_desc = "promocja"
                            break
                            
            # 3. Save - Use Product Code as EAN if found, else fallback to URL proxy
            final_ean: str = product_code if product_code else url.split("/")[-1]
            
            # Ensure last_30d_price defaults to raw_price if 0
            if last_30d_price <= 0:
                last_30d_price = raw_price

            self._save_to_db(
                ean=final_ean,
                brand=brand,
                name=name,
                category="Face",
                unit=unit,
                volume=volume,
                price=raw_price,
                last_30d_price=last_30d_price,
                ratings=ratings,
                desc=promo_desc if promo_desc else "",
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
        desc: Optional[str] = None,
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
                is_promo=bool(desc) and desc != "Standard",
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
