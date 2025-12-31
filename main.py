import scrapers.notino_scraper as notino


# Usage Example (Context dependent)
if __name__ == "__main__":
    # This block assumes the logger and config setup is functional
    try:
        notino_scraper = notino.NotinoScraper()
        # notino_scraper.scrape_product_links()
        notino_scraper.scrape_products()
    except Exception as e:
        print(f"Critical Error: {e}")
