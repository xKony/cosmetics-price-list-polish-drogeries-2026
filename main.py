import scrapers.notino_scraper as notino


# Usage Example (Context dependent)
if __name__ == "__main__":
    # This block assumes the logger and config setup is functional
    try:
        scraper = notino.NotinoScraper()
        scraper.scrape()
    except Exception as e:
        print(f"Critical Error: {e}")
