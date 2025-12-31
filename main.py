import scrapers.notino_scraper as notino
from utils.vpn_manager import VpnManager


def main():
    try:
        vpn = VpnManager(max_retries=2, kill_wait_time=5, reconnect_wait_time=10)
        notino_scraper = notino.NotinoScraper()
        vpn.rotate_ip()
        # notino_scraper.scrape_product_links()
        notino_scraper.scrape_products()

    except Exception as e:
        print(f"Critical Error: {e}")


if __name__ == "__main__":
    main()
